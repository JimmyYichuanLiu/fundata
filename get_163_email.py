#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
163邮箱基金净值数据采集程序（智能版）
使用IMAP协议连接到163邮箱，遍历所有邮件，智能提取Excel附件中的基金净值数据并存储到SQLite数据库

功能：
1. 增量拉取邮件（基于IMAP UID，避免重复处理）
2. 智能识别并提取Excel附件中的核心数据（支持多种格式）
   - 产品名称
   - 产品代码
   - 净值日期
   - 单位净值
   - 累计单位净值
3. 将数据存储到SQLite数据库
4. 自动去重（基于产品代码和净值日期）
5. 将无法识别/提取失败的邮件附件信息持久化到 extraction_failures 表
6. 按产品代码分类、按净值日期排序展示数据
"""

import os
import re
import imaplib
import email
from email.header import decode_header
import sys
import sqlite3
import pandas as pd
from datetime import datetime
from io import BytesIO
from dotenv import load_dotenv
from smart_extractor import extract_and_normalize


def init_database(db_path):
    """Initialize through the shared, backed-up versioned migration."""
    from fund_store import initialize_database
    return initialize_database(db_path)


def get_sync_state(conn):
    """读取上次同步状态，返回 (last_uid, uidvalidity)"""
    cursor = conn.cursor()
    cursor.execute("SELECT value FROM sync_state WHERE key = 'last_uid'")
    row = cursor.fetchone()
    last_uid = int(row[0]) if row else 0

    cursor.execute("SELECT value FROM sync_state WHERE key = 'uidvalidity'")
    row = cursor.fetchone()
    uidvalidity = row[0] if row else None

    return last_uid, uidvalidity


def save_sync_state(conn, last_uid, uidvalidity):
    """保存同步状态"""
    cursor = conn.cursor()
    cursor.execute(
        "INSERT OR REPLACE INTO sync_state (key, value) VALUES (?, ?)",
        ('last_uid', str(last_uid))
    )
    cursor.execute(
        "INSERT OR REPLACE INTO sync_state (key, value) VALUES (?, ?)",
        ('uidvalidity', str(uidvalidity))
    )
    conn.commit()


def get_or_create_fund_id(conn, product_code, product_name=None):
    """获取或创建基金的 fund_id（基于 fund_code 全局唯一，按首次录入时间自增）"""
    cursor = conn.cursor()
    cursor.execute('SELECT fund_id FROM funds WHERE fund_code = ?', (product_code,))
    row = cursor.fetchone()
    if row:
        return row[0]
    cursor.execute(
        'INSERT INTO funds (fund_code, fund_name) VALUES (?, ?)',
        (product_code, product_name)
    )
    return cursor.lastrowid


def insert_email_source(conn, email_subject, email_sender, email_date, filename, sheet_name):
    """插入邮件来源记录，返回 source_id"""
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO email_sources (邮件主题, 邮件发件人, 邮件日期, 附件文件名, sheet名称)
        VALUES (?, ?, ?, ?, ?)
    ''', (email_subject, email_sender, email_date, filename, sheet_name))
    return cursor.lastrowid


def log_extraction_failure(conn, email_subject, email_sender, email_date,
                           filename, sheet_name, reason):
    """将提取或识别失败的附件信息写入 extraction_failures 表"""
    try:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO extraction_failures
            (邮件主题, 邮件发件人, 邮件日期, 附件文件名, sheet名称, 失败原因)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (email_subject, email_sender, email_date, filename, sheet_name, reason))
        conn.commit()
    except Exception as e:
        # 写入失败日志不应中断主流程
        conn.rollback()
        print(f"\n  [警告] 写入失败日志时出错: {e}")


def decode_str(s):
    """解码邮件头部信息"""
    value, charset = decode_header(s)[0]
    if charset:
        try:
            value = value.decode(charset)
        except:
            value = value.decode('utf-8', errors='ignore')
    elif isinstance(value, bytes):
        value = value.decode('utf-8', errors='ignore')
    return value


def get_attachment_filename(part):
    """获取附件文件名"""
    filename = part.get_filename()
    if filename:
        # 解码文件名
        decoded_filename = decode_header(filename)[0]
        if isinstance(decoded_filename[0], bytes):
            charset = decoded_filename[1]
            if charset:
                filename = decoded_filename[0].decode(charset, errors='ignore')
            else:
                filename = decoded_filename[0].decode('utf-8', errors='ignore')
        else:
            filename = decoded_filename[0]
    return filename


def extract_excel_attachments(msg, failed_extractions):
    """Extract every worksheet, keeping exact attachment/sheet provenance."""
    dataframes = []
    has_excel = False
    for part in msg.walk():
        filename = get_attachment_filename(part)
        if not filename or not filename.lower().endswith(('.xls', '.xlsx', '.xlsm')):
            continue
        has_excel = True
        try:
            with pd.ExcelFile(BytesIO(part.get_payload(decode=True))) as workbook:
                for sheet_name in workbook.sheet_names:
                    try:
                        raw = pd.read_excel(workbook, sheet_name=sheet_name, header=None)
                        extracted = extract_and_normalize(raw)
                        if not extracted:
                            raise ValueError('无法识别数据格式')
                        dataframes.append({'filename': filename, 'sheet_name': sheet_name,
                                           'data': pd.DataFrame(extracted), 'extracted_data': extracted})
                    except Exception as exc:
                        failed_extractions.append({'filename': filename, 'sheet_name': sheet_name, 'reason': str(exc)})
        except Exception as exc:
            failed_extractions.append({'filename': filename, 'sheet_name': '', 'reason': str(exc)})
    return dataframes, has_excel


def normalize_nav_date_for_db(value):
    """Return a real NAV date as ISO text, or ``None`` when it is invalid."""
    from fund_store import normalize_nav_date
    return normalize_nav_date(value)


def is_valid_nav_date(value):
    """Return whether a NAV date has a supported shape and is a real date."""
    return normalize_nav_date_for_db(value) is not None


def insert_data_to_db(conn, df, failed_inserts, source_id=None):
    """将DataFrame数据插入数据库（仅插入核心字段）

    Args:
        conn: 数据库连接
        df: 要插入的数据框
        failed_inserts: 失败记录列表
        source_id: 对应的 email_sources 表主键（可选）

    Returns:
        inserted_count: 插入成功的数量
        skipped_count: 跳过的数量
    """
    cursor = conn.cursor()
    inserted_count = 0
    skipped_count = 0

    for _, row in df.iterrows():
        try:
            from fund_store import positive_number
            product_code = row.get('产品代码')
            code_missing = product_code is None or pd.isna(product_code) or not str(product_code).strip()
            accum_nav = row.get('累计单位净值')
            if accum_nav is not None and pd.isna(accum_nav):
                accum_nav = None
            numeric_invalid = (not positive_number(row.get('单位净值')) or
                               (accum_nav is not None and not positive_number(accum_nav)))
            if code_missing or numeric_invalid:
                failed_inserts.append({'product_name': row.get('产品名称'), 'product_code': product_code,
                                       'reason': '数据校验失败: 产品代码缺失或净值不是有限正数', 'data': row.to_dict()})
                skipped_count += 1
                continue
            # 验证必需字段
            raw_nav_date = row.get('净值日期')
            nav_date_missing = (
                raw_nav_date is None
                or pd.isna(raw_nav_date)
                or not str(raw_nav_date).strip()
            )
            normalized_nav_date = normalize_nav_date_for_db(raw_nav_date)
            invalid_nav_date = not nav_date_missing and normalized_nav_date is None
            if (not row.get('产品代码') or nav_date_missing or
                    not row.get('单位净值') or invalid_nav_date):
                reason = "数据校验失败: "
                missing_fields = []
                if not row.get('产品代码'):
                    missing_fields.append('产品代码')
                if nav_date_missing:
                    missing_fields.append('净值日期')
                elif invalid_nav_date:
                    missing_fields.append('净值日期格式无效')
                if not row.get('单位净值'):
                    missing_fields.append('单位净值')
                reason += ', '.join(missing_fields)

                failed_inserts.append({
                    'product_name': row.get('产品名称'),
                    'product_code': row.get('产品代码'),
                    'reason': reason,
                    'data': row.to_dict()
                })
                skipped_count += 1
                continue

            # 历史邮件数据中仍有 YYYYMMDD；同时检查两种表示，避免同日逻辑重复。
            compact_nav_date = normalized_nav_date.replace('-', '')
            existing_nav = cursor.execute(
                '''
                SELECT id, unit_nav, accum_nav
                FROM fund_nav_data
                WHERE fund_code = ? AND nav_date IN (?, ?)
                LIMIT 1
                ''',
                (row.get('产品代码'), normalized_nav_date, compact_nav_date)
            ).fetchone()
            if existing_nav:
                existing_accum = float(existing_nav[2]) if existing_nav[2] is not None else None
                incoming_accum = float(accum_nav) if accum_nav is not None else None
                if float(existing_nav[1]) != float(row.get('单位净值')) or existing_accum != incoming_accum:
                    from fund_store import record_ingestion_conflict
                    record_ingestion_conflict(conn,existing_nav[0],row.to_dict(),source_id)
                    failed_inserts.append({'product_name':row.get('产品名称'),'product_code':product_code,
                                           'reason':'同基金同日期净值冲突，已保留证据且未覆盖原值','data':row.to_dict()})
                skipped_count += 1
                continue

            # 获取或创建该产品的 fund_id（优先用于唯一标识基金）
            fund_id = get_or_create_fund_id(conn, row.get('产品代码'), row.get('产品名称'))

            cursor.execute('''
                INSERT OR IGNORE INTO fund_nav_data
                (fund_id, fund_name, fund_code, nav_date, unit_nav, accum_nav, source_id, data_source)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                fund_id,
                row.get('产品名称'),
                row.get('产品代码'),
                normalized_nav_date,
                row.get('单位净值'),
                accum_nav,
                source_id,
                'email'
            ))

            if cursor.rowcount > 0:
                inserted_count += 1
            else:
                # 数据已存在
                skipped_count += 1

        except Exception as e:
            reason = f"插入数据库失败: {str(e)}"
            failed_inserts.append({
                'product_name': row.get('产品名称'),
                'product_code': row.get('产品代码'),
                'reason': reason,
                'data': row.to_dict()
            })
            skipped_count += 1

    return inserted_count, skipped_count


def compute_adjusted_nav(conn, product_code):
    """Compatibility entry point using canonical adj_nav and full-series math."""
    from fund_store import recalculate_adj_nav
    recalculate_adj_nav(conn, product_code)
    conn.commit()


def print_failure_report(failed_extractions, failed_inserts):
    """打印失败报告

    Args:
        failed_extractions: 提取失败的记录列表
        failed_inserts: 插入失败的记录列表
    """
    print("\n" + "="*80)
    print("失败案例分析报告")
    print("="*80)

    if not failed_extractions and not failed_inserts:
        print("\n所有有Excel附件的邮件都已成功提取并存储！")
        return

    # 1. 处理提取失败的记录
    if failed_extractions:
        print("\n" + "-"*80)
        print("1. 数据提取失败的邮件（无法识别Excel格式）")
        print("-"*80)

        # 按基金名称去重，只显示唯一的失败案例
        unique_failures = {}
        for record in failed_extractions:
            email_subject = record.get('email_subject', '未知')
            filename = record.get('filename', '未知')

            # 使用文件名作为唯一标识
            key = filename
            if key not in unique_failures:
                unique_failures[key] = record

        for idx, (key, record) in enumerate(unique_failures.items(), 1):
            email_subject = record.get('email_subject', '未知')
            filename = record.get('filename', '未知')
            reason = record.get('reason', '未知原因')

            print(f"\n  [{idx}] {filename}")
            print(f"      邮件: {email_subject}")
            print(f"      原因: {reason}")

        print(f"\n  小计: {len(unique_failures)} 个不同的Excel文件无法提取")

    # 2. 处理插入失败的记录
    if failed_inserts:
        print("\n" + "-"*80)
        print("2. 数据插入数据库失败的记录")
        print("-"*80)

        # 按产品代码去重
        unique_failures = {}
        for record in failed_inserts:
            product_code = record.get('product_code', '未知')

            # 使用产品代码作为唯一标识
            key = product_code if product_code and product_code != '未知' else record.get('filename', '未知')
            if key not in unique_failures:
                unique_failures[key] = record

        for idx, (key, record) in enumerate(unique_failures.items(), 1):
            product_name = record.get('product_name', '未知')
            product_code = record.get('product_code', '未知')
            reason = record.get('reason', '未知原因')
            filename = record.get('filename', '未知')

            print(f"\n  [{idx}] 产品: {product_name}")
            print(f"      产品代码: {product_code}")
            print(f"      文件名: {filename}")
            print(f"      原因: {reason}")

        print(f"\n  小计: {len(unique_failures)} 个基金的数据插入失败")

    # 3. 总结
    print("\n" + "="*80)
    print("问题总结")
    print("="*80)

    total_failures = len(set([r.get('filename') for r in failed_extractions])) + \
                     len(set([r.get('product_code') for r in failed_inserts if r.get('product_code')]))

    print(f"\n共有 {total_failures} 个不同的失败案例")

    if failed_extractions:
        print("\n提取失败可能原因:")
        print("  - Excel格式不符合智能识别规则")
        print("  - 关键字段缺失或名称不匹配")
        print("  - 建议: 查看上述文件，手动检查格式，更新 smart_extractor.py 识别规则")

    if failed_inserts:
        print("\n插入失败可能原因:")
        print("  - 缺少必需字段（产品代码、净值日期、单位净值）")
        print("  - 数据格式不正确")
        print("  - 建议: 检查智能提取器是否正确识别了所有必需字段")

    print("\n" + "="*80)


def query_and_display_data(conn):
    """查询并显示数据库统计信息（以 fund_id 为主键排序）"""
    cursor = conn.cursor()

    cursor.execute('''
        SELECT f.fund_id, f.fund_code, f.fund_name,
               COUNT(n.id), MIN(n.nav_date), MAX(n.nav_date)
        FROM funds f
        LEFT JOIN fund_nav_data n ON f.fund_id = n.fund_id
        GROUP BY f.fund_id
        ORDER BY f.fund_id
    ''')
    rows = cursor.fetchall()

    print("\n" + "="*80)
    print("数据库统计信息")
    print("="*80)

    total_count = sum(r[3] or 0 for r in rows)
    print(f"\n数据库中共有 {total_count} 条净值记录")
    print(f"涵盖 {len(rows)} 个不同的基金产品")

    print("\n" + "-"*80)
    print("各基金净值记录统计:")
    print("-"*80)

    for fund_id, product_code, product_name, count, min_date, max_date in rows:
        label = f"{product_code} - {product_name}" if product_name else product_code
        print(f"\n[{fund_id:03d}] {label}")
        print(f"  记录数: {count} 条")
        print(f"  日期范围: {min_date} ~ {max_date}")

    print("\n" + "="*80)


def get_email_content(msg):
    """递归解析邮件内容"""
    content = ""

    if msg.is_multipart():
        # 如果邮件是多部分的，递归解析每一部分
        for part in msg.walk():
            content_type = part.get_content_type()
            content_disposition = str(part.get("Content-Disposition"))

            # 跳过附件
            if "attachment" in content_disposition:
                continue

            # 获取文本内容
            if content_type == "text/plain":
                try:
                    body = part.get_payload(decode=True)
                    charset = part.get_content_charset()
                    if charset:
                        content += body.decode(charset, errors='ignore')
                    else:
                        content += body.decode('utf-8', errors='ignore')
                except:
                    pass
            elif content_type == "text/html":
                try:
                    body = part.get_payload(decode=True)
                    charset = part.get_content_charset()
                    if charset:
                        html_content = body.decode(charset, errors='ignore')
                    else:
                        html_content = body.decode('utf-8', errors='ignore')
                    # 如果没有纯文本内容，则使用HTML内容
                    if not content:
                        content += "\n[HTML内容]:\n" + html_content
                except:
                    pass
    else:
        # 单部分邮件
        content_type = msg.get_content_type()
        body = msg.get_payload(decode=True)
        charset = msg.get_content_charset()

        try:
            if charset:
                content = body.decode(charset, errors='ignore')
            else:
                content = body.decode('utf-8', errors='ignore')
        except:
            content = str(body)

    return content


def connect_and_fetch_email(email_user, email_pwd, db_path, **kwargs):
    """Use the same durable sync service as API and scheduler callers."""
    from sync_service import run_email_sync
    return run_email_sync(email_user, email_pwd, db_path, **kwargs)


def main():
    """主函数"""
    import argparse
    import json
    parser = argparse.ArgumentParser(description='增量同步基金邮件，或重试指定失败记录/邮件 UID')
    parser.add_argument('--db', help='数据库文件路径（默认读取 DB_PATH）')
    retry = parser.add_mutually_exclusive_group()
    retry.add_argument('--retry-failure', type=int, help='重试失败记录 ID')
    retry.add_argument('--retry-uid', type=int, help='重试当前收件箱中的单个邮件 UID')
    args = parser.parse_args()
    print("163邮箱基金净值数据采集程序（智能版）")
    print("="*60)
    print("功能说明:")
    print("1. 增量拉取邮件（基于IMAP UID）")
    print("2. 智能识别并提取Excel附件中的核心数据")
    print("3. 支持多种Excel格式自动识别")
    print("4. 将数据存储到SQLite数据库")
    print("5. 将无法识别的附件持久化记录到 extraction_failures 表")
    print("="*60)
    print()

    # 从 .env 文件加载环境变量
    load_dotenv()

    # SQLite 数据库路径（默认 fund_data.db）
    db_path = args.db or os.getenv('DB_PATH', 'fund_data.db')

    # 163邮箱登录信息
    email_user = os.getenv('EMAIL_USER', '')
    email_pwd = os.getenv('EMAIL_PASSWORD', '')

    if not email_user or not email_pwd:
        print("错误: 环境变量 EMAIL_USER 和 EMAIL_PASSWORD 不能为空，请检查 .env 文件！")
        raise SystemExit(1)

    if "@163.com" not in email_user:
        print("警告: 邮箱地址似乎不是163邮箱")

    # 连接并获取邮件
    result = connect_and_fetch_email(email_user, email_pwd, db_path, trigger='cli',
                                    retry_failure_id=args.retry_failure, retry_uid=args.retry_uid)
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
