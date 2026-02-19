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
    """初始化 SQLite 数据库，创建所有必要的表"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # 基金净值数据表
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS fund_nav_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            产品名称 TEXT,
            产品代码 TEXT NOT NULL,
            净值日期 TEXT NOT NULL,
            单位净值 REAL NOT NULL,
            累计单位净值 REAL,
            插入时间 DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(产品代码, 净值日期)
        )
    ''')

    # 创建索引以提高查询性能
    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_product_code
        ON fund_nav_data(产品代码)
    ''')

    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_nav_date
        ON fund_nav_data(净值日期)
    ''')

    # 增量同步状态表：记录上次处理到的最大UID和UIDVALIDITY
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS sync_state (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    ''')

    # 提取/识别失败记录表：持久化所有无法处理的邮件附件信息
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS extraction_failures (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            失败时间 DATETIME DEFAULT CURRENT_TIMESTAMP,
            邮件主题 TEXT,
            邮件发件人 TEXT,
            邮件日期 TEXT,
            附件文件名 TEXT,
            sheet名称 TEXT,
            失败原因 TEXT
        )
    ''')

    conn.commit()
    print(f"SQLite 数据库初始化成功: {db_path}")
    return conn


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
    """提取邮件中的Excel附件并使用智能提取器读取数据

    Args:
        msg: 邮件消息对象
        failed_extractions: 失败记录列表，用于记录提取失败的附件

    Returns:
        dataframes: 成功提取的数据列表
    """
    dataframes = []
    has_excel = False  # 标记是否有Excel附件

    if msg.is_multipart():
        for part in msg.walk():
            content_disposition = str(part.get("Content-Disposition"))
            content_type = part.get_content_type()

            # 检查是否是附件
            if "attachment" in content_disposition:
                filename = get_attachment_filename(part)

                if filename:
                    # 检查是否是Excel文件
                    if filename.lower().endswith(('.xls', '.xlsx', '.xlsm')):
                        has_excel = True

                        try:
                            # 获取附件数据
                            attachment_data = part.get_payload(decode=True)

                            # 直接从内存读取Excel，不保存到本地
                            extraction_success = False
                            try:
                                # 使用BytesIO将二进制数据转换为文件对象
                                excel_buffer = BytesIO(attachment_data)
                                # 重要：使用 header=None 读取原始数据
                                df = pd.read_excel(excel_buffer, header=None)

                                # 使用智能提取器提取数据（返回列表）
                                extracted_data = extract_and_normalize(df)

                                if extracted_data:
                                    # extracted_data 是 list of dict，直接构建多行DataFrame
                                    df_normalized = pd.DataFrame(extracted_data)

                                    dataframes.append({
                                        'filename': filename,
                                        'data': df_normalized,
                                        'sheet_name': 'default',
                                        'extracted_data': extracted_data
                                    })
                                    extraction_success = True
                                else:
                                    failed_extractions.append({
                                        'filename': filename,
                                        'sheet_name': 'default',
                                        'reason': '无法识别数据格式',
                                        'product_name': None,
                                        'product_code': None
                                    })

                            except Exception as e:
                                # 如果有多个sheet，尝试读取所有sheet
                                try:
                                    excel_buffer = BytesIO(attachment_data)
                                    excel_file = pd.ExcelFile(excel_buffer)
                                    sheet_success = False
                                    for sheet_name in excel_file.sheet_names:
                                        excel_buffer = BytesIO(attachment_data)
                                        df = pd.read_excel(excel_buffer, sheet_name=sheet_name, header=None)

                                        # 使用智能提取器（返回列表）
                                        extracted_data = extract_and_normalize(df)

                                        if extracted_data:
                                            df_normalized = pd.DataFrame(extracted_data)
                                            dataframes.append({
                                                'filename': filename,
                                                'data': df_normalized,
                                                'sheet_name': sheet_name,
                                                'extracted_data': extracted_data
                                            })
                                            sheet_success = True
                                            extraction_success = True
                                        else:
                                            failed_extractions.append({
                                                'filename': filename,
                                                'sheet_name': sheet_name,
                                                'reason': '无法识别数据格式',
                                                'product_name': None,
                                                'product_code': None
                                            })

                                except Exception as e2:
                                    if not extraction_success:
                                        failed_extractions.append({
                                            'filename': filename,
                                            'sheet_name': 'unknown',
                                            'reason': f'读取工作表失败: {str(e2)}',
                                            'product_name': None,
                                            'product_code': None
                                        })

                        except Exception as e:
                            failed_extractions.append({
                                'filename': filename,
                                'sheet_name': 'unknown',
                                'reason': f'处理附件失败: {str(e)}',
                                'product_name': None,
                                'product_code': None
                            })

    return dataframes, has_excel


def insert_data_to_db(conn, df, failed_inserts):
    """将DataFrame数据插入数据库（仅插入核心字段）

    Args:
        conn: 数据库连接
        df: 要插入的数据框
        failed_inserts: 失败记录列表

    Returns:
        inserted_count: 插入成功的数量
        skipped_count: 跳过的数量
    """
    cursor = conn.cursor()
    inserted_count = 0
    skipped_count = 0

    for _, row in df.iterrows():
        try:
            # 验证必需字段
            if not row.get('产品代码') or not row.get('净值日期') or not row.get('单位净值'):
                reason = "缺少必需字段: "
                missing_fields = []
                if not row.get('产品代码'):
                    missing_fields.append('产品代码')
                if not row.get('净值日期'):
                    missing_fields.append('净值日期')
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

            cursor.execute('''
                INSERT OR IGNORE INTO fund_nav_data
                (产品名称, 产品代码, 净值日期, 单位净值, 累计单位净值)
                VALUES (?, ?, ?, ?, ?)
            ''', (
                row.get('产品名称'),
                row.get('产品代码'),
                row.get('净值日期'),
                row.get('单位净值'),
                row.get('累计单位净值')
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

    conn.commit()
    return inserted_count, skipped_count


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
    """查询并显示数据库统计信息"""
    cursor = conn.cursor()

    # 查询所有产品代码
    cursor.execute('SELECT DISTINCT 产品代码 FROM fund_nav_data ORDER BY 产品代码')
    product_codes = [row[0] for row in cursor.fetchall()]

    print("\n" + "="*80)
    print("数据库统计信息")
    print("="*80)

    # 统计总记录数
    cursor.execute('SELECT COUNT(*) FROM fund_nav_data')
    total_count = cursor.fetchone()[0]

    print(f"\n数据库中共有 {total_count} 条净值记录")
    print(f"涵盖 {len(product_codes)} 个不同的基金产品")

    # 显示每个产品的记录数
    print("\n" + "-"*80)
    print("各基金净值记录统计:")
    print("-"*80)

    for product_code in product_codes:
        # 查询该产品的记录数和日期范围
        cursor.execute('''
            SELECT 产品名称, COUNT(*), MIN(净值日期), MAX(净值日期)
            FROM fund_nav_data
            WHERE 产品代码 = ?
            GROUP BY 产品名称
        ''', (product_code,))

        row = cursor.fetchone()
        if row:
            product_name, count, min_date, max_date = row
            if product_name:
                print(f"\n{product_code} - {product_name}")
            else:
                print(f"\n{product_code}")
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


def connect_and_fetch_email(email_user, email_pwd, db_path):
    """连接到163邮箱并增量拉取新邮件，提取附件数据到数据库"""

    # 初始化数据库，读取上次同步状态
    conn = init_database(db_path)
    last_uid, stored_uidvalidity = get_sync_state(conn)

    # 步骤1: 连接到IMAP服务器
    print("正在连接到163邮箱IMAP服务器...")
    imap_host = "imap.163.com"

    try:
        # 使用SSL连接
        imap_client = imaplib.IMAP4_SSL(imap_host, 993)
        print(f"成功连接到 {imap_host}")
    except Exception as e:
        print(f"连接失败: {e}")
        return

    # 步骤2: 登录
    print(f"正在登录邮箱: {email_user}")
    try:
        imap_client.login(email_user, email_pwd)
        print("登录成功！")
    except Exception as e:
        print(f"登录失败: {e}")
        print("\n提示：")
        print("1. 请确保已在163邮箱设置中开启IMAP服务")
        print("2. 使用的是授权码而不是登录密码")
        print("3. 获取授权码路径：邮箱设置 -> POP3/SMTP/IMAP -> 开启IMAP服务 -> 获取授权码")
        return

    # 步骤3: 登录后立即发送ID命令（163邮箱必需，必须在SELECT之前）
    print("正在发送客户端标识信息...")
    try:
        # 注册ID命令（如果还未注册）
        if 'ID' not in imaplib.Commands:
            imaplib.Commands['ID'] = ('AUTH',)

        # 构造ID命令参数 - 163邮箱要求的格式
        # 这是关键步骤，用于避免"Unsafe Login"错误
        args = ("name", "myclient", "contact", email_user, "version", "1.0.0", "vendor", "myclient")

        # 格式化为IMAP ID命令格式
        typ, dat = imap_client._simple_command('ID', '("' + '" "'.join(args) + '")')

        if typ == 'OK':
            print("客户端标识信息发送成功")
        else:
            print(f"警告: ID命令返回状态: {typ}")
    except Exception as e:
        print(f"警告: 发送ID命令时出错: {e}")
        print("继续尝试访问邮箱...")

    # 步骤4: 选择收件箱
    print("\n正在打开收件箱...")
    try:
        # 使用SELECT命令打开收件箱
        status, messages = imap_client.select("INBOX")

        # 检查返回状态
        if status != 'OK':
            print(f"打开收件箱失败，服务器返回状态: {status}")
            print(f"详细信息: {messages}")
            imap_client.logout()
            return

        # 解析邮件数量
        try:
            total_messages = int(messages[0])
            print(f"收件箱中共有 {total_messages} 封邮件")
        except (ValueError, TypeError) as e:
            print(f"无法解析邮件数量，服务器返回: {messages}")
            print("\n可能的原因和解决方法：")
            print("1. 163邮箱的安全限制 - 需要使用正确的授权码（不是登录密码）")
            print("2. 需要在163邮箱网页端确认开启IMAP服务")
            print("3. 授权码可能已过期，需要重新生成")
            print("4. 可能需要授权第三方客户端访问，请访问：")
            print(f"   http://config.mail.163.com/settings/imap/index.jsp?uid={email_user}")
            print("   按照页面提示完成短信验证授权")
            imap_client.logout()
            return

        # 获取服务器的 UIDVALIDITY，用于检测邮箱是否被重建
        uidvalidity_list = imap_client.untagged_responses.get('UIDVALIDITY', [b'0'])
        server_uidvalidity = uidvalidity_list[0].decode() if uidvalidity_list else '0'

    except Exception as e:
        print(f"打开收件箱失败: {e}")
        imap_client.logout()
        return

    if total_messages == 0:
        print("收件箱为空！")
        imap_client.logout()
        return

    # 步骤5: 增量拉取新邮件（基于 IMAP UID）
    print("\n开始处理邮件...")
    print("="*80)

    try:
        # 判断是否需要全量扫描
        # 条件：首次运行（last_uid==0）或 UIDVALIDITY 变化（邮箱被重建）
        full_scan = (last_uid == 0 or server_uidvalidity != stored_uidvalidity)

        if full_scan:
            if server_uidvalidity != stored_uidvalidity and stored_uidvalidity is not None:
                print(f"检测到邮箱 UIDVALIDITY 变化，执行全量扫描")
            else:
                print("首次运行，执行全量扫描")
            status, uid_data = imap_client.uid('search', None, 'ALL')
        else:
            print(f"增量模式：拉取 UID > {last_uid} 的新邮件")
            status, uid_data = imap_client.uid('search', None, f'UID {last_uid + 1}:*')

        uid_list = uid_data[0].split()  # 每个元素是 bytes 类型的 UID

        if not uid_list:
            print("没有新邮件需要处理。")
            save_sync_state(conn, last_uid, server_uidvalidity)
            query_and_display_data(conn)
            conn.close()
            imap_client.close()
            imap_client.logout()
            return

        # 统计信息
        total_processed = 0
        emails_with_attachments = 0
        emails_without_attachments = []
        total_data_inserted = 0

        # 失败追踪
        failed_extraction_emails = []  # 有Excel但提取失败的邮件
        failed_insert_records = []  # 提取成功但插入失败的记录

        total_emails = len(uid_list)
        max_uid = last_uid  # 记录本次处理到的最大 UID
        print(f"共 {total_emails} 封新邮件需要处理\n")

        # 遍历每封邮件（使用 UID fetch）
        for idx, uid in enumerate(uid_list, 1):
            # 显示进度条
            progress = idx / total_emails * 100
            bar_length = 50
            filled_length = int(bar_length * idx // total_emails)
            bar = '█' * filled_length + '-' * (bar_length - filled_length)
            print(f'\r进度: [{bar}] {progress:.1f}% ({idx}/{total_emails})', end='', flush=True)

            try:
                # 使用 UID fetch 获取邮件原文
                status, email_data = imap_client.uid('fetch', uid, '(RFC822)')

                # 解析邮件
                raw_email = email_data[0][1]
                msg = email.message_from_bytes(raw_email)

                # 获取邮件基本信息
                subject_header = msg.get("Subject")
                subject = decode_str(subject_header) if subject_header else "(无主题)"

                from_header = msg.get("From")
                sender = decode_str(from_header) if from_header else "(未知发件人)"

                date_header = msg.get("Date")

                # 提取Excel附件
                email_failed_extractions = []
                dataframes, has_excel = extract_excel_attachments(msg, email_failed_extractions)

                if dataframes:
                    emails_with_attachments += 1

                    # 将数据插入数据库
                    for df_info in dataframes:
                        df = df_info['data']
                        email_failed_inserts = []
                        inserted, skipped = insert_data_to_db(conn, df, email_failed_inserts)
                        total_data_inserted += inserted

                        # 记录插入失败的记录（排除重复数据）
                        for fail_record in email_failed_inserts:
                            fail_record['email_subject'] = subject
                            fail_record['email_date'] = date_header
                            fail_record['filename'] = df_info['filename']
                            failed_insert_records.append(fail_record)
                            # 持久化到数据库（仅记录真正的失败，跳过重复数据）
                            if '缺少必需字段' in fail_record.get('reason', '') or \
                               '插入数据库失败' in fail_record.get('reason', ''):
                                log_extraction_failure(
                                    conn, subject, sender, date_header,
                                    df_info['filename'], '',
                                    fail_record.get('reason', '')
                                )

                # 记录提取失败的附件
                if email_failed_extractions:
                    for fail_record in email_failed_extractions:
                        fail_record['email_subject'] = subject
                        fail_record['email_date'] = date_header
                        failed_extraction_emails.append(fail_record)
                        # 持久化到数据库
                        log_extraction_failure(
                            conn, subject, sender, date_header,
                            fail_record.get('filename', ''),
                            fail_record.get('sheet_name', ''),
                            fail_record.get('reason', '')
                        )

                # 如果有Excel但是都提取失败了
                if has_excel and not dataframes:
                    pass  # 已在failed_extraction_emails中记录
                elif not has_excel:
                    # 没有附件的邮件
                    emails_without_attachments.append({
                        'id': uid.decode(),
                        'subject': subject,
                        'sender': sender,
                        'date': date_header
                    })

                # 更新已处理到的最大 UID
                max_uid = max(max_uid, int(uid))
                total_processed += 1

            except Exception as e:
                print(f"\n  [错误] 处理邮件 UID={uid.decode()} 时出错: {e}")
                import traceback
                traceback.print_exc()

        # 完成进度显示
        print()  # 换行

        # 保存同步状态（记录本次处理到的最大 UID）
        save_sync_state(conn, max_uid, server_uidvalidity)
        print(f"同步状态已更新：last_uid={max_uid}")

        # 显示统计信息
        print("\n" + "="*80)
        print("处理完成!")
        print("="*80)
        print(f"总共处理邮件: {total_processed} 封")
        print(f"有Excel附件: {emails_with_attachments} 封")
        print(f"无Excel附件: {len(emails_without_attachments)} 封")
        print(f"成功插入数据: {total_data_inserted} 条")

        # 显示失败报告
        print_failure_report(failed_extraction_emails, failed_insert_records)

        # 查询并显示数据库中的所有数据
        query_and_display_data(conn)

    except Exception as e:
        print(f"遍历邮件失败: {e}")
        import traceback
        traceback.print_exc()

    # 步骤6: 关闭连接
    print("\n正在关闭连接...")
    try:
        conn.close()
        imap_client.close()
        imap_client.logout()
        print("已断开所有连接")
    except Exception as e:
        print(f"断开连接时出错: {e}")


def main():
    """主函数"""
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
    db_path = os.getenv('DB_PATH', 'fund_data.db')

    # 163邮箱登录信息
    email_user = os.getenv('EMAIL_USER', '')
    email_pwd = os.getenv('EMAIL_PASSWORD', '')

    if not email_user or not email_pwd:
        print("错误: 环境变量 EMAIL_USER 和 EMAIL_PASSWORD 不能为空，请检查 .env 文件！")
        return

    if "@163.com" not in email_user:
        print("警告: 邮箱地址似乎不是163邮箱")

    # 连接并获取邮件
    connect_and_fetch_email(email_user, email_pwd, db_path)


if __name__ == "__main__":
    main()
