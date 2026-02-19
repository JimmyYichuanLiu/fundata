#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据质量检测脚本

对 fund_data.db 执行三类异常检测，并将校准后的数据写入 fund_clean.db（对外展示库）。

检测项目：
1. 净值超范围：单位净值 / 累计单位净值 > 5
2. 同名产品多代码：相同产品名称对应多个产品代码
3. 重复净值日期：同一产品在同一日期存在多条记录

每次运行前删除旧的 fund_clean.db，重建。
"""

import os
import sqlite3
from dotenv import load_dotenv


def check1_nav_out_of_range(src_conn):
    """检测1：单位净值或累计单位净值 > 5"""
    cursor = src_conn.cursor()
    cursor.execute('''
        SELECT f.id, f.产品名称, f.产品代码, f.净值日期, f.单位净值, f.累计单位净值,
               e.邮件主题, e.邮件发件人, e.邮件日期, e.附件文件名, e.sheet名称
        FROM fund_nav_data f
        LEFT JOIN email_sources e ON f.source_id = e.id
        WHERE f.单位净值 > 5 OR (f.累计单位净值 IS NOT NULL AND f.累计单位净值 > 5)
    ''')
    return cursor.fetchall()


def check2_same_name_multi_code(src_conn):
    """检测2：相同产品名称对应多个产品代码"""
    cursor = src_conn.cursor()
    cursor.execute('''
        SELECT 产品名称, GROUP_CONCAT(DISTINCT 产品代码), COUNT(DISTINCT 产品代码)
        FROM fund_nav_data
        WHERE 产品名称 IS NOT NULL
        GROUP BY 产品名称
        HAVING COUNT(DISTINCT 产品代码) > 1
    ''')
    conflicts = cursor.fetchall()

    # 为每个冲突产品名称追溯来源
    details = []
    for product_name, codes_str, code_count in conflicts:
        codes = codes_str.split(',')
        sources = []
        for code in codes:
            cursor.execute('''
                SELECT DISTINCT e.邮件主题, e.邮件发件人, e.邮件日期, e.附件文件名
                FROM fund_nav_data f
                LEFT JOIN email_sources e ON f.source_id = e.id
                WHERE f.产品名称 = ? AND f.产品代码 = ?
                LIMIT 3
            ''', (product_name, code.strip()))
            rows = cursor.fetchall()
            sources.append({'code': code.strip(), 'emails': rows})
        details.append({
            'product_name': product_name,
            'codes': codes,
            'code_count': code_count,
            'sources': sources
        })
    return details


def check3_duplicate_nav_dates(src_conn):
    """检测3：重复净值日期（同产品代码同日期多条记录）"""
    cursor = src_conn.cursor()
    cursor.execute('''
        SELECT 产品名称, 产品代码, 净值日期, COUNT(*) as cnt
        FROM fund_nav_data
        GROUP BY 产品名称, 产品代码, 净值日期
        HAVING COUNT(*) > 1
    ''')
    return cursor.fetchall()


def build_clean_db(src_conn, clean_db_path):
    """构建 fund_clean.db：排除异常记录，反范式化来源信息"""
    # 统计源数据总数
    cursor = src_conn.cursor()
    cursor.execute('SELECT COUNT(*) FROM fund_nav_data')
    total_src = cursor.fetchone()[0]

    # 查询需要写入的干净数据
    cursor.execute('''
        SELECT f.产品名称, f.产品代码, f.净值日期, f.单位净值, f.累计单位净值,
               e.邮件主题, e.邮件发件人, e.邮件日期, e.附件文件名, e.sheet名称, f.source_id
        FROM fund_nav_data f
        LEFT JOIN email_sources e ON f.source_id = e.id
        WHERE f.单位净值 <= 5
          AND (f.累计单位净值 IS NULL OR f.累计单位净值 <= 5)
          AND f.id = (SELECT MIN(f2.id) FROM fund_nav_data f2
                      WHERE f2.产品代码 = f.产品代码 AND f2.净值日期 = f.净值日期)
        ORDER BY f.产品代码, f.净值日期
    ''')
    clean_rows = cursor.fetchall()

    # 删除旧的 clean DB，重建
    if os.path.exists(clean_db_path):
        os.remove(clean_db_path)

    clean_conn = sqlite3.connect(clean_db_path)
    clean_cursor = clean_conn.cursor()
    clean_cursor.execute('''
        CREATE TABLE IF NOT EXISTS fund_nav_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            产品名称 TEXT,
            产品代码 TEXT NOT NULL,
            净值日期 TEXT NOT NULL,
            单位净值 REAL NOT NULL,
            累计单位净值 REAL,
            插入时间 DATETIME DEFAULT CURRENT_TIMESTAMP,
            source_id INTEGER,
            来源邮件主题 TEXT,
            来源发件人 TEXT,
            来源邮件日期 TEXT,
            来源附件文件名 TEXT,
            来源sheet名称 TEXT,
            UNIQUE(产品代码, 净值日期)
        )
    ''')

    clean_cursor.executemany('''
        INSERT OR IGNORE INTO fund_nav_data
        (产品名称, 产品代码, 净值日期, 单位净值, 累计单位净值,
         来源邮件主题, 来源发件人, 来源邮件日期, 来源附件文件名, 来源sheet名称, source_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', clean_rows)
    clean_conn.commit()
    clean_conn.close()

    written = len(clean_rows)
    excluded = total_src - written
    return total_src, written, excluded


def print_report(check1_rows, check2_details, check3_rows, total_src, written, excluded, clean_db_path):
    """打印检测报告"""
    print()
    print("=" * 70)
    print("数据质量检测报告")
    print("=" * 70)

    # 检测1
    print()
    print("【检测1】单位净值 / 累计单位净值 > 5")
    if check1_rows:
        print(f"  发现 {len(check1_rows)} 条异常记录：")
        for row in check1_rows:
            rid, name, code, date, nav, accum_nav, subj, sender, edate, fname, sheet = row
            print(f"  ID={rid} | {code} - {name or '未知'} | {date}")
            print(f"    单位净值={nav}  累计单位净值={accum_nav}")
            if subj or fname:
                print(f"    来源邮件: {subj or '无'} | 发件人: {sender or '无'} | 附件: {fname or '无'}")
    else:
        print("  未发现异常")

    # 检测2
    print()
    print("【检测2】相同产品名称对应多个产品代码")
    if check2_details:
        print(f"  发现 {len(check2_details)} 个产品名称存在代码不一致：")
        for item in check2_details:
            print(f"  产品名称: {item['product_name']}")
            print(f"  对应代码: {', '.join(item['codes'])}（共{item['code_count']}个）")
            print("  来源追溯:")
            for src in item['sources']:
                print(f"    代码 {src['code']}:")
                if src['emails']:
                    for e in src['emails']:
                        subj, sender, edate, fname = e
                        print(f"      邮件: {subj or '无'} | 发件人: {sender or '无'} | 附件: {fname or '无'}")
                else:
                    print("      （来源邮件信息不可用，可能是旧数据）")
    else:
        print("  未发现异常")

    # 检测3
    print()
    print("【检测3】重复净值日期")
    if check3_rows:
        print(f"  发现 {len(check3_rows)} 条重复净值日期：")
        for row in check3_rows:
            name, code, date, cnt = row
            print(f"  {code} - {name or '未知'} | {date} | 重复 {cnt} 次")
    else:
        print("  未发现异常")

    # 构建结果
    print()
    print("构建校准数据库")
    print(f"  源数据库总记录数: {total_src}")
    print(f"  写入校准数据库: {written} 条")
    print(f"  排除异常记录: {excluded} 条")
    print(f"  校准数据库已保存至: {clean_db_path}")
    print()
    print("=" * 70)


def main():
    load_dotenv()

    db_path = os.getenv('DB_PATH', 'fund_data.db')
    clean_db_path = os.getenv('CLEAN_DB_PATH', 'fund_clean.db')

    if not os.path.exists(db_path):
        print(f"错误：源数据库不存在: {db_path}")
        return

    src_conn = sqlite3.connect(db_path)

    # 检查 email_sources 表是否存在（兼容旧库）
    cursor = src_conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='email_sources'")
    has_sources_table = cursor.fetchone() is not None

    if not has_sources_table:
        print("警告：源数据库中不存在 email_sources 表，来源信息将为空。")
        print("请先运行 get_163_email.py 以建立该表。")

    check1_rows = check1_nav_out_of_range(src_conn)
    check2_details = check2_same_name_multi_code(src_conn)
    check3_rows = check3_duplicate_nav_dates(src_conn)
    total_src, written, excluded = build_clean_db(src_conn, clean_db_path)

    src_conn.close()

    print_report(check1_rows, check2_details, check3_rows, total_src, written, excluded, clean_db_path)


if __name__ == "__main__":
    main()
