#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
检查数据库中的问题数据
"""

import sqlite3

def check_database():
    """检查数据库中是否有表头内容被当作数据插入的问题"""

    conn = sqlite3.connect('fund_data.db')
    cursor = conn.cursor()

    # 查找可能的表头数据
    # 这些是常见的列名，不应该出现在数据中
    header_keywords = ['产品名称', '产品代码', '基金名称', '基金代码',
                      '单位净值', '累计单位净值', '净值日期', '日期',
                      '名称', '代码', '净值']

    print("检查数据库中的问题数据...")
    print("="*80)

    problem_count = 0

    for keyword in header_keywords:
        # 检查产品名称字段
        cursor.execute('''
            SELECT id, 产品名称, 产品代码, 净值日期, 单位净值, 累计单位净值
            FROM fund_nav_data
            WHERE 产品名称 = ? OR 产品代码 = ?
        ''', (keyword, keyword))

        rows = cursor.fetchall()

        if rows:
            problem_count += len(rows)
            print(f"\n找到 {len(rows)} 条包含表头关键字 '{keyword}' 的记录:")
            for row in rows:
                print(f"  ID: {row[0]}")
                print(f"    产品名称: {row[1]}")
                print(f"    产品代码: {row[2]}")
                print(f"    净值日期: {row[3]}")
                print(f"    单位净值: {row[4]}")
                print(f"    累计单位净值: {row[5]}")
                print()

    # 检查单位净值是否为非数字
    cursor.execute('''
        SELECT id, 产品名称, 产品代码, 净值日期, 单位净值, 累计单位净值
        FROM fund_nav_data
        WHERE typeof(单位净值) != 'real' AND typeof(单位净值) != 'integer'
    ''')

    rows = cursor.fetchall()
    if rows:
        problem_count += len(rows)
        print(f"\n找到 {len(rows)} 条单位净值不是数字的记录:")
        for row in rows:
            print(f"  ID: {row[0]}")
            print(f"    产品名称: {row[1]}")
            print(f"    产品代码: {row[2]}")
            print(f"    净值日期: {row[3]}")
            print(f"    单位净值: {row[4]} (类型: {type(row[4])})")
            print(f"    累计单位净值: {row[5]}")
            print()

    print("="*80)
    if problem_count > 0:
        print(f"共发现 {problem_count} 条问题数据")

        # 询问是否删除
        delete = input("\n是否删除这些问题数据？(输入 yes 确认): ").strip().lower()
        if delete == 'yes':
            for keyword in header_keywords:
                cursor.execute('''
                    DELETE FROM fund_nav_data
                    WHERE 产品名称 = ? OR 产品代码 = ?
                ''', (keyword, keyword))

            cursor.execute('''
                DELETE FROM fund_nav_data
                WHERE typeof(单位净值) != 'real' AND typeof(单位净值) != 'integer'
            ''')

            conn.commit()
            print("✓ 问题数据已删除")
    else:
        print("✓ 未发现问题数据")

    conn.close()

if __name__ == "__main__":
    check_database()
