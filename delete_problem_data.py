#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
删除数据库中的问题数据
"""

import sqlite3

def delete_problem_data():
    """删除数据库中的表头数据和无效数据"""

    conn = sqlite3.connect('fund_data.db')
    cursor = conn.cursor()

    # 表头关键字列表
    header_keywords = [
        '产品名称', '基金名称', '名称',
        '产品代码', '基金代码', '代码',
        '单位净值', '累计单位净值', '净值',
        '净值日期', '日期',
        '客户名称', '参与计提份额'
    ]

    deleted_count = 0

    # 删除包含表头关键字的记录
    for keyword in header_keywords:
        cursor.execute('''
            DELETE FROM fund_nav_data
            WHERE 产品名称 = ? OR 产品代码 = ?
        ''', (keyword, keyword))
        deleted_count += cursor.rowcount

    # 删除单位净值不是数字的记录
    cursor.execute('''
        DELETE FROM fund_nav_data
        WHERE typeof(单位净值) != 'real' AND typeof(单位净值) != 'integer'
    ''')
    deleted_count += cursor.rowcount

    conn.commit()
    conn.close()

    print(f"✓ 已删除 {deleted_count} 条问题数据")

if __name__ == "__main__":
    delete_problem_data()
