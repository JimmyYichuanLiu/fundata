#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
整理fund_data.db数据库中的数据
按产品代码分类，按净值日期由远及近排序
"""

import sqlite3
import pandas as pd
from datetime import datetime

def organize_fund_data():
    """整理基金数据并输出"""

    # 连接数据库
    conn = sqlite3.connect('fund_data.db')

    print("正在读取数据库...")

    # 查询所有数据，按产品代码和净值日期排序
    query = '''
        SELECT
            产品名称,
            产品代码,
            净值日期,
            单位净值,
            累计单位净值
        FROM fund_nav_data
        ORDER BY 产品代码, 净值日期 ASC
    '''

    df = pd.read_sql_query(query, conn)
    conn.close()

    if df.empty:
        print("数据库中没有数据")
        return

    print(f"共读取 {len(df)} 条记录")
    print("="*100)

    # 获取所有产品代码
    product_codes = df['产品代码'].unique()
    print(f"共有 {len(product_codes)} 个不同的产品\n")

    # 按产品代码分组 - 仅显示摘要
    for code in product_codes:
        product_data = df[df['产品代码'] == code]
        product_name = product_data.iloc[0]['产品名称']
        print(f"产品代码: {code}, 产品名称: {product_name}, 记录数: {len(product_data)}")

    # 生成汇总统计
    print("\n\n数据汇总统计")
    print("="*100)

    summary_data = []
    for code in product_codes:
        product_data = df[df['产品代码'] == code]
        product_name = product_data.iloc[0]['产品名称']

        summary_data.append({
            '产品代码': code,
            '产品名称': product_name,
            '记录数': len(product_data),
            '最早日期': product_data.iloc[0]['净值日期'],
            '最新日期': product_data.iloc[-1]['净值日期'],
            '最早单位净值': product_data.iloc[0]['单位净值'],
            '最新单位净值': product_data.iloc[-1]['单位净值']
        })

    summary_df = pd.DataFrame(summary_data)
    print(summary_df.to_string(index=False))

    # 保存到Excel文件
    output_file = 'fund_data_organized.xlsx'
    print(f"\n正在保存数据到 {output_file}...")

    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        # 保存汇总表
        summary_df.to_excel(writer, sheet_name='汇总', index=False)

        # 为每个产品创建一个工作表
        for code in product_codes:
            product_data = df[df['产品代码'] == code]
            # Excel工作表名称不能超过31个字符，且不能包含特殊字符
            sheet_name = str(code)[:31]
            product_data.to_excel(writer, sheet_name=sheet_name, index=False)

    print(f"[OK] 数据已保存到 {output_file}")
    print(f"  - 汇总工作表: 包含所有产品的统计信息")
    print(f"  - 各产品工作表: 每个产品代码一个独立的工作表")

if __name__ == "__main__":
    try:
        organize_fund_data()
    except Exception as e:
        print(f"错误: {e}")
        import traceback
        traceback.print_exc()
