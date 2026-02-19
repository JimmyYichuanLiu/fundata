#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试修复后的智能提取器
测试东恺和利幄基金格式
"""

import pandas as pd
from smart_extractor import extract_and_normalize

# 测试1: 东恺基金格式（表格格式，表头在第0行，数据在第1行）
print("="*80)
print("测试1: 东恺基金格式")
print("="*80)

dongkai_data = {
    0: ['客户名称', '中邮永安鑫享成长FOF二号私募证券投资基金'],
    1: ['基金账号', 'S58005924402'],
    2: ['估值基准日', '2024-11-27'],
    3: ['计提方式', 'TA计提'],
    4: ['持仓份额', '860,511.14'],
    5: ['虚拟业绩报酬', '0'],
    6: ['虚拟净值', '1.1613'],
    7: ['实际净值', '1.1613'],
    8: ['实际累计净值', '1.1613'],
    9: ['产品代码', 'T05851'],
    10: ['产品名称', '东恺多资产ETF轮动一号私募证券投资基金B类'],
}

# 转置为表格格式（第0行是表头）
df_dongkai = pd.DataFrame(dongkai_data).T

print("\n原始数据:")
print(df_dongkai)

result = extract_and_normalize(df_dongkai)

if result:
    print("\n[SUCCESS] 提取成功！")
    print(f"  产品名称: {result.get('产品名称')}")
    print(f"  产品代码: {result.get('产品代码')}")
    print(f"  净值日期: {result.get('净值日期')}")
    print(f"  单位净值: {result.get('单位净值')}")
    print(f"  累计单位净值: {result.get('累计单位净值')}")
else:
    print("\n[FAILED] 提取失败")

# 测试2: 利幄基金格式（表头包含换行符）
print("\n" + "="*80)
print("测试2: 利幄基金格式（表头包含换行符）")
print("="*80)

liwu_data = [
    ['日期\n（NAV As Of Date）', '产品名称\n（Fund Name）', '单位净值\n（NAV/Share）', '累计单位净值\n（Accumulated NAV/Share）', '协会备案编码\n（Fund Filling Code）'],
    ['2025-09-26', '利幄全天候1号私募证券投资基金', '1.5809', '1.5809', 'SB6346']
]

df_liwu = pd.DataFrame(liwu_data)

print("\n原始数据:")
print(df_liwu)

# 注意：需要用header=None读取
result = extract_and_normalize(df_liwu)

if result:
    print("\n[SUCCESS] 提取成功！")
    print(f"  产品名称: {result.get('产品名称')}")
    print(f"  产品代码: {result.get('产品代码')}")
    print(f"  净值日期: {result.get('净值日期')}")
    print(f"  单位净值: {result.get('单位净值')}")
    print(f"  累计单位净值: {result.get('累计单位净值')}")
else:
    print("\n[FAILED] 提取失败")

print("\n" + "="*80)
print("测试完成")
print("="*80)
