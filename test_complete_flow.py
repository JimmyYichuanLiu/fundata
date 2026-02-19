#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
完整测试：模拟真实邮件处理流程
"""

import pandas as pd
from io import BytesIO
from smart_extractor import extract_and_normalize

print("="*80)
print("完整流程测试：模拟从邮件读取Excel并提取")
print("="*80)

# 创建一个真实的Excel数据
data = {
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
    11: ['参考市值', None],
    12: ['参考市值（虚拟）', None],
    13: ['备注', None],
    14: ['产品类别', None],
    15: ['协会备案代码', None]
}

# 转置为横向表格（第0行是表头，第1行是数据）
df_transposed = pd.DataFrame(data).T

print("\n模拟方式1: 横向表格（标准格式）")
print(f"DataFrame形状: {df_transposed.shape}")
print("\n前5列:")
print(df_transposed.iloc[:, :5])

result1 = extract_and_normalize(df_transposed)

if result1:
    print("\n[SUCCESS] 提取成功！")
    print(f"  产品代码: {result1[0].get('产品代码')}")
    print(f"  产品名称: {result1[0].get('产品名称')}")
    print(f"  净值日期: {result1[0].get('净值日期')}")
    print(f"  单位净值: {result1[0].get('单位净值')}")
else:
    print("\n[FAILED] 提取失败")

# 测试另一种可能的格式：纵向表格
print("\n" + "="*80)
print("模拟方式2: 纵向表格（键值对格式）")
print("="*80)

data_vertical = [
    ['客户名称', '中邮永安鑫享成长FOF二号私募证券投资基金'],
    ['基金账号', 'S58005924402'],
    ['估值基准日', '2024-11-27'],
    ['计提方式', 'TA计提'],
    ['持仓份额', '860,511.14'],
    ['虚拟业绩报酬', '0'],
    ['虚拟净值', '1.1613'],
    ['实际净值', '1.1613'],
    ['实际累计净值', '1.1613'],
    ['产品代码', 'T05851'],
    ['产品名称', '东恺多资产ETF轮动一号私募证券投资基金B类'],
]

df_vertical = pd.DataFrame(data_vertical)

print(f"DataFrame形状: {df_vertical.shape}")
print("\n前5行:")
print(df_vertical.head())

result2 = extract_and_normalize(df_vertical)

if result2:
    print("\n[SUCCESS] 提取成功！")
    print(f"  产品代码: {result2[0].get('产品代码')}")
    print(f"  产品名称: {result2[0].get('产品名称')}")
    print(f"  净值日期: {result2[0].get('净值日期')}")
    print(f"  单位净值: {result2[0].get('单位净值')}")
else:
    print("\n[FAILED] 提取失败")

print("\n" + "="*80)
print("测试完成")
print("="*80)
