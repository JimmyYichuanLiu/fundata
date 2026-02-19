#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试东恺基金格式 - 诊断问题
"""

import pandas as pd
from smart_extractor import extract_and_normalize

# 模拟东恺基金的Excel格式（横向表格）
print("="*80)
print("测试东恺基金格式（真实数据结构）")
print("="*80)

# 创建真实的数据结构
data = [
    ['客户名称', '基金账号', '估值基准日', '计提方式', '持仓份额', '虚拟业绩报酬', '虚拟净值', '实际净值', '实际累计净值', '产品代码', '产品名称', '参考市值', '参考市值（虚拟）', '备注', '产品类别', '协会备案代码'],
    ['中邮永安鑫享成长FOF二号私募证券投资基金', 'S58005924402', '2024-11-27', 'TA计提', '860,511.14', '0', '1.1613', '1.1613', '1.1613', 'T05851', '东恺多资产ETF轮动一号私募证券投资基金B类', None, None, None, None, None]
]

df = pd.DataFrame(data)

print("\n原始数据结构:")
print(f"第0行（表头）: {df.iloc[0].tolist()[:11]}")
print(f"第1行（数据）: {df.iloc[1].tolist()[:11]}")

print("\n尝试提取...")
result = extract_and_normalize(df)

if result:
    print("\n[SUCCESS] 提取成功！")
    r = result[0]
    print(f"  产品名称: {r.get('产品名称')}")
    print(f"  产品代码: {r.get('产品代码')}")
    print(f"  净值日期: {r.get('净值日期')}")
    print(f"  单位净值: {r.get('单位净值')}")
    print(f"  累计单位净值: {r.get('累计单位净值')}")
else:
    print("\n[FAILED] 提取失败！")

# 再次测试，打印中间步骤
print("\n" + "="*80)
print("诊断：查看第0行和第1行的数据")
print("="*80)

print(f"\n第0行第9列: {df.iloc[0, 9]}")  # 产品代码（表头）
print(f"第0行第10列: {df.iloc[0, 10]}")  # 产品名称（表头）

print(f"\n第1行第9列: {df.iloc[1, 9]}")  # T05851（数据）
print(f"第1行第10列: {df.iloc[1, 10]}")  # 东恺...（数据）

# 测试表格提取函数
from smart_extractor import extract_table_format

keywords = {
    '产品名称': ['产品名称', '基金名称', '名称', 'FundName'],
    '产品代码': ['产品代码', '基金代码', '代码', '协会备案编码', '协会备案代码', 'FundFillingCode'],
    '单位净值': ['单位净值', '基金份额净值', '产品单位净值', '净值', '实际净值', 'NAV/Share', 'NAVShare'],
    '累计单位净值': ['累计单位净值', '基金份额累计净值', '产品累计单位净值', '累计净值', '实际累计净值', 'AccumulatedNAV/Share', 'AccumulatedNAVShare'],
    '净值日期': ['净值日期', '日期', '估值基准日', 'NAVAsOfDate']
}

print("\n" + "="*80)
print("直接测试 extract_table_format 函数")
print("="*80)

result = extract_table_format(df, keywords)
if result:
    print("\n提取结果:")
    for key, value in result.items():
        print(f"  {key}: {value}")
else:
    print("\n提取失败")
