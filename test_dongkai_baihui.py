#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试东恺百会全视角六号基金格式
"""

import pandas as pd
from smart_extractor import extract_and_normalize

print("="*80)
print("测试东恺百会全视角六号基金格式（多行表头）")
print("="*80)

# 模拟真实的Excel数据结构
data = [
    ['虚拟业绩报酬', None, None, None, None, None, None, None, None, None],  # 第0行：标题
    ['基金代码', '基金名称', '净值日期', '客户代码', '客户名称', '发生份额', '单位净值', '累计单位净值', '虚拟单位净值', '实际提成金额'],  # 第1行：表头
    ['SXD843', '东恺百会全视角六号私募证券投资基金', '20250121', 'S10850693120', '中邮永安鑫享成长FOF二号私募证券投资基金', '1927339.31', '1.0388', '1.0388', '1.0386', '339.21']  # 第2行：数据
]

df = pd.DataFrame(data)

print("\n原始数据结构:")
print(f"第0行（标题）: {df.iloc[0].tolist()[:5]}")
print(f"第1行（表头）: {df.iloc[1].tolist()[:8]}")
print(f"第2行（数据）: {df.iloc[2].tolist()[:8]}")

print("\n尝试提取...")
result = extract_and_normalize(df)

if result:
    print("\n[SUCCESS] 提取成功！")
    r = result[0]  # 取第一条记录
    print(f"  产品名称: {r.get('产品名称')}")
    print(f"  产品代码: {r.get('产品代码')}")
    print(f"  净值日期: {r.get('净值日期')}")
    print(f"  单位净值: {r.get('单位净值')}")
    print(f"  累计单位净值: {r.get('累计单位净值')}")

    # 验证结果是否正确
    print("\n验证:")
    is_correct = (
        r.get('产品代码') == 'SXD843' and
        r.get('产品名称') == '东恺百会全视角六号私募证券投资基金' and
        r.get('净值日期') == '20250121' and
        r.get('单位净值') == 1.0388
    )

    if is_correct:
        print("  [PASS] 所有字段都正确！")
    else:
        print("  [FAIL] 有字段不正确！")
        if r.get('产品代码') != 'SXD843':
            print(f"    产品代码错误: {r.get('产品代码')} (应该是 SXD843)")
        if r.get('产品名称') != '东恺百会全视角六号私募证券投资基金':
            print(f"    产品名称错误")
else:
    print("\n[FAILED] 提取失败！")

print("\n" + "="*80)

# 测试旧格式是否仍然正常工作
print("测试兼容性：标准格式（第0行就是表头）")
print("="*80)

data_standard = [
    ['基金代码', '基金名称', '净值日期', '单位净值', '累计单位净值'],  # 第0行：表头
    ['ABC123', '测试基金', '20250121', '1.5000', '1.5000']  # 第1行：数据
]

df_standard = pd.DataFrame(data_standard)

result_standard = extract_and_normalize(df_standard)

if result_standard:
    print("\n[SUCCESS] 标准格式提取成功！")
    print(f"  产品代码: {result_standard[0].get('产品代码')}")
    print(f"  产品名称: {result_standard[0].get('产品名称')}")
else:
    print("\n[FAILED] 标准格式提取失败！")

print("\n" + "="*80)
print("测试完成")
print("="*80)
