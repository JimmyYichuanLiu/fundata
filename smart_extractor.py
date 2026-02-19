#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
智能Excel数据提取器
自动识别不同格式的Excel文件并提取核心字段
"""

import pandas as pd
import re
from datetime import datetime


def extract_fund_data_smart(df):
    """
    智能提取基金数据

    参数:
        df: pandas DataFrame，使用 header=None 读取的原始数据

    返回:
        list of dict: 包含提取的字段列表，如果提取失败则返回None
        每个dict包含:
        {
            '产品名称': str,
            '产品代码': str,
            '单位净值': float,
            '累计单位净值': float,
            '净值日期': str
        }
    """

    # 定义关键字映射（增加更多变体以支持不同格式）
    keywords = {
        '产品名称': ['产品名称', '基金名称', '资产名称', '名称', 'FundName'],
        '产品代码': ['产品代码', '基金代码', '资产代码', '代码', '协会备案编码', '协会备案代码', 'FundFillingCode'],
        '单位净值': ['单位净值', '基金份额净值', '产品单位净值', '当期净值', '资产净值', '净值', '实际净值', 'NAV/Share', 'NAVShare'],
        '累计单位净值': ['累计单位净值', '基金份额累计净值', '产品累计单位净值', '当期累计净值', '资产净值累计净值', '累计净值', '实际累计净值', 'AccumulatedNAV/Share', 'AccumulatedNAVShare'],
        '净值日期': ['净值日期', '日期', '估值基准日', 'NAVAsOfDate']
    }

    # 格式1: 尝试作为标准表格（某一行是表头，其后为数据行）
    results = extract_table_format(df, keywords)
    if results:
        valid_results = [r for r in results if is_valid_result(r)]
        if valid_results:
            return valid_results

    # 格式2&3: 键值对格式
    result = extract_keyvalue_format(df, keywords)
    if result and is_valid_result(result):
        return [result]

    return None


def extract_table_format(df, keywords):
    """提取表格格式的数据，返回所有数据行的列表"""
    try:
        # 检查前几行，找到真正的表头行
        # 有些Excel前几行是标题/副标题，之后才是表头
        header_row_idx = None

        for row_idx in range(min(5, len(df))):  # 检查前5行（支持最多4行标题行）
            row = df.iloc[row_idx].astype(str).values

            # 清理表头中的换行符和空格
            row_cleaned = [str(cell).replace('\n', '').replace(' ', '') for cell in row]

            # 检查这一行是否包含关键字（可能是表头）
            # 要求至少匹配2个不同字段的关键字，避免标题行（如"资产净值报告"）被误判为表头
            matched_field_count = 0
            for field_name, patterns in keywords.items():
                for pattern in patterns:
                    if any(pattern in cell or pattern.replace(' ', '') in cell for cell in row_cleaned):
                        matched_field_count += 1
                        break  # 该字段已匹配，检查下一个字段

            if matched_field_count >= 2:
                header_row_idx = row_idx
                break

        if header_row_idx is None:
            return None

        # 检查是否有数据行
        data_row_idx = header_row_idx + 1
        if data_row_idx >= len(df):
            return None

        # 使用找到的表头行作为列名重新读取（清理换行符）
        cleaned_headers = [str(h).replace('\n', ' ').strip() for h in df.iloc[header_row_idx]]

        # 从表头行之后的所有行作为数据
        df_with_header = pd.DataFrame(df.values[data_row_idx:], columns=cleaned_headers)

        if len(df_with_header) == 0:
            return None

        # 遍历所有数据行（支持多行数据表）
        results = []
        for row_num in range(len(df_with_header)):
            data_row = df_with_header.iloc[row_num]

            # 跳过全为空的行
            if all(pd.isna(v) or str(v).strip() in ('', 'nan', 'NaN') for v in data_row):
                continue

            result = {}

            # 查找每个字段
            for field_name, patterns in keywords.items():
                found = False
                for pattern in patterns:
                    for col in df_with_header.columns:
                        col_cleaned = str(col).replace('\n', '').replace(' ', '')
                        if pattern in col_cleaned or pattern.replace(' ', '') in col_cleaned:
                            value = data_row[col]
                            if pd.notna(value) and str(value).strip() and str(value) != 'nan':
                                result[field_name] = clean_value(str(value))
                                found = True
                                break
                    if found:
                        break

            if len(result) >= 3:
                results.append(result)

        return results if results else None

    except Exception as e:
        return None


def extract_keyvalue_format(df, keywords):
    """提取键值对格式的数据（标签在左侧，值在右侧列）"""
    try:
        result = {}

        # 创建所有关键字的集合，用于验证提取的值不是表头
        all_keywords = set()
        for patterns in keywords.values():
            all_keywords.update(patterns)

        # 遍历所有单元格查找关键字
        for field_name, patterns in keywords.items():
            for i in range(len(df)):
                for j in range(len(df.columns)):
                    cell_value = str(df.iloc[i, j]).strip()

                    for pattern in patterns:
                        is_match = False

                        # 1. 精确匹配（去除空格和换行符后）
                        cell_value_cleaned = cell_value.replace(' ', '').replace('\n', '')
                        pattern_cleaned = pattern.replace(' ', '').replace('\n', '')

                        if cell_value_cleaned == pattern_cleaned:
                            is_match = True
                        # 2. 包含匹配（但要确保不是子串，例如"名称"不应匹配"客户名称"）
                        elif pattern in cell_value:
                            idx = cell_value.find(pattern)
                            before = cell_value[idx-1] if idx > 0 else ''

                            # 如果前面是中文字符（如"客户"、"基金"），则不匹配
                            if not (before.isalpha() or '\u4e00' <= before <= '\u9fff'):
                                is_match = True

                        if is_match:
                            # 若另一字段有更长（更精确）的模式也匹配此单元格，则跳过
                            # 例如：'净值日期：'中'净值'匹配单位净值，但'净值日期'更精确地匹配净值日期字段
                            better_field_match = False
                            for other_field, other_patterns in keywords.items():
                                if other_field == field_name:
                                    continue
                                for other_pattern in other_patterns:
                                    other_pattern_cleaned = other_pattern.replace(' ', '').replace('\n', '')
                                    if (other_pattern_cleaned in cell_value_cleaned and
                                            len(other_pattern_cleaned) > len(pattern_cleaned)):
                                        better_field_match = True
                                        break
                                if better_field_match:
                                    break
                            if better_field_match:
                                continue  # 跳过此模式，当前单元格更可能属于另一字段

                            # 模式1: "标签：值" 在同一单元格
                            if '：' in cell_value:
                                parts = cell_value.split('：', 1)
                                if len(parts) == 2:
                                    value = parts[1].strip()
                                    if value and value != 'nan' and not is_header_keyword(value, all_keywords):
                                        result[field_name] = clean_value(value)
                                        break

                            # 模式2: 标签在当前列，值在右侧列
                            for offset in [1, 2]:
                                if j + offset < len(df.columns):
                                    value = str(df.iloc[i, j + offset]).strip()
                                    if value and value != 'nan' and value != 'NaN' and not is_header_keyword(value, all_keywords):
                                        result[field_name] = clean_value(value)
                                        break

                            break

                    if field_name in result:
                        break

                if field_name in result:
                    break

        return result if len(result) >= 3 else None

    except Exception as e:
        return None


def is_header_keyword(value, keywords):
    """检查值是否是表头关键字"""
    value_lower = value.lower().strip()

    # 常见的表头关键字列表
    header_patterns = [
        '产品名称', '基金名称', '名称',
        '产品代码', '基金代码', '代码',
        '单位净值', '基金份额净值', '产品单位净值', '净值',
        '累计单位净值', '基金份额累计净值', '产品累计单位净值', '累计净值',
        '净值日期', '日期',
        '客户名称', '份额', '参与计提份额',
        '计提频率', '业绩报酬', '提取净值', '虚拟净值'
    ]

    for keyword in header_patterns:
        if keyword in value or value in keyword:
            return True

    for keyword in keywords:
        if keyword in value or value in keyword:
            return True

    return False


def clean_value(value):
    """清理提取的值"""
    value = str(value).strip()

    # 移除下划线后的内容（如 "SLA149_总层面" -> "SLA149"）
    if '_' in value:
        value = value.split('_')[0]

    return value


def is_valid_result(result):
    """验证提取结果是否有效"""
    if not result:
        return False

    # 至少需要有产品代码和单位净值
    required_fields = ['产品代码', '单位净值']
    for field in required_fields:
        if field not in result or not result[field]:
            return False

    return True


def normalize_date(date_str):
    """标准化日期格式为 YYYYMMDD"""
    if not date_str:
        return None

    date_str = str(date_str).strip()

    # 尝试不同的日期格式
    formats = [
        '%Y%m%d',               # 20240130
        '%Y-%m-%d',             # 2024-01-30
        '%Y/%m/%d',             # 2024/01/30
        '%Y-%m-%d %H:%M:%S',   # 2024-01-30 00:00:00 (pandas Timestamp字符串)
    ]

    for fmt in formats:
        try:
            dt = datetime.strptime(date_str, fmt)
            return dt.strftime('%Y%m%d')
        except:
            continue

    # 如果都失败，返回原始值
    return date_str


def convert_to_float(value):
    """尝试将值转换为浮点数"""
    try:
        return float(value)
    except:
        return value


def extract_and_normalize(df):
    """
    提取并标准化数据

    返回:
        list of dict: 标准化后的数据列表，如果提取失败则返回None
        每个dict包含以下字段:
        {
            '产品名称': str,
            '产品代码': str,
            '客户名称': str or None,
            '参与计提份额': float or None,
            '净值日期': str (YYYYMMDD格式),
            '单位净值': float,
            '累计单位净值': float or None,
            '计提频率': str or None,
            '虚拟计提业绩报酬金额': float or None,
            '提取净值': float or None,
            '计提后虚拟净值': float or None
        }
    """
    # 智能提取（返回列表）
    data_list = extract_fund_data_smart(df)

    if not data_list:
        return None

    normalized_list = []
    for data in data_list:
        normalized = {
            '产品名称': data.get('产品名称'),
            '产品代码': data.get('产品代码'),
            '客户名称': data.get('客户名称'),
            '参与计提份额': convert_to_float(data.get('参与计提份额')) if data.get('参与计提份额') else None,
            '净值日期': normalize_date(data.get('净值日期')),
            '单位净值': convert_to_float(data.get('单位净值')),
            '累计单位净值': convert_to_float(data.get('累计单位净值')) if data.get('累计单位净值') else None,
            '计提频率': data.get('计提频率'),
            '虚拟计提业绩报酬金额': convert_to_float(data.get('虚拟计提业绩报酬金额')) if data.get('虚拟计提业绩报酬金额') else None,
            '提取净值': convert_to_float(data.get('提取净值')) if data.get('提取净值') else None,
            '计提后虚拟净值': convert_to_float(data.get('计提后虚拟净值')) if data.get('计提后虚拟净值') else None
        }
        normalized_list.append(normalized)

    return normalized_list


# 测试函数
if __name__ == "__main__":
    # 测试示例
    print("智能Excel数据提取器")
    print("=" * 60)
    print()
    print("使用方法:")
    print("  from smart_extractor import extract_and_normalize")
    print("  import pandas as pd")
    print()
    print("  # 读取Excel（不指定header）")
    print("  df = pd.read_excel('file.xls', header=None)")
    print()
    print("  # 智能提取数据（返回列表）")
    print("  data_list = extract_and_normalize(df)")
    print()
    print("  if data_list:")
    print("      print(f'提取成功！共 {len(data_list)} 条记录')")
    print("      for data in data_list:")
    print("          print(data)")
    print("  else:")
    print("      print('提取失败')")
