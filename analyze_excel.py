#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Excel格式分析工具
从163邮箱读取不同基金的Excel附件，分析格式差异
目标：识别基金名称、基金代码、单位净值、累计单位净值、净值日期的不同位置模式
"""

import imaplib
import email
from email.header import decode_header
import pandas as pd
from io import BytesIO
import re


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


def analyze_excel_format(df, filename):
    """
    分析Excel格式，查找核心字段的位置模式
    核心字段：基金名称、基金代码、单位净值、累计单位净值、净值日期
    """
    analysis = {
        'filename': filename,
        'shape': df.shape,
        'fields_found': {},
        'format_pattern': None
    }

    # 定义要查找的关键字
    keywords = {
        '基金名称': ['基金名称', '产品名称', '名称'],
        '基金代码': ['基金代码', '产品代码', '代码'],
        '单位净值': ['单位净值', '净值'],
        '累计单位净值': ['累计单位净值', '累计净值'],
        '净值日期': ['净值日期', '日期']
    }

    # 将DataFrame转换为字符串以便搜索
    df_str = df.astype(str)

    for field_name, patterns in keywords.items():
        for i in range(len(df)):
            for j in range(len(df.columns)):
                cell_value = str(df.iloc[i, j]).strip()

                for pattern in patterns:
                    # 模式1: "标签：值" 格式（在同一单元格）
                    if pattern in cell_value and '：' in cell_value:
                        value = cell_value.split('：', 1)[1].strip()
                        analysis['fields_found'][field_name] = {
                            'position': (i, j),
                            'pattern': '标签：值（同一单元格）',
                            'label': pattern,
                            'value': value
                        }
                        break

                    # 模式2: 标签在左，值在右侧单元格
                    elif cell_value == pattern or cell_value.replace(' ', '') == pattern:
                        if j + 1 < len(df.columns):
                            value = str(df.iloc[i, j + 1]).strip()
                            if value and value != 'nan':
                                analysis['fields_found'][field_name] = {
                                    'position': (i, j),
                                    'pattern': '标签在左，值在右侧',
                                    'label': pattern,
                                    'value': value,
                                    'value_position': (i, j + 1)
                                }
                                break

                    # 模式3: 标签在上，值在下方单元格
                    elif cell_value == pattern or cell_value.replace(' ', '') == pattern:
                        if i + 1 < len(df):
                            value = str(df.iloc[i + 1, j]).strip()
                            if value and value != 'nan':
                                analysis['fields_found'][field_name] = {
                                    'position': (i, j),
                                    'pattern': '标签在上，值在下方',
                                    'label': pattern,
                                    'value': value,
                                    'value_position': (i + 1, j)
                                }
                                break

                if field_name in analysis['fields_found']:
                    break

            if field_name in analysis['fields_found']:
                break

    # 判断整体格式模式
    patterns_found = [info['pattern'] for info in analysis['fields_found'].values()]
    if patterns_found:
        most_common = max(set(patterns_found), key=patterns_found.count)
        analysis['format_pattern'] = most_common

    return analysis


def extract_excel_from_email(imap_client, email_id):
    """从邮件中提取Excel附件"""
    excel_data = []

    try:
        status, email_data = imap_client.fetch(email_id, "(RFC822)")
        raw_email = email_data[0][1]
        msg = email.message_from_bytes(raw_email)

        # 获取邮件信息
        subject = decode_str(msg.get("Subject")) if msg.get("Subject") else "(无主题)"
        sender = decode_str(msg.get("From")) if msg.get("From") else "(未知)"

        if msg.is_multipart():
            for part in msg.walk():
                content_disposition = str(part.get("Content-Disposition"))

                if "attachment" in content_disposition:
                    filename = get_attachment_filename(part)

                    if filename and filename.lower().endswith(('.xls', '.xlsx', '.xlsm')):
                        attachment_data = part.get_payload(decode=True)

                        try:
                            excel_buffer = BytesIO(attachment_data)
                            df = pd.read_excel(excel_buffer, header=None)

                            excel_data.append({
                                'filename': filename,
                                'dataframe': df,
                                'subject': subject,
                                'sender': sender
                            })
                        except Exception as e:
                            print(f"    读取Excel失败: {e}")

    except Exception as e:
        print(f"  提取邮件附件失败: {e}")

    return excel_data


def analyze_emails(email_user, email_pwd):
    """连接邮箱并分析所有Excel附件"""

    print("正在连接到163邮箱...")
    imap_host = "imap.163.com"

    try:
        imap_client = imaplib.IMAP4_SSL(imap_host, 993)
        print("连接成功")
    except Exception as e:
        print(f"连接失败: {e}")
        return

    print(f"正在登录: {email_user}")
    try:
        imap_client.login(email_user, email_pwd)
        print("登录成功")
    except Exception as e:
        print(f"登录失败: {e}")
        return

    # 发送ID命令
    try:
        if 'ID' not in imaplib.Commands:
            imaplib.Commands['ID'] = ('AUTH',)
        args = ("name", "analyzer", "contact", email_user, "version", "1.0.0", "vendor", "analyzer")
        imap_client._simple_command('ID', '("' + '" "'.join(args) + '")')
    except:
        pass

    print("正在打开收件箱...")
    status, messages = imap_client.select("INBOX")

    if status != 'OK':
        print(f"打开收件箱失败: {status}")
        imap_client.logout()
        return

    total_messages = int(messages[0])
    print(f"收件箱中共有 {total_messages} 封邮件\n")

    # 搜索所有邮件
    status, email_ids = imap_client.search(None, "ALL")
    email_id_list = email_ids[0].split()

    # 分析结果
    all_analyses = []

    output_file = 'excel_format_analysis.txt'
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write('='*80 + '\n')
        f.write('Excel格式分析报告\n')
        f.write('='*80 + '\n\n')

        excel_count = 0

        for idx, email_id in enumerate(email_id_list, 1):
            print(f"处理邮件 {idx}/{len(email_id_list)}...")

            excel_list = extract_excel_from_email(imap_client, email_id)

            for excel_info in excel_list:
                excel_count += 1
                df = excel_info['dataframe']
                filename = excel_info['filename']

                print(f"  分析Excel: {filename}")

                # 分析格式
                analysis = analyze_excel_format(df, filename)
                all_analyses.append(analysis)

                # 写入分析结果
                f.write(f'\n{"="*80}\n')
                f.write(f'文件 {excel_count}: {filename}\n')
                f.write(f'邮件主题: {excel_info["subject"]}\n')
                f.write(f'发件人: {excel_info["sender"]}\n')
                f.write(f'{"="*80}\n\n')

                f.write(f'数据维度: {analysis["shape"][0]} 行 × {analysis["shape"][1]} 列\n\n')

                # 写入原始数据
                f.write('原始数据内容:\n')
                f.write('-'*80 + '\n')
                f.write(df.to_string() + '\n\n')

                # 写入字段识别结果
                f.write('字段识别结果:\n')
                f.write('-'*80 + '\n')

                if analysis['fields_found']:
                    for field_name, info in analysis['fields_found'].items():
                        f.write(f'\n{field_name}:\n')
                        f.write(f'  位置: 第{info["position"][0]+1}行, 第{info["position"][1]+1}列\n')
                        f.write(f'  模式: {info["pattern"]}\n')
                        f.write(f'  标签: {info["label"]}\n')
                        f.write(f'  值: {info["value"]}\n')
                        if 'value_position' in info:
                            f.write(f'  值的位置: 第{info["value_position"][0]+1}行, 第{info["value_position"][1]+1}列\n')

                    f.write(f'\n整体格式模式: {analysis["format_pattern"]}\n')
                else:
                    f.write('未能识别出核心字段\n')

                f.write('\n')

        # 写入总结
        f.write('\n' + '='*80 + '\n')
        f.write('分析总结\n')
        f.write('='*80 + '\n\n')
        f.write(f'总共分析了 {excel_count} 个Excel文件\n\n')

        # 统计格式模式
        patterns = {}
        for analysis in all_analyses:
            if analysis['format_pattern']:
                pattern = analysis['format_pattern']
                if pattern not in patterns:
                    patterns[pattern] = []
                patterns[pattern].append(analysis['filename'])

        f.write('格式模式统计:\n')
        for pattern, files in patterns.items():
            f.write(f'\n{pattern}: {len(files)} 个文件\n')
            for filename in files:
                f.write(f'  - {filename}\n')

    print(f'\n分析完成！结果已保存到: {output_file}')

    imap_client.close()
    imap_client.logout()

    return all_analyses


def main():
    print("="*60)
    print("Excel格式分析工具")
    print("="*60)
    print()

    email_user = input("请输入163邮箱地址: ").strip()
    email_pwd = input("请输入IMAP授权码: ").strip()

    print()

    if not email_user or not email_pwd:
        print("错误: 邮箱地址和授权码不能为空！")
        return

    analyze_emails(email_user, email_pwd)


if __name__ == "__main__":
    main()
