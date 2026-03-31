#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
本地 → 服务器数据同步脚本
在本地（有VPN）运行，将原油价格/汇率/新闻数据推送到服务器。

用法：
    python sync_to_server.py --server ubuntu@YOUR_SERVER_IP

可选参数：
    --skip-fetch   跳过本地抓取，直接推送现有数据
    --news-only    只同步新闻（不同步原油/汇率）
    --crude-only   只同步原油/汇率（不同步新闻）
"""

import argparse
import os
import sqlite3
import subprocess
import sys
import tempfile

LOCAL_DB   = os.getenv("DB_PATH", "fund_data.db")
REMOTE_DB  = "/opt/fundata/fundata/fund_data.db"

CRUDE_TABLES = ["crude_daily", "fx_daily", "crude_price_cross"]
NEWS_TABLES  = ["crude_news"]


def run(cmd: list, check=True):
    print(f"  $ {' '.join(cmd)}")
    result = subprocess.run(cmd, check=check)
    return result.returncode == 0


def fetch_local(news=True, crude=True):
    if crude:
        print("\n[1/2] 抓取原油价格和汇率...")
        run([sys.executable, "get_crude_data.py"])
    if news:
        print("\n[2/2] 抓取新闻...")
        run([sys.executable, "get_news_data.py"])


def export_tables(tables: list, out_path: str):
    """将指定表导出为 SQL 文件（DELETE + INSERT OR REPLACE）。"""
    conn = sqlite3.connect(LOCAL_DB)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("PRAGMA journal_mode=WAL;\n")
        f.write("BEGIN TRANSACTION;\n")
        for table in tables:
            # 获取列信息
            cols_info = conn.execute(f"PRAGMA table_info({table})").fetchall()
            if not cols_info:
                print(f"  警告: 表 {table} 不存在，跳过")
                continue
            rows = conn.execute(f"SELECT * FROM {table}").fetchall()
            print(f"  {table}: {len(rows)} 行")
            f.write(f"\n-- {table}\n")
            f.write(f"DELETE FROM {table};\n")
            for row in rows:
                vals = ", ".join(
                    "NULL" if v is None else
                    str(v) if isinstance(v, (int, float)) else
                    "'" + str(v).replace("'", "''") + "'"
                    for v in row
                )
                f.write(f"INSERT OR REPLACE INTO {table} VALUES ({vals});\n")
        f.write("\nCOMMIT;\n")
    conn.close()


def push_to_server(server: str, sql_path: str):
    remote_tmp = "/tmp/fundata_sync.sql"
    print(f"\n  传输到服务器 {server}...")
    run(["scp", sql_path, f"{server}:{remote_tmp}"])
    print("  在服务器上导入...")
    run(["ssh", server, f"sqlite3 {REMOTE_DB} < {remote_tmp} && rm {remote_tmp}"])
    print("  重启服务...")
    run(["ssh", server, "sudo systemctl restart fundata-api"], check=False)


def main():
    parser = argparse.ArgumentParser(description="本地 → 服务器数据同步")
    parser.add_argument("--server", required=True, help="服务器地址，如 ubuntu@1.2.3.4")
    parser.add_argument("--skip-fetch", action="store_true", help="跳过本地抓取")
    parser.add_argument("--news-only",  action="store_true", help="只同步新闻")
    parser.add_argument("--crude-only", action="store_true", help="只同步原油/汇率")
    args = parser.parse_args()

    do_news  = not args.crude_only
    do_crude = not args.news_only
    tables   = (CRUDE_TABLES if do_crude else []) + (NEWS_TABLES if do_news else [])

    # 1. 本地抓取
    if not args.skip_fetch:
        fetch_local(news=do_news, crude=do_crude)

    # 2. 导出 SQL
    print("\n导出数据表...")
    with tempfile.NamedTemporaryFile(suffix=".sql", delete=False, mode="w") as tmp:
        tmp_path = tmp.name
    export_tables(tables, tmp_path)

    # 3. 推送到服务器
    print("\n推送到服务器...")
    push_to_server(args.server, tmp_path)
    os.unlink(tmp_path)

    print("\n✓ 同步完成")


if __name__ == "__main__":
    main()
