"""
一键修复：重置期货同步 checkpoint，补填换月后丢失的数据
在项目根目录运行：python scripts/fix_futures_checkpoint.py
"""
import sqlite3
import os
import sys

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "fund_data.db")


def main():
    print(f"数据库路径: {DB_PATH}")
    if not os.path.exists(DB_PATH):
        print("错误：找不到 fund_data.db，请在项目根目录运行此脚本")
        return

    conn = sqlite3.connect(DB_PATH)

    # 查当前状态
    current = conn.execute(
        "SELECT value FROM sync_state WHERE key='market_futures_last_date'"
    ).fetchone()
    print(f"当前 market_futures_last_date: {current[0] if current else '未设置'}")

    # 查期货数据实际最新日期
    actual = conn.execute(
        "SELECT MAX(trade_date) FROM futures_daily"
    ).fetchone()
    actual_date = actual[0] if actual else None
    print(f"期货数据实际最新日期: {actual_date}")

    if not actual_date:
        print("期货表无数据，无需修复")
        conn.close()
        return

    if current and current[0] == actual_date:
        print("checkpoint 与实际数据一致，无需修复")
        conn.close()
        return

    # 重置 checkpoint 到实际最新日期
    conn.execute(
        "UPDATE sync_state SET value=? WHERE key='market_futures_last_date'",
        (actual_date,)
    )
    conn.commit()
    print(f"已将 checkpoint 重置为 {actual_date}")
    print("请运行 python get_market_data.py 补填后续数据")
    conn.close()


if __name__ == "__main__":
    main()
