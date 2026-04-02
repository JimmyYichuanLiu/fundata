"""
一键修复：补全 fund_data.db 中所有基金的复权累计净值（adjusted_nav）
在项目根目录运行：python scripts/fix_adjusted_nav.py
"""
import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "fund_data.db")


def compute_adjusted_nav(conn, product_code):
    rows = conn.execute(
        """
        SELECT id, 净值日期, 单位净值, 累计单位净值, adjusted_nav
        FROM fund_nav_data
        WHERE 产品代码 = ? AND 净值日期 IS NOT NULL AND 单位净值 IS NOT NULL
        ORDER BY 净值日期 ASC
        """,
        (product_code,)
    ).fetchall()

    if not rows:
        return 0

    first_null_idx = next((i for i, r in enumerate(rows) if r[4] is None), None)
    if first_null_idx is None:
        return 0

    updates = []

    if first_null_idx == 0:
        try:
            prev_unit = float(rows[0][2])
        except (TypeError, ValueError):
            return 0
        try:
            prev_acc = float(rows[0][3]) if rows[0][3] is not None else prev_unit
        except (TypeError, ValueError):
            prev_acc = prev_unit
        adjusted = 1.0
        updates.append((adjusted, rows[0][0]))
        start_idx = 1
    else:
        last = rows[first_null_idx - 1]
        adjusted = last[4]
        try:
            prev_unit = float(last[2])
        except (TypeError, ValueError):
            return 0
        try:
            prev_acc = float(last[3]) if last[3] is not None else prev_unit
        except (TypeError, ValueError):
            prev_acc = prev_unit
        start_idx = first_null_idx

    for i in range(start_idx, len(rows)):
        row = rows[i]
        try:
            unit = float(row[2])
        except (TypeError, ValueError):
            continue
        try:
            acc = float(row[3]) if row[3] is not None else unit
        except (TypeError, ValueError):
            acc = unit

        div_cum_curr = acc - unit
        div_cum_prev = prev_acc - prev_unit
        dividend = div_cum_curr - div_cum_prev

        if prev_unit > 0:
            rt = (dividend + unit) / prev_unit - 1
            adjusted = adjusted * (1 + rt)

        updates.append((adjusted, row[0]))
        prev_unit = unit
        prev_acc = acc

    if updates:
        conn.executemany(
            "UPDATE fund_nav_data SET adjusted_nav = ? WHERE id = ?",
            updates
        )
    return len(updates)


def main():
    print(f"数据库路径: {DB_PATH}")
    if not os.path.exists(DB_PATH):
        print("错误：找不到 fund_data.db，请在项目根目录运行此脚本")
        return

    conn = sqlite3.connect(DB_PATH)
    codes = [
        r[0] for r in conn.execute(
            "SELECT DISTINCT 产品代码 FROM fund_nav_data WHERE 产品代码 IS NOT NULL AND adjusted_nav IS NULL"
        ) if r[0]
    ]
    print(f"{len(codes)} 只基金需要计算复权净值")
    if not codes:
        print("无需处理，已全部计算完毕")
        conn.close()
        return

    total = 0
    for code in codes:
        n = compute_adjusted_nav(conn, code)
        total += n

    conn.commit()
    notnull = conn.execute("SELECT COUNT(*) FROM fund_nav_data WHERE adjusted_nav IS NOT NULL").fetchone()[0]
    all_rows = conn.execute("SELECT COUNT(*) FROM fund_nav_data").fetchone()[0]
    print(f"完成：共更新 {total} 条，{notnull}/{all_rows} 条有复权净值")
    conn.close()


if __name__ == "__main__":
    main()
