#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
从 zxdemo/ 目录下的两个 Excel 文件导入基金数据：
  - 臻选货架.xlsx：基金主数据（策略标签、成立日期、对外展示等）
  - ZXdatabase.xlsx：净值数据（280个Sheet，每Sheet一只基金，周频）

运行方式：
  python get_excel_data.py
  或通过 API POST /api/excel/import 触发
"""

import logging
import os
import sqlite3
from datetime import datetime
from typing import Optional

import pandas as pd

logger = logging.getLogger("excel_import")

EXCEL_DIR = os.path.join(os.path.dirname(__file__), "zxdemo")
SHELF_FILE = os.path.join(EXCEL_DIR, "臻选货架.xlsx")
NAV_FILE = os.path.join(EXCEL_DIR, "ZXdatabase.xlsx")


# ---------------------------------------------------------------------------
# 工具函数
# ---------------------------------------------------------------------------

def _normalize_date(val) -> Optional[str]:
    """将各种日期格式统一转为 YYYYMMDD 字符串，失败返回 None。"""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    if isinstance(val, (datetime,)):
        return val.strftime("%Y%m%d")
    s = str(val).strip()
    # 已是 YYYYMMDD
    if len(s) == 8 and s.isdigit():
        return s
    # YYYY-MM-DD
    if len(s) == 10 and s[4] == "-":
        return s.replace("-", "")
    # pandas Timestamp 转字符串后可能是 YYYY-MM-DD HH:MM:SS
    if len(s) >= 10 and s[4] == "-":
        return s[:10].replace("-", "")
    # 尝试 pandas 解析
    try:
        return pd.to_datetime(s).strftime("%Y%m%d")
    except Exception:
        return None


def _get_or_create_fund_id(conn: sqlite3.Connection, code: str, name: Optional[str]) -> int:
    row = conn.execute("SELECT fund_id FROM funds WHERE 产品代码 = ?", (code,)).fetchone()
    if row:
        return row[0]
    conn.execute(
        "INSERT INTO funds (产品代码, 产品名称) VALUES (?, ?)",
        (code, name),
    )
    return conn.execute("SELECT fund_id FROM funds WHERE 产品代码 = ?", (code,)).fetchone()[0]


# ---------------------------------------------------------------------------
# Step 1：读臻选货架 → upsert funds 表
# ---------------------------------------------------------------------------

def _import_shelf(conn: sqlite3.Connection, df: pd.DataFrame) -> int:
    """将臻选货架数据 upsert 到 funds 表，返回处理行数。"""
    count = 0
    for _, row in df.iterrows():
        code = str(row.get("Code_Id", "")).strip()
        if not code:
            continue
        name = str(row.get("Code_Name", "")).strip() or None
        strategy1 = str(row.get("策略标签-一级", "")).strip() or None
        strategy2 = str(row.get("策略标签-二级", "")).strip() or None
        strategy3_raw = row.get("策略标签-三级", "")
        strategy3 = str(strategy3_raw).strip() if strategy3_raw and not (isinstance(strategy3_raw, float) and pd.isna(strategy3_raw)) else None
        is_show = str(row.get("对外展示", "")).strip() or None
        setup_date = _normalize_date(row.get("成立日期"))
        start_date = _normalize_date(row.get("Start_date"))
        benchmark = str(row.get("对标指数", "")).strip() or None

        # upsert：先确保 fund 存在，再更新字段
        fund_id = _get_or_create_fund_id(conn, code, name)
        conn.execute(
            """UPDATE funds SET
                产品名称 = COALESCE(?, 产品名称),
                strategy1 = ?,
                strategy2 = ?,
                strategy3 = ?,
                is_show = ?,
                setup_date = ?,
                start_date = ?,
                benchmark_index = COALESCE(?, benchmark_index)
               WHERE fund_id = ?""",
            (name, strategy1, strategy2, strategy3, is_show, setup_date, start_date, benchmark, fund_id),
        )
        count += 1
    return count


# ---------------------------------------------------------------------------
# Step 2：读 ZXdatabase → 白名单过滤 → upsert fund_nav_data
# ---------------------------------------------------------------------------

def _import_nav(conn: sqlite3.Connection, xls: pd.ExcelFile, whitelist: set) -> tuple:
    """
    遍历 ZXdatabase 的每个 Sheet，只导入白名单内的基金。
    返回 (nav_upserted, conflicts_detected)。
    """
    nav_count = 0
    conflict_count = 0

    for sheet_name in xls.sheet_names:
        code = str(sheet_name).strip()
        if code not in whitelist:
            continue

        try:
            df = xls.parse(sheet_name)
        except Exception as e:
            logger.warning("解析 Sheet %s 失败: %s", sheet_name, e)
            continue

        # 标准化列名（兼容大小写和空格）
        df.columns = [str(c).strip().lower() for c in df.columns]
        col_map = {}
        for c in df.columns:
            if "date" in c:
                col_map["date"] = c
            elif "unit" in c or "单位" in c:
                col_map["unit_value"] = c
            elif "accum" in c or "累计" in c:
                col_map["accumulated_value"] = c
        if "date" not in col_map or "unit_value" not in col_map:
            logger.warning("Sheet %s 缺少必要列，跳过", sheet_name)
            continue

        fund_id = conn.execute(
            "SELECT fund_id FROM funds WHERE 产品代码 = ?", (code,)
        ).fetchone()
        if not fund_id:
            continue
        fund_id = fund_id[0]

        for _, row in df.iterrows():
            nav_date = _normalize_date(row[col_map["date"]])
            if not nav_date:
                continue
            try:
                unit_nav = float(row[col_map["unit_value"]])
            except (TypeError, ValueError):
                continue
            accum_nav_raw = row.get(col_map.get("accumulated_value", ""), None)
            try:
                accum_nav = float(accum_nav_raw) if accum_nav_raw is not None and not (isinstance(accum_nav_raw, float) and pd.isna(accum_nav_raw)) else None
            except (TypeError, ValueError):
                accum_nav = None

            # 冲突检测：同基金同日期已有邮件数据
            existing = conn.execute(
                """SELECT id, 单位净值, source_id FROM fund_nav_data
                   WHERE 产品代码 = ? AND 净值日期 = ?""",
                (code, nav_date),
            ).fetchone()

            if existing and existing["source_id"] is not None:
                # 来自邮件的数据，检测冲突
                email_nav = existing["单位净值"]
                if email_nav is not None and abs(float(email_nav) - unit_nav) > 1e-6:
                    conn.execute(
                        """INSERT OR REPLACE INTO excel_conflicts
                           (产品代码, 净值日期, email_unit_nav, excel_unit_nav)
                           VALUES (?, ?, ?, ?)""",
                        (code, nav_date, float(email_nav), unit_nav),
                    )
                    conflict_count += 1
                # 以 Excel 为准，覆盖
                conn.execute(
                    """UPDATE fund_nav_data SET 单位净值 = ?, 累计单位净值 = ?
                       WHERE 产品代码 = ? AND 净值日期 = ?""",
                    (unit_nav, accum_nav, code, nav_date),
                )
            else:
                # 新记录或手动录入记录：直接 upsert（source_id=0 表示来自 Excel）
                conn.execute(
                    """INSERT INTO fund_nav_data
                       (fund_id, 产品代码, 净值日期, 单位净值, 累计单位净值, source_id)
                       VALUES (?, ?, ?, ?, ?, 0)
                       ON CONFLICT(产品代码, 净值日期) DO UPDATE SET
                           单位净值 = excluded.单位净值,
                           累计单位净值 = excluded.累计单位净值""",
                    (fund_id, code, nav_date, unit_nav, accum_nav),
                )
            nav_count += 1

    return nav_count, conflict_count


# ---------------------------------------------------------------------------
# Step 3：重算复权净值
# ---------------------------------------------------------------------------

def _recompute_adjusted_nav(conn: sqlite3.Connection, whitelist: set):
    """对白名单内所有基金重算复权净值。"""
    try:
        from get_163_email import compute_adjusted_nav
    except ImportError:
        logger.warning("无法导入 compute_adjusted_nav，跳过复权净值重算")
        return
    for code in whitelist:
        row = conn.execute("SELECT fund_id FROM funds WHERE 产品代码 = ?", (code,)).fetchone()
        if row:
            try:
                compute_adjusted_nav(conn, code)
            except Exception as e:
                logger.warning("基金 %s 复权净值重算失败: %s", code, e)


# ---------------------------------------------------------------------------
# 主入口
# ---------------------------------------------------------------------------

def import_excel_data(db_path: str) -> dict:
    """
    完整导入流程。返回统计信息字典。
    """
    if not os.path.exists(SHELF_FILE):
        raise FileNotFoundError(f"臻选货架文件不存在: {SHELF_FILE}")
    if not os.path.exists(NAV_FILE):
        raise FileNotFoundError(f"ZXdatabase 文件不存在: {NAV_FILE}")

    logger.info("开始读取臻选货架: %s", SHELF_FILE)
    shelf_df = pd.read_excel(SHELF_FILE, engine="openpyxl", dtype=str)
    # 去掉全空行
    shelf_df = shelf_df.dropna(how="all")

    conn = sqlite3.connect(db_path, check_same_thread=False, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA foreign_keys=ON")

    errors = []
    funds_upserted = 0
    nav_upserted = 0
    conflicts_detected = 0

    try:
        # 确保 email_sources 中有 id=0 的占位记录（Excel 导入来源标识）
        conn.execute(
            "INSERT OR IGNORE INTO email_sources (id) VALUES (0)"
        )
        conn.commit()

        # Step 1: 导入基金主数据
        logger.info("Step 1: upsert funds 表 (%d 行)", len(shelf_df))
        funds_upserted = _import_shelf(conn, shelf_df)
        conn.commit()

        # 构建白名单（臻选货架中的所有 Code_Id）
        whitelist = set(
            str(r).strip()
            for r in shelf_df["Code_Id"].dropna()
            if str(r).strip()
        )
        logger.info("白名单基金数: %d", len(whitelist))

        # Step 2: 导入净值数据
        logger.info("Step 2: 读取 ZXdatabase: %s", NAV_FILE)
        xls = pd.ExcelFile(NAV_FILE, engine="openpyxl")
        nav_upserted, conflicts_detected = _import_nav(conn, xls, whitelist)
        conn.commit()
        logger.info("净值记录 upserted: %d，冲突: %d", nav_upserted, conflicts_detected)

        # Step 3: 重算复权净值
        logger.info("Step 3: 重算复权净值")
        _recompute_adjusted_nav(conn, whitelist)
        conn.commit()

    except Exception as e:
        conn.rollback()
        logger.error("导入失败: %s", e)
        errors.append(str(e))
    finally:
        conn.close()

    result = {
        "funds_upserted": funds_upserted,
        "nav_upserted": nav_upserted,
        "conflicts_detected": conflicts_detected,
        "errors": errors,
    }
    logger.info("导入完成: %s", result)
    return result


if __name__ == "__main__":
    import sys
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        stream=sys.stdout,
    )
    db_path = os.getenv("DB_PATH", "fund_data.db")
    result = import_excel_data(db_path)
    print(result)
