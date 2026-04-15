#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
zx_importer.py — 臻选基金 Excel 导入模块

从两个 Excel 文件读取数据并写入独立的 SQLite 数据库 (zx_fund.db)：
  - 臻选货架.xlsx   → 基金产品目录 (Code_Id 白名单 + 元数据)
  - ZXdatabase.xlsx → 每个 Sheet 一只基金的净值历史

主要函数：
  init_zx_database(db_path)
  read_shelf(shelf_path) -> dict[str, dict]
  compute_adj_nav(unit_navs, accum_navs) -> list[float]
  import_zx_excel(db_path, zxdatabase_path, shelf_path) -> dict
"""

import logging
import sqlite3
from datetime import date, datetime
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)


# =============================================================================
# Database initialisation
# =============================================================================

def init_zx_database(db_path: str) -> None:
    """
    Create zx_fund_product and zx_fund_nav tables in db_path (if not exist).
    Safe to call on an existing database — uses CREATE TABLE IF NOT EXISTS.
    """
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS zx_fund_product (
                fund_id        INTEGER PRIMARY KEY AUTOINCREMENT,
                fund_code      TEXT NOT NULL UNIQUE,
                fund_name      TEXT,
                strategy_l1    TEXT,
                strategy_l2    TEXT,
                strategy_l3    TEXT,
                manager        TEXT,
                custodian      TEXT,
                inception_date TEXT,
                start_date     TEXT,
                benchmark_index TEXT,
                display        TEXT,
                created_at     DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS zx_fund_nav (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                fund_id     INTEGER NOT NULL,
                fund_code   TEXT,
                fund_name   TEXT,
                nav_date    TEXT NOT NULL,
                unit_nav    REAL,
                accum_nav   REAL,
                adj_nav     REAL,
                source_id   INTEGER,
                "录入时间"  DATETIME,
                data_source TEXT,
                UNIQUE(fund_id, nav_date),
                FOREIGN KEY (fund_id) REFERENCES zx_fund_product(fund_id)
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_zx_nav_fund_id_date "
            "ON zx_fund_nav(fund_id, nav_date)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_zx_nav_fund_code_date "
            "ON zx_fund_nav(fund_code, nav_date)"
        )
        conn.commit()
    finally:
        conn.close()


# =============================================================================
# Date parsing helper
# =============================================================================

def _parse_date(val) -> Optional[str]:
    """
    Convert various date representations to 'YYYY-MM-DD' string, or None.

    Handles: pd.Timestamp, datetime, date, str (several formats), NaN/None.
    """
    if val is None:
        return None
    # pandas NaN / NaT
    try:
        if pd.isna(val):
            return None
    except (TypeError, ValueError):
        pass

    if isinstance(val, pd.Timestamp):
        return val.strftime("%Y-%m-%d")
    if isinstance(val, datetime):
        return val.strftime("%Y-%m-%d")
    if isinstance(val, date):
        return val.strftime("%Y-%m-%d")

    s = str(val).strip()
    if not s or s in ("nan", "None", "NaT", "NAN", ""):
        return None

    # Try standard string formats (use only the first 10 chars for YYYY-MM-DD etc.)
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%d/%m/%Y", "%m/%d/%Y", "%Y%m%d"):
        try:
            return datetime.strptime(s[:10], fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue

    # Last resort: let pandas try
    try:
        ts = pd.to_datetime(s, errors="raise")
        return ts.strftime("%Y-%m-%d")
    except Exception:
        pass

    return None


def _clean_str(val) -> Optional[str]:
    """Return stripped string or None for NaN / empty values."""
    if val is None:
        return None
    try:
        if pd.isna(val):
            return None
    except (TypeError, ValueError):
        pass
    s = str(val).strip()
    return s if s and s not in ("nan", "None", "NaN") else None


# =============================================================================
# read_shelf — 臻选货架.xlsx
# =============================================================================

def read_shelf(shelf_path: str) -> dict:
    """
    Read 臻选货架.xlsx (Sheet1) and return a dict keyed by Code_Id.

    Column layout (0-indexed, per spec):
      0  策略标签-一级
      1  策略标签-二级
      3  对外展示
      7  跟踪基金全称
      10 管理人名称
      12 托管机构
      13 成立日期
      14 策略开始日期
      15 对标指数
      18 策略标签-三级
      19 Code_Id          ← primary key, matches ZXdatabase sheet name
      20 Code_Name
      21 Start_date       ← takes priority over col 14 and col 13

    start_date priority: col 21 (Start_date) > col 14 (策略开始日期) > col 13 (成立日期) > "2000-01-01"
    """
    df = pd.read_excel(shelf_path, sheet_name="Sheet1", header=0)
    products: dict = {}

    for _, row in df.iterrows():
        # ── Code_Id (col 19) ────────────────────────────────────────────────
        code_id_raw = row.iloc[19] if len(row) > 19 else None
        code_id = _clean_str(code_id_raw)
        if not code_id:
            continue

        # ── Dates ───────────────────────────────────────────────────────────
        inception_date = _parse_date(row.iloc[13] if len(row) > 13 else None)

        # Start_date priority
        start_date = _parse_date(row.iloc[21] if len(row) > 21 else None)
        if not start_date:
            start_date = _parse_date(row.iloc[14] if len(row) > 14 else None)
        if not start_date:
            start_date = inception_date
        if not start_date:
            start_date = "2000-01-01"

        products[code_id] = {
            "fund_name":     _clean_str(row.iloc[20] if len(row) > 20 else None),
            "strategy_l1":   _clean_str(row.iloc[0]),
            "strategy_l2":   _clean_str(row.iloc[1]),
            "strategy_l3":   _clean_str(row.iloc[18] if len(row) > 18 else None),
            "manager":       _clean_str(row.iloc[10]),
            "custodian":     _clean_str(row.iloc[12]),
            "inception_date": inception_date,
            "start_date":    start_date,
            "benchmark":     _clean_str(row.iloc[15] if len(row) > 15 else None),
            "display":       _clean_str(row.iloc[3]),
        }

    logger.info("read_shelf: loaded %d products from %s", len(products), shelf_path)
    return products


# =============================================================================
# compute_adj_nav — 复权净值计算
# =============================================================================

def compute_adj_nav(unit_navs: list, accum_navs: list) -> list:
    """
    Compute the adjusted NAV (复权净值) series.

    Formula derived from the spec (step-by-step expansion):
        adj_nav[0] = 1.0
        adj_nav[i] = adj_nav[i-1]
                     * (unit_nav[i-1] + accum_nav[i] - accum_nav[i-1])
                     / unit_nav[i-1]

    If unit_nav[i-1] == 0, carry adj_nav[i-1] forward to avoid division-by-zero.

    Args:
        unit_navs:  list of unit NAV floats, sorted ascending by date
        accum_navs: list of accumulated NAV floats, same length

    Returns:
        List of float adj_nav values, same length as inputs.
    """
    n = len(unit_navs)
    if n == 0:
        return []

    result = [0.0] * n
    result[0] = 1.0

    for i in range(1, n):
        prev_unit = unit_navs[i - 1]
        if prev_unit == 0:
            result[i] = result[i - 1]
        else:
            result[i] = (
                result[i - 1]
                * (prev_unit + accum_navs[i] - accum_navs[i - 1])
                / prev_unit
            )

    return result


# =============================================================================
# _read_nav_sheet — one ZXdatabase sheet
# =============================================================================

def _read_nav_sheet(xl: pd.ExcelFile, sheet_name: str) -> list:
    """
    Read one sheet from ZXdatabase.xlsx.

    Returns a list of (date_str, unit_nav, accum_nav) tuples:
      - sorted ascending by date
      - duplicates removed (last occurrence kept)
      - rows with null date or null unit_nav dropped

    accum_nav falls back to unit_nav when missing.
    """
    df = pd.read_excel(xl, sheet_name=sheet_name, header=0)

    if df.shape[1] < 2:
        logger.warning("Sheet %s has fewer than 2 columns — skipped", sheet_name)
        return []

    # Take first 3 columns regardless of their names
    n_cols = min(3, df.shape[1])
    df = df.iloc[:, :n_cols].copy()
    if n_cols == 2:
        df.columns = ["date", "unit_value"]
        df["accumulated_value"] = df["unit_value"]
    else:
        df.columns = ["date", "unit_value", "accumulated_value"]

    # Parse dates
    df["date_str"] = df["date"].apply(_parse_date)
    df = df.dropna(subset=["date_str"])

    # Parse NAV floats
    df["unit_value"] = pd.to_numeric(df["unit_value"], errors="coerce")
    df["accumulated_value"] = pd.to_numeric(df["accumulated_value"], errors="coerce")
    df = df.dropna(subset=["unit_value"])

    # Deduplicate by date (keep last as per spec)
    df = df.drop_duplicates(subset=["date_str"], keep="last")

    # Sort ascending
    df = df.sort_values("date_str").reset_index(drop=True)

    records = []
    for _, row in df.iterrows():
        unit = float(row["unit_value"])
        raw_accum = row["accumulated_value"]
        accum = float(raw_accum) if pd.notna(raw_accum) else unit
        records.append((row["date_str"], unit, accum))

    return records


# =============================================================================
# import_zx_excel — main entry point
# =============================================================================

def import_zx_excel(db_path: str, zxdatabase_path: str, shelf_path: str) -> dict:
    """
    Full import pipeline:
      1. Initialise the database (creates tables if absent).
      2. Read 臻选货架.xlsx → build Code_Id whitelist + product metadata.
      3. For each sheet in ZXdatabase.xlsx that is in the whitelist:
           a. Upsert the product row into zx_fund_product.
           b. Read, clean, and compute adj_nav for the NAV series.
           c. Bulk-insert NAV rows into zx_fund_nav (ON CONFLICT → update).
      4. Return stats dict.

    Args:
        db_path:          Path to the target SQLite database file.
        zxdatabase_path:  Path to ZXdatabase.xlsx.
        shelf_path:       Path to 臻选货架.xlsx.

    Returns:
        dict with keys: imported_funds, skipped_funds, total_nav_records
    """
    init_zx_database(db_path)

    products = read_shelf(shelf_path)
    logger.info("Whitelist loaded: %d funds", len(products))

    xl = pd.ExcelFile(zxdatabase_path)
    all_sheets = xl.sheet_names

    imported = 0
    skipped = 0
    total_nav = 0

    conn = sqlite3.connect(db_path)
    try:
        conn.execute("PRAGMA journal_mode=WAL")

        for sheet_name in all_sheets:
            if sheet_name not in products:
                skipped += 1
                logger.debug("Skipping sheet not in whitelist: %s", sheet_name)
                continue

            meta = products[sheet_name]

            # ── Upsert product metadata ────────────────────────────────────
            conn.execute(
                """
                INSERT INTO zx_fund_product
                    (fund_code, fund_name, strategy_l1, strategy_l2, strategy_l3,
                     manager, custodian, inception_date, start_date, benchmark_index, display)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(fund_code) DO UPDATE SET
                    fund_name       = excluded.fund_name,
                    strategy_l1     = excluded.strategy_l1,
                    strategy_l2     = excluded.strategy_l2,
                    strategy_l3     = excluded.strategy_l3,
                    manager         = excluded.manager,
                    custodian       = excluded.custodian,
                    inception_date  = excluded.inception_date,
                    start_date      = excluded.start_date,
                    benchmark_index = excluded.benchmark_index,
                    display         = excluded.display
                """,
                (
                    sheet_name,
                    meta["fund_name"],
                    meta["strategy_l1"],
                    meta["strategy_l2"],
                    meta["strategy_l3"],
                    meta["manager"],
                    meta["custodian"],
                    meta["inception_date"],
                    meta["start_date"],
                    meta["benchmark"],
                    meta["display"],
                ),
            )

            # ── Resolve integer fund_id ────────────────────────────────────
            row = conn.execute(
                "SELECT fund_id, fund_name FROM zx_fund_product WHERE fund_code = ?",
                (sheet_name,),
            ).fetchone()
            if row is None:
                logger.error("fund_id not found for %s after upsert — skipping NAV", sheet_name)
                imported += 1
                continue
            fund_id, fund_name_db = row

            # ── Read & clean NAV data ──────────────────────────────────────
            records = _read_nav_sheet(xl, sheet_name)
            if not records:
                logger.warning("No valid NAV rows for fund %s", sheet_name)
                imported += 1
                continue

            # ── Compute adj_nav ────────────────────────────────────────────
            unit_navs  = [r[1] for r in records]
            accum_navs = [r[2] for r in records]
            adj_navs   = compute_adj_nav(unit_navs, accum_navs)

            # ── Bulk insert NAV rows ───────────────────────────────────────
            nav_rows = [
                (fund_id, sheet_name, fund_name_db,
                 records[i][0], records[i][1], records[i][2], adj_navs[i],
                 "zx_excel")
                for i in range(len(records))
            ]
            conn.executemany(
                """
                INSERT INTO zx_fund_nav
                    (fund_id, fund_code, fund_name,
                     nav_date, unit_nav, accum_nav, adj_nav, data_source)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(fund_id, nav_date) DO UPDATE SET
                    unit_nav    = excluded.unit_nav,
                    accum_nav   = excluded.accum_nav,
                    adj_nav     = excluded.adj_nav,
                    fund_code   = excluded.fund_code,
                    fund_name   = excluded.fund_name,
                    data_source = excluded.data_source
                """,
                nav_rows,
            )

            total_nav += len(nav_rows)
            imported += 1

        conn.commit()

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    stats = {
        "imported_funds":   imported,
        "skipped_funds":    skipped,
        "total_nav_records": total_nav,
    }
    logger.info("Import complete: %s", stats)
    return stats


# =============================================================================
# CLI convenience
# =============================================================================

if __name__ == "__main__":
    import argparse

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="Import ZX fund Excel data to SQLite")
    parser.add_argument("--db",    default="zx_fund.db",          help="Target SQLite DB path")
    parser.add_argument("--zxdb",  default="demo/ZXdatabase.xlsx", help="ZXdatabase.xlsx path")
    parser.add_argument("--shelf", default="demo/臻选货架.xlsx",   help="臻选货架.xlsx path")
    args = parser.parse_args()

    result = import_zx_excel(args.db, args.zxdb, args.shelf)
    print(f"Done — imported {result['imported_funds']} funds, "
          f"skipped {result['skipped_funds']}, "
          f"total NAV records: {result['total_nav_records']}")
