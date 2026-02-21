#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Market data ingestion: A-share indices + financial futures via tushare.pro
Run standalone or called from api.py scheduler.
"""

import logging
import sqlite3
from datetime import datetime, timedelta

logger = logging.getLogger("market_data")

INDICES = [
    "000001.SH",   # 上证指数
    "399001.SZ",   # 深证成指
    "399006.SZ",   # 创业板指
    "000016.SH",   # 上证50
    "000300.SH",   # 沪深300
    "000905.SH",   # 中证500
    "000852.SH",   # 中证1000
    "932000.CSI",  # 中证2000
    "000688.SH",   # 科创50
]

INDEX_NAMES = {
    "000001.SH":  "上证指数",
    "399001.SZ":  "深证成指",
    "399006.SZ":  "创业板指",
    "000016.SH":  "上证50",
    "000300.SH":  "沪深300",
    "000905.SH":  "中证500",
    "000852.SH":  "中证1000",
    "932000.CSI": "中证2000",
    "000688.SH":  "科创50",
}

FUTURE_SYMBOLS = ["IF", "IC", "IH", "IM", "T", "TF", "TS"]


# =============================================================================
# Schema
# =============================================================================

def init_market_schema(db_path: str):
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS sync_state (
            key   TEXT PRIMARY KEY,
            value TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS index_daily (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            ts_code     TEXT NOT NULL,
            trade_date  TEXT NOT NULL,
            close       REAL, open REAL, high REAL, low REAL,
            vol         REAL, amount REAL, pct_chg REAL,
            inserted_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(ts_code, trade_date)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS futures_daily (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            ts_code     TEXT NOT NULL,
            symbol      TEXT NOT NULL,
            trade_date  TEXT NOT NULL,
            close       REAL, open REAL, high REAL, low REAL,
            vol         REAL, amount REAL, oi REAL,
            inserted_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(ts_code, trade_date)
        )
    """)
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_index_daily_code_date ON index_daily(ts_code, trade_date)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_futures_daily_code_date ON futures_daily(ts_code, trade_date)"
    )
    conn.commit()
    conn.close()
    logger.info("Market schema initialised at %s", db_path)


# =============================================================================
# Sync-state helpers (work on an open connection)
# =============================================================================

def _get_sync_key(conn, key: str):
    row = conn.execute("SELECT value FROM sync_state WHERE key=?", (key,)).fetchone()
    return row[0] if row else None


def _set_sync_key(conn, key: str, value: str):
    conn.execute("INSERT OR REPLACE INTO sync_state(key, value) VALUES(?,?)", (key, value))


# =============================================================================
# Active futures resolution
# =============================================================================

def get_active_futures(ts_api, symbols: list) -> list:
    """Return [(symbol, ts_code)] for the nearest active contract per symbol."""
    today = datetime.now().strftime("%Y%m%d")
    active = []
    try:
        df = ts_api.fut_basic(exchange="CFFEX", fut_type="1",
                              fields="ts_code,symbol,delist_date")
    except Exception as e:
        logger.warning("fut_basic call failed: %s", e)
        return active

    if df is None or df.empty:
        logger.warning("fut_basic returned no data")
        return active

    for sym in symbols:
        try:
            sym_df = df[df["ts_code"].str.startswith(sym)]
            # Keep contracts that haven't expired yet
            future_contracts = sym_df[sym_df["delist_date"] >= today]
            if future_contracts.empty:
                future_contracts = sym_df
            # Nearest expiry = smallest delist_date
            nearest = future_contracts.sort_values("delist_date").iloc[0]
            active.append((sym, nearest["ts_code"]))
            logger.info("Active contract for %s: %s", sym, nearest["ts_code"])
        except Exception as e:
            logger.warning("Could not resolve active contract for %s: %s", sym, e)

    return active


# =============================================================================
# Index sync
# =============================================================================

def sync_indices(ts_api, conn, since_date: str) -> int:
    """Fetch index daily data from since_date to today and INSERT OR IGNORE."""
    today = datetime.now().strftime("%Y%m%d")
    inserted_total = 0

    for ts_code in INDICES:
        try:
            df = ts_api.index_daily(ts_code=ts_code, start_date=since_date, end_date=today)
            if df is None or df.empty:
                logger.info("No new data for index %s", ts_code)
                continue
            rows = [
                (
                    ts_code,
                    str(row.get("trade_date", "")),
                    row.get("close"),
                    row.get("open"),
                    row.get("high"),
                    row.get("low"),
                    row.get("vol"),
                    row.get("amount"),
                    row.get("pct_chg"),
                )
                for _, row in df.iterrows()
            ]
            conn.executemany(
                """INSERT OR IGNORE INTO index_daily
                   (ts_code, trade_date, close, open, high, low, vol, amount, pct_chg)
                   VALUES (?,?,?,?,?,?,?,?,?)""",
                rows,
            )
            inserted_total += len(rows)
            logger.info("Synced %d rows for index %s", len(rows), ts_code)
        except Exception as e:
            logger.warning("Failed to sync index %s: %s", ts_code, e)

    return inserted_total


# =============================================================================
# Futures sync
# =============================================================================

def sync_futures(ts_api, conn, since_date: str) -> int:
    """Fetch futures daily data for active CFFEX contracts."""
    today = datetime.now().strftime("%Y%m%d")
    active_contracts = get_active_futures(ts_api, FUTURE_SYMBOLS)

    if not active_contracts:
        logger.warning("No active futures contracts resolved; skipping futures sync")
        return 0

    inserted_total = 0
    for symbol, ts_code in active_contracts:
        try:
            df = ts_api.fut_daily(ts_code=ts_code, start_date=since_date, end_date=today)
            if df is None or df.empty:
                logger.info("No new data for futures %s (%s)", symbol, ts_code)
                continue
            rows = [
                (
                    ts_code,
                    symbol,
                    str(row.get("trade_date", "")),
                    row.get("close"),
                    row.get("open"),
                    row.get("high"),
                    row.get("low"),
                    row.get("vol"),
                    row.get("amount"),
                    row.get("oi"),
                )
                for _, row in df.iterrows()
            ]
            conn.executemany(
                """INSERT OR IGNORE INTO futures_daily
                   (ts_code, symbol, trade_date, close, open, high, low, vol, amount, oi)
                   VALUES (?,?,?,?,?,?,?,?,?,?)""",
                rows,
            )
            inserted_total += len(rows)
            logger.info("Synced %d rows for futures %s (%s)", len(rows), symbol, ts_code)
        except Exception as e:
            logger.warning("Failed to sync futures %s (%s): %s", symbol, ts_code, e)

    return inserted_total


# =============================================================================
# Main entry point
# =============================================================================

def connect_and_fetch_market(token: str, db_path: str):
    """Full market sync. Called by api.py scheduler or standalone."""
    if not token:
        raise ValueError("TUSHARE_TOKEN is empty")

    import tushare as ts
    ts.set_token(token)
    ts_api = ts.pro_api()

    init_market_schema(db_path)

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    try:
        # Determine start dates for incremental sync
        last_index_date = _get_sync_key(conn, "market_index_last_date")
        if last_index_date:
            since_idx = (
                datetime.strptime(last_index_date, "%Y%m%d") + timedelta(days=1)
            ).strftime("%Y%m%d")
        else:
            since_idx = (datetime.now() - timedelta(days=90)).strftime("%Y%m%d")

        last_futures_date = _get_sync_key(conn, "market_futures_last_date")
        if last_futures_date:
            since_fut = (
                datetime.strptime(last_futures_date, "%Y%m%d") + timedelta(days=1)
            ).strftime("%Y%m%d")
        else:
            since_fut = (datetime.now() - timedelta(days=90)).strftime("%Y%m%d")

        today = datetime.now().strftime("%Y%m%d")

        sync_indices(ts_api, conn, since_idx)
        _set_sync_key(conn, "market_index_last_date", today)

        sync_futures(ts_api, conn, since_fut)
        _set_sync_key(conn, "market_futures_last_date", today)

        conn.commit()
        logger.info("Market data sync completed successfully")
    finally:
        conn.close()


# =============================================================================
# Standalone entry point
# =============================================================================

if __name__ == "__main__":
    import os
    from dotenv import load_dotenv

    load_dotenv()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    token = os.getenv("TUSHARE_TOKEN", "")
    db_path = os.getenv("DB_PATH", "fund_data.db")
    connect_and_fetch_market(token, db_path)
