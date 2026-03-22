#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
原油价格数据抓取模块 — 独立于 get_market_data.py

覆盖三个基准品种：
  WTI   — NYMEX 西德克萨斯中质原油，单位 USD/桶，via akshare futures_foreign_hist("CL")
  BRENT — ICE 布伦特原油，单位 USD/桶，via akshare futures_foreign_hist("OIL")
  SC    — 上海国际能源交易中心原油主力连续合约（SC888），单位 CNY/桶，via akshare futures_zh_daily_sina("SC888")

数据库表：crude_daily（写入 fund_data.db，与现有表共存）
"""

import logging
import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

DB_PATH: str = os.getenv("DB_PATH", "fund_data.db")

# ---------------------------------------------------------------------------
# 品种配置表
# ---------------------------------------------------------------------------
CRUDE_SYMBOLS = {
    "WTI": {
        "fetch": "foreign",       # 使用 ak.futures_foreign_hist
        "ak_symbol": "CL",
        "currency": "USD",
        "description": "WTI原油 (NYMEX)",
    },
    "BRENT": {
        "fetch": "foreign",       # 使用 ak.futures_foreign_hist
        "ak_symbol": "OIL",
        "currency": "USD",
        "description": "布伦特原油 (ICE)",
    },
    "SC": {
        "fetch": "domestic",      # 使用 ak.futures_zh_daily_sina
        "ak_symbol": "SC0",       # SC0 = 主力合约（SC888在新版akshare中已不可用）
        "currency": "CNY",
        "description": "上海原油 (INE SC0主力合约)",
    },
}

# Yahoo Finance 代码映射（仅 WTI / BRENT，SC 不在 Yahoo Finance）
CROSS_SYMBOLS = {
    "WTI":   "CL=F",   # NYMEX WTI 近月合约
    "BRENT": "BZ=F",   # ICE Brent 近月合约
}

# 交叉验证容忍度：差异超过此百分比则标记为未验证
CROSS_DIFF_THRESHOLD = 3.0

# ---------------------------------------------------------------------------
# 数据库初始化
# ---------------------------------------------------------------------------

def init_crude_db(conn: sqlite3.Connection):
    """创建 crude_daily 表（若不存在）和 sync_state 所需 key。"""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS crude_daily (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            ts_code    TEXT    NOT NULL,
            trade_date TEXT    NOT NULL,
            open       REAL,
            high       REAL,
            low        REAL,
            close      REAL    NOT NULL,
            volume     REAL,
            currency   TEXT    NOT NULL DEFAULT 'USD',
            UNIQUE(ts_code, trade_date)
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_crude_daily_code_date
        ON crude_daily(ts_code, trade_date)
    """)
    conn.commit()
    logger.info("crude_daily 表初始化完毕")


def init_cross_db(conn: sqlite3.Connection):
    """创建 crude_price_cross 交叉验证表（若不存在）。"""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS crude_price_cross (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            ts_code        TEXT    NOT NULL,
            trade_date     TEXT    NOT NULL,
            close_primary  REAL,
            close_alt      REAL,
            diff_pct       REAL,
            is_verified    INTEGER DEFAULT 1,
            verified_at    TEXT,
            UNIQUE(ts_code, trade_date)
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_crude_cross_code_date
        ON crude_price_cross(ts_code, trade_date)
    """)
    conn.commit()
    logger.info("crude_price_cross 表初始化完毕")


# ---------------------------------------------------------------------------
# sync_state 工具函数（复用 fund_data.db 中现有的 sync_state 表）
# ---------------------------------------------------------------------------

def _get_sync_key(conn: sqlite3.Connection, key: str) -> str:
    row = conn.execute(
        "SELECT value FROM sync_state WHERE key=?", (key,)
    ).fetchone()
    return row[0] if row else ""


def _set_sync_key(conn: sqlite3.Connection, key: str, value: str):
    conn.execute(
        "INSERT OR REPLACE INTO sync_state(key, value) VALUES(?,?)",
        (key, value),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# 增量同步辅助：获取该品种已有数据的最新日期
# ---------------------------------------------------------------------------

def _get_last_date(conn: sqlite3.Connection, ts_code: str) -> str | None:
    """返回 crude_daily 表中该品种最新的 trade_date (YYYYMMDD)，无数据返回 None。"""
    row = conn.execute(
        "SELECT MAX(trade_date) FROM crude_daily WHERE ts_code=?", (ts_code,)
    ).fetchone()
    return row[0] if row and row[0] else None


# ---------------------------------------------------------------------------
# WTI / Brent 同步（akshare futures_foreign_hist）
# ---------------------------------------------------------------------------

def _sync_foreign(conn: sqlite3.Connection, ts_code: str, ak_symbol: str, currency: str) -> int:
    """
    拉取国际原油期货历史日频数据并增量写入。
    ak.futures_foreign_hist 每次返回全量历史（数据源：新浪财经国际期货）。
    返回本次新增行数。
    """
    import akshare as ak

    last_date = _get_last_date(conn, ts_code)
    logger.info("[%s] 已有数据截止: %s，开始拉取 akshare futures_foreign_hist(%s)",
                ts_code, last_date or "无", ak_symbol)

    df = ak.futures_foreign_hist(symbol=ak_symbol)
    if df is None or df.empty:
        logger.warning("[%s] akshare 返回空数据", ts_code)
        return 0

    # 规范化日期列（akshare 返回 datetime 或 date 对象）
    df = df.copy()
    if "date" in df.columns:
        df["trade_date"] = df["date"].apply(
            lambda x: x.strftime("%Y%m%d") if hasattr(x, "strftime") else str(x).replace("-", "")
        )
    else:
        logger.warning("[%s] 找不到 date 列，columns=%s", ts_code, df.columns.tolist())
        return 0

    # 增量过滤：只写 last_date 之后的行
    if last_date:
        df = df[df["trade_date"] > last_date]

    if df.empty:
        logger.info("[%s] 无新增数据", ts_code)
        return 0

    rows = []
    for _, row in df.iterrows():
        rows.append((
            ts_code,
            row["trade_date"],
            float(row["open"])   if "open"   in row and row["open"]   == row["open"] else None,
            float(row["high"])   if "high"   in row and row["high"]   == row["high"] else None,
            float(row["low"])    if "low"    in row and row["low"]    == row["low"]  else None,
            float(row["close"])  if "close"  in row and row["close"]  == row["close"] else None,
            float(row["volume"]) if "volume" in row and row["volume"] == row["volume"] else None,
            currency,
        ))

    # 过滤掉 close 为 None 的行（不完整数据）
    rows = [r for r in rows if r[5] is not None]

    conn.executemany("""
        INSERT OR IGNORE INTO crude_daily
            (ts_code, trade_date, open, high, low, close, volume, currency)
        VALUES (?,?,?,?,?,?,?,?)
    """, rows)
    conn.commit()

    added = len(rows)
    logger.info("[%s] 新增 %d 行", ts_code, added)
    return added


# ---------------------------------------------------------------------------
# 上海 SC 同步（akshare futures_zh_daily_sina）
# ---------------------------------------------------------------------------

def _sync_sc(conn: sqlite3.Connection) -> int:
    """
    拉取上海国际能源交易中心原油期货主力连续合约（SC888）日频数据并增量写入。
    ak.futures_zh_daily_sina 返回全量历史，数据源：新浪财经。
    返回本次新增行数。
    """
    import akshare as ak

    ts_code = "SC"
    currency = "CNY"
    last_date = _get_last_date(conn, ts_code)
    logger.info("[SC] 已有数据截止: %s，开始拉取 akshare futures_zh_daily_sina(SC0)",
                last_date or "无")

    df = ak.futures_zh_daily_sina(symbol="SC0")
    if df is None or df.empty:
        logger.warning("[SC] akshare 返回空数据")
        return 0

    df = df.copy()

    # 规范化日期列（新浪接口返回的列名为 "date"，格式 YYYY-MM-DD 或 datetime）
    date_col = None
    for c in ["date", "日期", "trade_date"]:
        if c in df.columns:
            date_col = c
            break
    if date_col is None:
        logger.warning("[SC] 找不到日期列，columns=%s", df.columns.tolist())
        return 0

    df["trade_date"] = df[date_col].apply(
        lambda x: x.strftime("%Y%m%d") if hasattr(x, "strftime") else str(x).replace("-", "")
    )

    # 增量过滤
    if last_date:
        df = df[df["trade_date"] > last_date]

    if df.empty:
        logger.info("[SC] 无新增数据")
        return 0

    # 列名映射（SC0 返回 date, open, high, low, close, volume, hold, settle）
    col_map = {
        "open":  ["open", "开盘价", "开盘"],
        "high":  ["high", "最高价", "最高"],
        "low":   ["low",  "最低价", "最低"],
        "close": ["close","收盘价", "收盘", "settle"],  # 若无 close 则用 settle（结算价）
        "vol":   ["volume","vol","成交量"],
    }

    def _get_col(row, candidates):
        for c in candidates:
            if c in row.index and row[c] == row[c]:
                return float(row[c])
        return None

    rows = []
    for _, row in df.iterrows():
        close_val = _get_col(row, col_map["close"])
        if close_val is None:
            continue
        rows.append((
            ts_code,
            row["trade_date"],
            _get_col(row, col_map["open"]),
            _get_col(row, col_map["high"]),
            _get_col(row, col_map["low"]),
            close_val,
            _get_col(row, col_map["vol"]),
            currency,
        ))

    conn.executemany("""
        INSERT OR IGNORE INTO crude_daily
            (ts_code, trade_date, open, high, low, close, volume, currency)
        VALUES (?,?,?,?,?,?,?,?)
    """, rows)
    conn.commit()

    added = len(rows)
    logger.info("[SC] 新增 %d 行", added)
    return added


# ---------------------------------------------------------------------------
# 交叉验证：yfinance vs akshare
# ---------------------------------------------------------------------------

def _sync_cross_validate(conn: sqlite3.Connection, lookback_days: int = 90) -> int:
    """
    用 yfinance 数据（Yahoo Finance）与 crude_daily 中的 akshare 数据做交叉验证。
    只验证 WTI 和 BRENT（SC 不在 Yahoo Finance）。
    验证结果写入 crude_price_cross 表。
    返回更新/新增的行数。
    """
    try:
        import yfinance as yf
        import pandas as pd
    except ImportError:
        logger.warning("yfinance 未安装，跳过交叉验证。请执行: pip install yfinance")
        return 0

    from datetime import date, timedelta
    today = date.today()
    start_str = (today - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    end_str   = today.strftime("%Y-%m-%d")

    init_cross_db(conn)
    verified_at = datetime.now().isoformat()
    total_updated = 0

    for ts_code, yf_ticker in CROSS_SYMBOLS.items():
        logger.info("[cross] %s — yfinance(%s) %s ~ %s", ts_code, yf_ticker, start_str, end_str)
        try:
            df = yf.download(
                yf_ticker, start=start_str, end=end_str,
                progress=False, auto_adjust=True,
            )
        except Exception as e:
            logger.error("[cross] %s yfinance 拉取失败: %s", ts_code, e)
            continue

        if df is None or df.empty:
            logger.warning("[cross] %s yfinance 返回空", ts_code)
            continue

        # 兼容新旧版 yfinance 的列结构（MultiIndex vs flat）
        if isinstance(df.columns, pd.MultiIndex):
            close_series = df[("Close", yf_ticker)]
        else:
            close_series = df["Close"]

        symbol_updated = 0
        for date_idx, close_val in close_series.items():
            if pd.isna(close_val):
                continue
            trade_date = date_idx.strftime("%Y%m%d")
            close_alt = float(close_val)

            # 从 crude_daily 取 akshare 收盘价
            row = conn.execute(
                "SELECT close FROM crude_daily WHERE ts_code=? AND trade_date=?",
                (ts_code, trade_date),
            ).fetchone()
            close_primary = float(row[0]) if row else None

            diff_pct   = None
            is_verified = 1
            if close_primary and close_primary != 0:
                diff_pct    = abs(close_alt - close_primary) / close_primary * 100
                is_verified = 1 if diff_pct < CROSS_DIFF_THRESHOLD else 0
                if not is_verified:
                    logger.warning(
                        "[cross] %s %s 差异 %.2f%% (akshare=%.2f, yfinance=%.2f)",
                        ts_code, trade_date, diff_pct, close_primary, close_alt,
                    )

            conn.execute(
                """
                INSERT OR REPLACE INTO crude_price_cross
                    (ts_code, trade_date, close_primary, close_alt,
                     diff_pct, is_verified, verified_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (ts_code, trade_date, close_primary, close_alt,
                 diff_pct, is_verified, verified_at),
            )
            symbol_updated += 1

        conn.commit()
        logger.info("[cross] %s 更新 %d 行", ts_code, symbol_updated)
        total_updated += symbol_updated

    return total_updated


# ---------------------------------------------------------------------------
# 主入口：供 api.py 调度器调用 & 直接运行
# ---------------------------------------------------------------------------

def connect_and_fetch_crude(db_path: str = DB_PATH):
    """
    完整同步一次所有三个原油品种，并执行 yfinance 交叉验证。
    设计原则：单品种失败不影响其他品种；sync_state 记录状态供 API 查询。
    """
    conn = sqlite3.connect(db_path, check_same_thread=False)
    try:
        init_crude_db(conn)
        init_cross_db(conn)

        now_str = datetime.now().isoformat()
        _set_sync_key(conn, "crude_last_status", "running")
        _set_sync_key(conn, "crude_last_time", now_str)
        _set_sync_key(conn, "crude_last_error", "")

        total_added = 0
        errors = []

        for ts_code, cfg in CRUDE_SYMBOLS.items():
            try:
                if cfg["fetch"] == "foreign":
                    added = _sync_foreign(conn, ts_code, cfg["ak_symbol"], cfg["currency"])
                else:
                    added = _sync_sc(conn)
                total_added += added
            except Exception as e:
                msg = f"[{ts_code}] 同步失败: {e}"
                logger.error(msg, exc_info=True)
                errors.append(msg)

        # 交叉验证（yfinance vs akshare，失败不影响主数据）
        try:
            cross_updated = _sync_cross_validate(conn)
            logger.info("交叉验证完毕，更新 %d 行", cross_updated)
        except Exception as e:
            logger.error("交叉验证失败（不影响主数据入库）: %s", e)

        if errors:
            _set_sync_key(conn, "crude_last_status", "partial_error")
            _set_sync_key(conn, "crude_last_error", "; ".join(errors))
        else:
            _set_sync_key(conn, "crude_last_status", "success")

        _set_sync_key(conn, "crude_last_added", str(total_added))
        logger.info("原油同步完毕，共新增 %d 行", total_added)
        return total_added

    except Exception as e:
        logger.error("原油同步整体失败: %s", e, exc_info=True)
        try:
            _set_sync_key(conn, "crude_last_status", "error")
            _set_sync_key(conn, "crude_last_error", str(e))
        except Exception:
            pass
        raise
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 直接运行入口
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    print("=== 原油数据同步（直接运行模式）===")
    added = connect_and_fetch_crude()
    print(f"完成，新增 {added} 条记录")

    # 简单验证：打印每个品种的最新5条数据
    conn = sqlite3.connect(DB_PATH)
    for ts_code in CRUDE_SYMBOLS:
        rows = conn.execute(
            "SELECT trade_date, open, high, low, close, currency "
            "FROM crude_daily WHERE ts_code=? ORDER BY trade_date DESC LIMIT 5",
            (ts_code,),
        ).fetchall()
        print(f"\n--- {ts_code} 最新5条 ---")
        for r in rows:
            print(r)
    conn.close()
