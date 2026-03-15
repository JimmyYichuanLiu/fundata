"""
get_market_data.py — A-share index + CFFEX financial futures daily / intraday data

Data sources (all free, no API key required):
  • Index daily      : Tencent QQ Finance kline API (proxy.finance.qq.com)
  • Futures daily    : akshare get_futures_daily (www.cffex.com.cn official data)
  • Index 5-min      : akshare stock_zh_a_minute (Sina Finance)
  • Futures 5-min    : akshare futures_zh_minute_sina (Sina Finance)
  • Active contracts : akshare match_main_contract (cffex)

Daily mode  : sync_indices / sync_futures → index_daily / futures_daily tables
Intraday mode: sync_index_5min / sync_futures_5min → index_5min / futures_5min tables

Note: 932000.CSI (中证2000) is unavailable via Tencent/Sina; it is silently skipped.
"""

import json
import logging
import os
import random
import re
import sqlite3
import time
from datetime import datetime, timedelta

import pytz
import requests

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Proxy bypass for Chinese domestic data sources
# ---------------------------------------------------------------------------
# On Windows with a Clash/system proxy configured via IE settings, Python's
# requests auto-routes ALL traffic through it.  Chinese domestic sites should
# be accessed directly.

_CN_NO_PROXY = (
    "finance.sina.com.cn,stock2.finance.sina.com.cn,hq.sinajs.cn,"
    "proxy.finance.qq.com,qt.gtimg.cn,www.cffex.com.cn"
)
_existing_no_proxy = os.environ.get("NO_PROXY", os.environ.get("no_proxy", ""))
if _existing_no_proxy:
    os.environ["NO_PROXY"] = f"{_existing_no_proxy},{_CN_NO_PROXY}"
else:
    os.environ["NO_PROXY"] = _CN_NO_PROXY

# ---------------------------------------------------------------------------
# Constants (exported; api.py imports INDEX_NAMES)
# ---------------------------------------------------------------------------

INDICES = [
    "000001.SH",  # 上证指数
    "399001.SZ",  # 深证成指
    "399006.SZ",  # 创业板指
    "000016.SH",  # 上证50
    "000300.SH",  # 沪深300
    "000905.SH",  # 中证500
    "000852.SH",  # 中证1000
    "932000.CSI", # 中证2000 (data unavailable via accessible APIs, silently skipped)
    "000688.SH",  # 科创50
]

INDEX_NAMES: dict = {
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

FUTURE_SYMBOLS  = ["IF", "IC", "IH", "IM", "T", "TF", "TS"]

# Stock-index futures — have a corresponding spot index, quarterly delivery
EQUITY_FUTURES  = ["IF", "IC", "IH", "IM"]
# Bond futures — no spot basis analysis; only active contract needed
BOND_FUTURES    = ["T", "TF", "TS"]

# Quarterly delivery months for CFFEX stock-index futures
_QUARTERLY_MONTHS = [3, 6, 9, 12]

_SHANGHAI_TZ = pytz.timezone("Asia/Shanghai")

# Tencent / Sina symbol mapping (ts_code -> exchange-prefixed symbol)
# Used for both daily kline (Tencent) and 5-min (Sina); same sh/sz prefix format.
# 932000.CSI is excluded — not available via Tencent or Sina.
_EXCHANGE_SYMBOL: dict = {
    "000001.SH": "sh000001",
    "399001.SZ": "sz399001",
    "399006.SZ": "sz399006",
    "000016.SH": "sh000016",
    "000300.SH": "sh000300",
    "000905.SH": "sh000905",
    "000852.SH": "sh000852",
    "000688.SH": "sh000688",
}

# ---------------------------------------------------------------------------
# Trading-hours helpers
# ---------------------------------------------------------------------------

def is_trading_hours() -> bool:
    """Return True when Shanghai time is within an intraday polling window.

    Windows (with 5-minute buffer):
      Morning  : 09:25 – 11:35
      Afternoon: 12:55 – 15:05
    Weekends and evenings return False.
    """
    from datetime import time as dtime
    now = datetime.now(_SHANGHAI_TZ)
    if now.weekday() >= 5:          # Sat=5, Sun=6
        return False
    t = now.time()
    morning   = dtime(9, 25)  <= t <= dtime(11, 35)
    afternoon = dtime(12, 55) <= t <= dtime(15, 5)
    return morning or afternoon


def is_trading_day() -> bool:
    """Return True if today is Mon–Fri (no public-holiday calendar)."""
    return datetime.now(_SHANGHAI_TZ).weekday() < 5


def _get_third_friday(year: int, month: int) -> datetime:
    """Return the third Friday of year/month — CFFEX stock-index futures expiry."""
    first_day = datetime(year, month, 1)
    days_to_friday = (4 - first_day.weekday()) % 7   # Monday=0, Friday=4
    first_friday = first_day + timedelta(days=days_to_friday)
    return first_friday + timedelta(weeks=2)


def _get_equity_quarterly_codes(since_year: int = 2025) -> list:
    """Return [(symbol, ts_code, expiry_datetime)] for all quarterly equity-index
    futures contracts with expiry from since_year up to the next two quarters.

    Covers: IF2503/06/09/12, IF2603/06 (and equivalents for IC/IH/IM).
    """
    now = datetime.now()
    cutoff = now + timedelta(days=210)   # ~2 quarters ahead
    results = []
    for symbol in EQUITY_FUTURES:
        done = False
        for year in range(since_year, now.year + 2):
            if done:
                break
            for month in _QUARTERLY_MONTHS:
                expiry = _get_third_friday(year, month)
                if expiry < datetime(since_year, 1, 1):
                    continue
                if expiry > cutoff:
                    done = True
                    break
                ts_code = f"{symbol}{year % 100:02d}{month:02d}"
                results.append((symbol, ts_code, expiry))
    return results


# ---------------------------------------------------------------------------
# Code-conversion helpers
# ---------------------------------------------------------------------------

def ts_code_to_ak_symbol(ts_code: str) -> str:
    """'000001.SH' -> '000001'  (strip exchange suffix)."""
    return ts_code.split(".")[0]


def _yyyymmdd(date_str: str) -> str:
    """'2025-02-25' -> '20250225'  (handle both formats)."""
    return date_str.replace("-", "")


def _display_date(yyyymmdd: str) -> str:
    """'20250225' -> '2025-02-25'."""
    return f"{yyyymmdd[:4]}-{yyyymmdd[4:6]}-{yyyymmdd[6:]}"

# ---------------------------------------------------------------------------
# DB schema
# ---------------------------------------------------------------------------

def _get_sync_key(conn: sqlite3.Connection, key: str) -> str:
    row = conn.execute(
        "SELECT value FROM sync_state WHERE key=?", (key,)
    ).fetchone()
    return row[0] if row else ""


def _set_sync_key(conn: sqlite3.Connection, key: str, value: str) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO sync_state (key, value) VALUES (?,?)", (key, value)
    )


def init_market_schema(db_path: str) -> None:
    """Create / migrate all market tables.  Idempotent."""
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

    # 5-minute intraday tables
    conn.execute("""
        CREATE TABLE IF NOT EXISTS index_5min (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            ts_code     TEXT NOT NULL,
            bar_time    TEXT NOT NULL,
            open        REAL, high REAL, low REAL, close REAL,
            vol         REAL, amount REAL,
            inserted_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(ts_code, bar_time)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS futures_5min (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            ts_code     TEXT NOT NULL,
            symbol      TEXT NOT NULL,
            bar_time    TEXT NOT NULL,
            open        REAL, high REAL, low REAL, close REAL,
            vol         REAL, hold REAL,
            inserted_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(ts_code, bar_time)
        )
    """)

    # Real-time snapshot table (latest quotes, overwritten each poll)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS market_realtime (
            ts_code     TEXT PRIMARY KEY,
            name        TEXT,
            category    TEXT NOT NULL,
            price       REAL,
            open        REAL,
            high        REAL,
            low         REAL,
            prev_close  REAL,
            pct_chg     REAL,
            volume      REAL,
            amount      REAL,
            extra_json  TEXT,
            updated_at  DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)

    conn.execute("CREATE INDEX IF NOT EXISTS idx_index_daily_code_date   ON index_daily(ts_code, trade_date)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_futures_daily_code_date ON futures_daily(ts_code, trade_date)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_index_5min_code_time    ON index_5min(ts_code, bar_time)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_futures_5min_code_time  ON futures_5min(ts_code, bar_time)")
    conn.commit()
    conn.close()

# ---------------------------------------------------------------------------
# Active futures resolution
# ---------------------------------------------------------------------------

def get_active_futures_ak(symbols: list) -> list:
    """Return [(base_symbol, contract_code)] for nearest active CFFEX contracts.

    Tries ak.match_main_contract(symbol='cffex') first; falls back to
    constructing current/next-month codes if akshare fails.
    """
    import akshare as ak
    try:
        import pandas as pd
        _pd = pd
    except ImportError:
        _pd = None

    active = []
    try:
        result = ak.match_main_contract(symbol="cffex")
        # Normalise: str / list / Series / DataFrame all handled
        if _pd is not None and isinstance(result, _pd.DataFrame):
            contracts = result.iloc[:, 0].astype(str).tolist()
        elif isinstance(result, str):
            # akshare may return comma-separated, space-separated, or mixed
            contracts = re.split(r"[,\s]+", result.strip())
        else:
            contracts = [str(x) for x in result]

        for contract in contracts:
            contract = contract.strip().upper()
            m = re.match(r"^([A-Z]+)", contract)
            if not m:
                continue
            base = m.group(1)
            if base in symbols:
                active.append((base, contract))
                logger.info("Active CFFEX contract: %s -> %s", base, contract)

        # If match_main_contract returned nothing at all, fall back completely
        if not active:
            logger.warning("match_main_contract returned no contracts — using fallback")
            return _fallback_active_futures(symbols)

        # Do NOT add fallback for missing symbols: if match_main_contract succeeded
        # but didn't include a symbol, that contract likely has no active data.

    except Exception as exc:
        logger.warning("match_main_contract failed (%s) — using fallback", exc)
        return _fallback_active_futures(symbols)

    return active


def _fallback_active_futures(symbols: list) -> list:
    """Construct plausible contract codes when akshare is unavailable."""
    now = datetime.now()
    codes = []
    for sym in symbols:
        month = now.month
        year  = now.year
        codes.append((sym, f"{sym}{year % 100:02d}{month:02d}"))
    return codes

# ---------------------------------------------------------------------------
# Daily index sync — Tencent QQ Finance kline API
# ---------------------------------------------------------------------------

_TENCENT_KLINE_URL = (
    "https://proxy.finance.qq.com/ifzqgtimg/appstock/app/newfqkline/get"
)


def _fetch_index_tencent(tx_symbol: str, since_yyyymmdd: str, to_yyyymmdd: str) -> list:
    """Fetch daily kline from Tencent QQ Finance API.

    Returns list of dicts with keys: trade_date, open, close, high, low,
    vol, pct_chg, amount.  trade_date is YYYYMMDD string.

    The API response is JSONP: kline_dayqfq={...}
    Row format: [date, open, close, high, low, vol, {}, pct_chg, amount, ...]
    """
    since_d = _display_date(since_yyyymmdd)
    to_d    = _display_date(to_yyyymmdd)

    # Tencent API maximum: 2000 rows per request (sufficient for ~8 years)
    params = {
        "_var": "kline_dayqfq",
        "param": f"{tx_symbol},day,{since_d},{to_d},2000,qfq",
        "r": str(random.random()),
    }
    resp = requests.get(_TENCENT_KLINE_URL, params=params, timeout=15)
    resp.raise_for_status()

    text = resp.text
    eq_pos = text.index("=")
    data = json.loads(text[eq_pos + 1:])
    raw_inner = data.get("data", {})
    if not isinstance(raw_inner, dict):
        # API returned an empty list when count > 2000 — shouldn't happen with limit=2000
        return []
    rows_raw = raw_inner.get(tx_symbol, {}).get("day", [])

    records = []
    for row in rows_raw:
        if not isinstance(row, list) or len(row) < 9:
            continue
        trade_date = str(row[0]).replace("-", "")
        if len(trade_date) != 8:
            continue
        try:
            records.append({
                "trade_date": trade_date,
                "open":     float(row[1]) if row[1] else None,
                "close":    float(row[2]) if row[2] else None,
                "high":     float(row[3]) if row[3] else None,
                "low":      float(row[4]) if row[4] else None,
                "vol":      float(row[5]) if row[5] else None,
                "pct_chg":  float(row[7]) if row[7] else None,
                "amount":   float(row[8]) if row[8] else None,
            })
        except (ValueError, TypeError, IndexError):
            continue

    return records


def sync_indices(conn: sqlite3.Connection, since_date: str) -> int:
    """Fetch daily index data from Tencent QQ Finance and INSERT OR IGNORE.

    Includes open/high/low/close/vol/amount/pct_chg.
    Skips 932000.CSI (中证2000) — unavailable via Tencent.
    """
    today = datetime.now().strftime("%Y%m%d")
    inserted_total = 0

    for ts_code, tx_sym in _EXCHANGE_SYMBOL.items():
        try:
            records = _fetch_index_tencent(tx_sym, since_date, today)
            if not records:
                logger.info("No new data for index %s", ts_code)
                time.sleep(0.2)
                continue

            rows = [
                (
                    ts_code,
                    r["trade_date"],
                    r["close"],
                    r["open"],
                    r["high"],
                    r["low"],
                    r["vol"],
                    r["amount"],
                    r["pct_chg"],
                )
                for r in records
            ]
            conn.executemany(
                """INSERT OR IGNORE INTO index_daily
                   (ts_code, trade_date, close, open, high, low, vol, amount, pct_chg)
                   VALUES (?,?,?,?,?,?,?,?,?)""",
                rows,
            )
            inserted_total += len(rows)
            logger.info("index %s: %d rows inserted", ts_code, len(rows))

        except Exception as exc:
            logger.warning("sync_indices failed for %s: %s", ts_code, exc)

        time.sleep(0.2)

    return inserted_total

# ---------------------------------------------------------------------------
# Daily futures sync — akshare / CFFEX official website
# ---------------------------------------------------------------------------

def sync_futures(conn: sqlite3.Connection, since_date: str) -> int:
    """Fetch CFFEX futures daily data via Sina Finance.

    Equity index futures (IF/IC/IH/IM):
      • Syncs ALL quarterly contracts since 2025-01-01.
      • Expired contracts that already have rows in the DB are skipped.
      • Contracts with no rows yet are backfilled from 2025-01-01 regardless
        of since_date (one-time historical backfill).
      • Active contracts are synced incrementally from since_date.

    Bond futures (T/TF/TS):
      • Only the currently active contract is synced, incrementally.
    """
    import akshare as ak
    inserted_total = 0
    now = datetime.now()

    # ── Equity index futures: all quarterly contracts since 2025 ─────────────
    quarterly = _get_equity_quarterly_codes(since_year=2025)

    for base_sym, ts_code, expiry in quarterly:
        try:
            is_expired = expiry.date() < now.date()

            # Skip expired contracts that are already fully loaded
            if is_expired:
                existing = conn.execute(
                    "SELECT COUNT(*) FROM futures_daily WHERE ts_code=?", (ts_code,)
                ).fetchone()[0]
                if existing > 0:
                    logger.debug("Skipping expired %s (%d rows already)", ts_code, existing)
                    continue

            df = ak.futures_zh_daily_sina(symbol=ts_code)
            if df is None or df.empty:
                logger.info("No Sina data for %s", ts_code)
                time.sleep(0.3)
                continue

            # First ever fetch for this contract → backfill from 2025-01-01
            # Subsequent fetches → incremental from since_date
            has_data = conn.execute(
                "SELECT COUNT(*) FROM futures_daily WHERE ts_code=?", (ts_code,)
            ).fetchone()[0]
            effective_since = "20250101" if has_data == 0 else since_date

            rows = []
            for _, row in df.iterrows():
                td = str(row.get("date", "")).replace("-", "")
                if len(td) != 8 or td < effective_since:
                    continue
                rows.append((
                    ts_code, base_sym, td,
                    row.get("close"), row.get("open"), row.get("high"), row.get("low"),
                    row.get("volume"), None, row.get("hold"),
                ))

            if rows:
                conn.executemany(
                    """INSERT OR IGNORE INTO futures_daily
                       (ts_code, symbol, trade_date, close, open, high, low, vol, amount, oi)
                       VALUES (?,?,?,?,?,?,?,?,?,?)""",
                    rows,
                )
            inserted_total += len(rows)
            logger.info("futures %s: %d rows (since=%s)", ts_code, len(rows), effective_since)

        except Exception as exc:
            logger.warning("sync_futures failed for %s: %s", ts_code, exc)

        time.sleep(0.3)

    # ── Bond futures: active contract only ──────────────────────────────────
    active_bond = get_active_futures_ak(BOND_FUTURES)

    for base_sym, ts_code in active_bond:
        try:
            df = ak.futures_zh_daily_sina(symbol=ts_code)
            if df is None or df.empty:
                logger.info("No Sina data for %s", ts_code)
                time.sleep(0.2)
                continue

            rows = []
            for _, row in df.iterrows():
                td = str(row.get("date", "")).replace("-", "")
                if len(td) != 8 or td < since_date:
                    continue
                rows.append((
                    ts_code, base_sym, td,
                    row.get("close"), row.get("open"), row.get("high"), row.get("low"),
                    row.get("volume"), None, row.get("hold"),
                ))

            if rows:
                conn.executemany(
                    """INSERT OR IGNORE INTO futures_daily
                       (ts_code, symbol, trade_date, close, open, high, low, vol, amount, oi)
                       VALUES (?,?,?,?,?,?,?,?,?,?)""",
                    rows,
                )
            inserted_total += len(rows)
            logger.info("futures %s: %d rows (since=%s)", ts_code, len(rows), since_date)

        except Exception as exc:
            logger.warning("sync_futures failed for %s: %s", ts_code, exc)

        time.sleep(0.2)

    return inserted_total

# ---------------------------------------------------------------------------
# 5-minute intraday sync — indices (Sina Finance)
# ---------------------------------------------------------------------------

def sync_index_5min(conn: sqlite3.Connection) -> int:
    """Fetch today's 5-minute bars for all available indices via Sina Finance.

    Uses akshare stock_zh_a_minute with sh/sz-prefixed symbols.
    Skips 932000.CSI (not available via Sina).
    """
    if not is_trading_hours():
        return 0

    import akshare as ak
    inserted_total = 0

    for ts_code, sina_sym in _EXCHANGE_SYMBOL.items():
        try:
            df = ak.stock_zh_a_minute(symbol=sina_sym, period="5", adjust="")
            if df is None or df.empty:
                time.sleep(0.2)
                continue

            rows = []
            for _, row in df.iterrows():
                bar_time = str(row.get("day", ""))
                rows.append((
                    ts_code, bar_time,
                    row.get("open"), row.get("high"), row.get("low"), row.get("close"),
                    row.get("volume"), None,  # amount not available in Sina minute data
                ))

            conn.executemany(
                """INSERT OR IGNORE INTO index_5min
                   (ts_code, bar_time, open, high, low, close, vol, amount)
                   VALUES (?,?,?,?,?,?,?,?)""",
                rows,
            )
            inserted_total += len(rows)
            logger.info("5min index %s: %d bars", ts_code, len(rows))

        except Exception as exc:
            logger.warning("sync_index_5min failed for %s: %s", ts_code, exc)

        time.sleep(0.2)

    return inserted_total

# ---------------------------------------------------------------------------
# 5-minute intraday sync — futures (Sina Finance)
# ---------------------------------------------------------------------------

def sync_futures_5min(conn: sqlite3.Connection, active_contracts: list) -> int:
    """Fetch today's 5-minute bars for active CFFEX contracts via Sina Finance."""
    if not is_trading_hours():
        return 0

    import akshare as ak
    inserted_total = 0

    for base_sym, ts_code in active_contracts:
        try:
            df = ak.futures_zh_minute_sina(symbol=ts_code, period="5")
            if df is None or df.empty:
                time.sleep(0.2)
                continue

            rows = []
            for _, row in df.iterrows():
                bar_time = str(row.get("datetime", ""))
                rows.append((
                    ts_code, base_sym, bar_time,
                    row.get("open"), row.get("high"), row.get("low"), row.get("close"),
                    row.get("volume"), row.get("hold"),
                ))

            conn.executemany(
                """INSERT OR IGNORE INTO futures_5min
                   (ts_code, symbol, bar_time, open, high, low, close, vol, hold)
                   VALUES (?,?,?,?,?,?,?,?,?)""",
                rows,
            )
            inserted_total += len(rows)
            logger.info("5min futures %s: %d bars", ts_code, len(rows))

        except Exception as exc:
            logger.warning("sync_futures_5min failed for %s: %s", ts_code, exc)

        time.sleep(0.3)

    return inserted_total

# ---------------------------------------------------------------------------
# Real-time snapshot sync — index + futures (East Money / Sina)
# ---------------------------------------------------------------------------

# Mapping from East Money bare code to our ts_code
_EM_CODE_TO_TS: dict = {
    "000001": "000001.SH",
    "399001": "399001.SZ",
    "399006": "399006.SZ",
    "000016": "000016.SH",
    "000300": "000300.SH",
    "000905": "000905.SH",
    "000852": "000852.SH",
    "932000": "932000.CSI",
    "000688": "000688.SH",
}


def sync_realtime_indices(conn: sqlite3.Connection) -> int:
    """Fetch real-time index quotes via akshare East Money API.

    Uses ak.stock_zh_index_spot_em() to get latest prices for all major indices.
    Writes into market_realtime table (INSERT OR REPLACE).
    """
    import akshare as ak
    updated = 0
    now_str = datetime.now(_SHANGHAI_TZ).strftime("%Y-%m-%d %H:%M:%S")

    # Fetch from multiple categories to cover all our indices
    for category in ["沪深重要指数", "上证系列指数", "中证系列指数"]:
        try:
            df = ak.stock_zh_index_spot_em(symbol=category)
            if df is None or df.empty:
                continue

            for _, row in df.iterrows():
                code = str(row.get("代码", "")).strip()
                ts_code = _EM_CODE_TO_TS.get(code)
                if not ts_code:
                    continue

                name = str(row.get("名称", ""))
                price = row.get("最新价")
                open_p = row.get("今开")
                high = row.get("最高")
                low = row.get("最低")
                prev_close = row.get("昨收")
                pct_chg = row.get("涨跌幅")
                volume = row.get("成交量")
                amount = row.get("成交额")

                conn.execute(
                    """INSERT OR REPLACE INTO market_realtime
                       (ts_code, name, category, price, open, high, low,
                        prev_close, pct_chg, volume, amount, updated_at)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (ts_code, name, "index", price, open_p, high, low,
                     prev_close, pct_chg, volume, amount, now_str),
                )
                updated += 1

        except Exception as exc:
            logger.warning("sync_realtime_indices failed for %s: %s", category, exc)
        time.sleep(0.3)

    logger.info("Realtime index snapshot: %d updated", updated)
    return updated


def sync_realtime_futures(conn: sqlite3.Connection) -> int:
    """Fetch real-time futures quotes via akshare Sina API.

    Uses ak.futures_zh_spot() with market='FF' for CFFEX financial futures.
    Writes into market_realtime table (INSERT OR REPLACE).
    """
    import akshare as ak
    updated = 0
    now_str = datetime.now(_SHANGHAI_TZ).strftime("%Y-%m-%d %H:%M:%S")

    try:
        # Get main contract codes
        cffex_result = ak.match_main_contract(symbol="cffex")
        if isinstance(cffex_result, str):
            cffex_text = cffex_result.strip()
        else:
            try:
                import pandas as pd
                if isinstance(cffex_result, pd.DataFrame):
                    cffex_text = ",".join(cffex_result.iloc[:, 0].astype(str).tolist())
                else:
                    cffex_text = ",".join(str(x) for x in cffex_result)
            except Exception:
                cffex_text = ",".join(str(x) for x in cffex_result)

        if not cffex_text:
            logger.warning("match_main_contract returned empty for realtime futures")
            return 0

        # Parse contract codes to build symbol mapping
        contract_codes = [c.strip().upper() for c in re.split(r"[,\s]+", cffex_text) if c.strip()]
        code_to_symbol = {}
        for code in contract_codes:
            m = re.match(r"^([A-Z]+)", code)
            if m:
                code_to_symbol[code] = m.group(1)

        df = ak.futures_zh_spot(symbol=cffex_text, market="FF", adjust="0")
        if df is None or df.empty:
            return 0

        for idx_row, row in df.iterrows():
            # futures_zh_spot returns rows in same order as input symbols
            if idx_row < len(contract_codes):
                ts_code = contract_codes[idx_row]
                base_symbol = code_to_symbol.get(ts_code, "")
            else:
                continue

            name = str(row.get("symbol", ""))
            price = row.get("current_price")
            open_p = row.get("open")
            high = row.get("high")
            low = row.get("low")
            prev_close = row.get("last_close")
            volume = row.get("volume")
            amount = row.get("amount")
            hold = row.get("hold")
            last_settle = row.get("last_settle_price")

            # Calculate pct_chg from prev settlement
            pct_chg = None
            if price and last_settle and last_settle != 0:
                pct_chg = round((price - last_settle) / last_settle * 100, 4)

            extra = json.dumps({
                "hold": hold,
                "last_settle": last_settle,
                "base_symbol": base_symbol,
            })

            conn.execute(
                """INSERT OR REPLACE INTO market_realtime
                   (ts_code, name, category, price, open, high, low,
                    prev_close, pct_chg, volume, amount, extra_json, updated_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (ts_code, name, "futures", price, open_p, high, low,
                 prev_close, pct_chg, volume, amount, extra, now_str),
            )
            updated += 1

    except Exception as exc:
        logger.warning("sync_realtime_futures failed: %s", exc)

    logger.info("Realtime futures snapshot: %d updated", updated)
    return updated


def connect_and_fetch_realtime(db_path: str) -> None:
    """Real-time snapshot sync: index + futures quotes.

    Called by api.py scheduler every 5 minutes during trading hours.
    No API token required.
    """
    if not is_trading_hours():
        return

    init_market_schema(db_path)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    try:
        n_idx = sync_realtime_indices(conn)
        n_fut = sync_realtime_futures(conn)
        conn.commit()
        logger.info("Realtime sync done: %d index, %d futures", n_idx, n_fut)
    finally:
        conn.close()

# ---------------------------------------------------------------------------
# Main entry points (exported / called by api.py)
# ---------------------------------------------------------------------------

def connect_and_fetch_market(db_path: str) -> None:
    """Daily market sync: index_daily + futures_daily.

    Called by api.py scheduler (11:30 and 15:15 Asia/Shanghai) or standalone.
    No API token required.
    """
    init_market_schema(db_path)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    try:
        today = datetime.now().strftime("%Y%m%d")

        # --- Indices ---
        last_idx = _get_sync_key(conn, "market_index_last_date")
        if last_idx:
            since_idx = (
                datetime.strptime(last_idx, "%Y%m%d") + timedelta(days=1)
            ).strftime("%Y%m%d")
        else:
            # Initial sync: fetch ~365 calendar days (~250 trading days) for frontend chart
            since_idx = (datetime.now() - timedelta(days=365)).strftime("%Y%m%d")

        n_idx = 0
        if since_idx <= today:
            n_idx = sync_indices(conn, since_idx)
        else:
            logger.info("Index data already up to date (last=%s)", last_idx)
        _set_sync_key(conn, "market_index_last_date", today)

        # --- Futures ---
        last_fut = _get_sync_key(conn, "market_futures_last_date")
        if last_fut:
            since_fut = (
                datetime.strptime(last_fut, "%Y%m%d") + timedelta(days=1)
            ).strftime("%Y%m%d")
        else:
            # Initial sync: 90 calendar days (~63 trading days).
            # CFFEX makes one HTTP call per trading day; keeping this small avoids
            # multi-minute initial fetches.  History grows automatically on each run.
            since_fut = (datetime.now() - timedelta(days=90)).strftime("%Y%m%d")

        n_fut = 0
        if since_fut <= today:
            n_fut = sync_futures(conn, since_fut)
        else:
            logger.info("Futures data already up to date (last=%s)", last_fut)
        _set_sync_key(conn, "market_futures_last_date", today)

        conn.commit()
        logger.info("Daily market sync done: %d index rows, %d futures rows", n_idx, n_fut)

    finally:
        conn.close()


def connect_and_fetch_market_5min(db_path: str) -> None:
    """Intraday 5-minute sync. Called every 5 minutes; skips outside trading hours."""
    if not is_trading_hours():
        return

    init_market_schema(db_path)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    try:
        active_contracts = get_active_futures_ak(FUTURE_SYMBOLS)
        n_idx = sync_index_5min(conn)
        n_fut = sync_futures_5min(conn, active_contracts)
        conn.commit()
        logger.info("5-min sync done: %d index bars, %d futures bars", n_idx, n_fut)
    finally:
        conn.close()

# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys
    from dotenv import load_dotenv

    load_dotenv()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    db_path = os.getenv("DB_PATH", "fund_data.db")
    mode = sys.argv[1] if len(sys.argv) > 1 else "daily"

    if mode == "5min":
        logger.info("Running 5-minute intraday sync...")
        connect_and_fetch_market_5min(db_path)
    else:
        logger.info("Running daily market sync...")
        connect_and_fetch_market(db_path)
