#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
原油价格数据抓取模块 — 独立于 get_market_data.py

覆盖五个品种：
  WTI     — NYMEX 西德克萨斯中质原油，单位 USD/桶，via akshare futures_foreign_hist("CL")
  BRENT   — ICE 布伦特原油，单位 USD/桶，via akshare futures_foreign_hist("OIL")
  SC      — 上海国际能源交易中心原油主力连续合约（SC0），单位 CNY/桶，via akshare futures_zh_daily_sina("SC0")
  MURBAN  — 阿布扎比轻质原油（ICE IFAD），单位 USD/桶，via oilprice.com 爬取（备用：Google News EIA RSS）
  DME_OMAN— 迪拜商品交易所阿曼原油，单位 USD/桶，via oilprice.com 爬取（备用：Google News EIA RSS）

data_source 字段：
  "akshare"      — akshare 官方数据（WTI/BRENT/SC）
  "scrape"       — oilprice.com 爬取（MURBAN/DME_OMAN 主数据源）
  "news_estimate"— Google News 新闻标题中提取的价格估算（仅供参考，is_reference=1）

数据库表：crude_daily（写入 fund_data.db，与现有表共存）
"""

import logging
import os
import re
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta
from urllib.parse import quote_plus

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
        "fetch": "foreign",
        "ak_symbol": "CL",
        "currency": "USD",
        "description": "WTI原油 (NYMEX)",
        "exchange_tz": "纽约时间 UTC-5/-4",
    },
    "BRENT": {
        "fetch": "foreign",
        "ak_symbol": "OIL",
        "currency": "USD",
        "description": "布伦特原油 (ICE)",
        "exchange_tz": "伦敦时间 UTC+0",
    },
    "SC": {
        "fetch": "domestic",
        "ak_symbol": "SC0",
        "currency": "CNY",
        "description": "上海原油 (INE SC0主力合约)",
        "exchange_tz": "上海时间 UTC+8",
    },
    "MURBAN": {
        "fetch": "scrape",
        "oilprice_page_id": "4464",   # oilprice.com/oil-price-charts/4464
        "news_query": "Murban crude oil price",
        "currency": "USD",
        "description": "Murban原油 (ICE IFAD，阿布扎比)",
        "exchange_tz": "阿布扎比时间 UTC+4",
    },
    "DME_OMAN": {
        "fetch": "scrape",
        "oilprice_page_id": "48",     # oilprice.com/oil-price-charts/48
        "news_query": "DME Oman crude oil price",
        "currency": "USD",
        "description": "DME阿曼原油 (Dubai Mercantile Exchange)",
        "exchange_tz": "迪拜时间 UTC+4",
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
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            ts_code      TEXT    NOT NULL,
            trade_date   TEXT    NOT NULL,
            open         REAL,
            high         REAL,
            low          REAL,
            close        REAL    NOT NULL,
            volume       REAL,
            currency     TEXT    NOT NULL DEFAULT 'USD',
            data_source  TEXT    NOT NULL DEFAULT 'akshare',
            is_reference INTEGER NOT NULL DEFAULT 0,
            UNIQUE(ts_code, trade_date)
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_crude_daily_code_date
        ON crude_daily(ts_code, trade_date)
    """)
    # 向后兼容：旧库中缺失的列
    for col_def in [
        "ALTER TABLE crude_daily ADD COLUMN data_source TEXT NOT NULL DEFAULT 'akshare'",
        "ALTER TABLE crude_daily ADD COLUMN is_reference INTEGER NOT NULL DEFAULT 0",
    ]:
        try:
            conn.execute(col_def)
        except Exception:
            pass
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
# MURBAN / DME_OMAN — oilprice.com 爬取（主数据源）
# ---------------------------------------------------------------------------

def _fetch_oilprice_scrape(ts_code: str, page_id: str) -> tuple[str | None, float | None]:
    """
    从 oilprice.com/oil-price-charts/{page_id} 爬取当日价格。
    返回 (trade_date: YYYYMMDD, close: float) 或 (None, None) 表示失败。
    失败时静默，不抛异常。
    """
    try:
        import requests
        from bs4 import BeautifulSoup
    except ImportError:
        logger.warning("[scrape] requests 或 beautifulsoup4 未安装，跳过 oilprice.com 爬取")
        return None, None

    url = f"https://oilprice.com/oil-price-charts/{page_id}"
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9",
    }
    try:
        resp = requests.get(url, headers=headers, timeout=15)
        resp.raise_for_status()
    except Exception as e:
        logger.warning("[%s] oilprice.com 请求失败: %s", ts_code, e)
        return None, None

    try:
        soup = BeautifulSoup(resp.text, "html.parser")
        price_val = None

        # oilprice.com 价格在 <td class="last_price" data-price="XX.XX"> 中
        el = soup.select_one(f'tr[data-id="{page_id}"] td.last_price')
        if el:
            # 优先取 data-price 属性，更精确
            raw = el.get("data-price") or el.get_text(strip=True)
            raw = raw.replace(",", "")
            m = re.search(r"(\d+\.?\d*)", raw)
            if m:
                price_val = float(m.group(1))

        if price_val is None:
            logger.warning("[%s] oilprice.com 未找到价格元素，页面结构可能已变化", ts_code)
            return None, None

        trade_date = datetime.now().strftime("%Y%m%d")
        logger.info("[%s] oilprice.com 爬取成功: %s = %.2f", ts_code, trade_date, price_val)
        return trade_date, price_val

    except Exception as e:
        logger.warning("[%s] oilprice.com 解析失败: %s", ts_code, e)
        return None, None


# ---------------------------------------------------------------------------
# MURBAN / DME_OMAN — Google News EIA RSS 备用估算
# ---------------------------------------------------------------------------

def _fetch_news_price_estimate(ts_code: str, query: str) -> tuple[str | None, float | None]:
    """
    从 Google News RSS 搜索标题中用正则提取价格数字作为估算值。
    返回 (trade_date: YYYYMMDD, close: float) 或 (None, None)。
    标注 is_reference=1，仅供参考。
    """
    try:
        import feedparser
    except ImportError:
        logger.warning("[news_estimate] feedparser 未安装，跳过新闻价格估算")
        return None, None

    encoded = quote_plus(f"({query}) when:2d")
    rss_url = f"https://news.google.com/rss/search?q={encoded}&hl=en-US&gl=US&ceid=US:en"

    try:
        parsed = feedparser.parse(rss_url, request_headers={"User-Agent": "Mozilla/5.0"})
    except Exception as e:
        logger.warning("[%s] Google News RSS 请求失败: %s", ts_code, e)
        return None, None

    entries = parsed.get("entries", [])
    # 在标题中搜索 "$XX.XX" 或 "XX.XX USD" 格式的价格
    price_pattern = re.compile(r"\$\s*(\d{2,3}(?:\.\d{1,2})?)")
    for entry in entries[:10]:
        title = entry.get("title", "")
        m = price_pattern.search(title)
        if m:
            price_val = float(m.group(1))
            # 合理性检查：原油价格通常在 $20-$200 之间
            if 20 <= price_val <= 200:
                trade_date = datetime.now().strftime("%Y%m%d")
                logger.info("[%s] 新闻估算价格: %s = %.2f（仅供参考）来源: %s",
                            ts_code, trade_date, price_val, title[:60])
                return trade_date, price_val

    logger.info("[%s] 新闻标题中未找到有效价格", ts_code)
    return None, None


# ---------------------------------------------------------------------------
# MURBAN / DME_OMAN 主同步函数
# ---------------------------------------------------------------------------

def _sync_scrape(conn: sqlite3.Connection, ts_code: str, cfg: dict) -> int:
    """
    先尝试 oilprice.com 爬取，失败则降级到 Google News 估算。
    返回新增行数（0 或 1）。
    """
    currency = cfg["currency"]
    page_id = cfg.get("oilprice_page_id")
    news_query = cfg.get("news_query", ts_code)

    trade_date, close_val = None, None
    data_source = "scrape"
    is_reference = 0

    # 主数据源：oilprice.com
    if page_id:
        trade_date, close_val = _fetch_oilprice_scrape(ts_code, page_id)

    # 备用：Google News 估算
    if close_val is None:
        logger.info("[%s] oilprice.com 失败，降级到新闻估算", ts_code)
        trade_date, close_val = _fetch_news_price_estimate(ts_code, news_query)
        data_source = "news_estimate"
        is_reference = 1

    if close_val is None:
        logger.warning("[%s] 所有数据源均失败，跳过", ts_code)
        return 0

    try:
        conn.execute(
            """
            INSERT OR IGNORE INTO crude_daily
                (ts_code, trade_date, close, currency, data_source, is_reference)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (ts_code, trade_date, close_val, currency, data_source, is_reference),
        )
        conn.commit()
        added = conn.execute("SELECT changes()").fetchone()[0]
        if added:
            logger.info("[%s] 写入 %s = %.2f (%s)", ts_code, trade_date, close_val, data_source)
        return added
    except Exception as e:
        logger.warning("[%s] 写入失败: %s", ts_code, e)
        return 0


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
                elif cfg["fetch"] == "domestic":
                    added = _sync_sc(conn)
                elif cfg["fetch"] == "scrape":
                    added = _sync_scrape(conn, ts_code, cfg)
                else:
                    added = 0
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
