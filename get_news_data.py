#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
原油与中东冲突新闻 RSS 抓取模块

数据源（14个，免费、无需 token）：
  - USNI News          https://news.usni.org/feed               — 美海军/霍尔木兹专项
  - OilPrice.com       https://oilprice.com/rss/main            — 原油市场（全量收录）
  - Al Jazeera         https://www.aljazeera.com/xml/rss/all.xml — 中东冲突（关键词过滤）
  - IAEA               https://www.iaea.org/feeds/topnews       — 国际原子能机构（全量）
  - White House        Google News RSS site:whitehouse.gov       — 美国官方立场（关键词过滤）
  - State Dept         Google News RSS site:state.gov           — 国务院声明（关键词过滤）
  - Press TV           Google News RSS site:presstv.ir          — 伊朗国家电视台（全量）
  - IRNA               Google News RSS site:irna.ir             — 伊朗官方通讯社（全量）
  - Tasnim News        Google News RSS site:tasnimnews.com      — 伊朗半官方通讯社（全量）
  - 新华社             Google News 中文 RSS                     — 中国官方（中文直接存储）
  - 中国日报           Google News 中文 RSS                     — 中国官方（中文直接存储）
  - 环球时报           Google News 中文 RSS                     — 中国官方（中文直接存储）
  - CGTN               Google News 中文 RSS                     — 中国官方（中文直接存储）
  - The National       Google News RSS site:thenationalnews.com — 阿联酋视角（关键词过滤）
  - Reuters Energy     Google News RSS site:reuters.com         — 路透能源（关键词过滤）
  - Hormuz/Shipping    Google News RSS 关键词聚合               — 航运/油轮动态

数据库表：crude_news（写入 fund_data.db）

category 合法值：conflict / shipping / crude / official_west / official_iran / official_china
priority：1~10，数值越小越重要
  - source_weight：官方源 2，USNI/航运 3，中东视角 4，通用媒体 5
  - topic_score：命中关键词扣分（Hormuz/tanker -2，missile/sanctions -2，oil/OPEC -1）
title_zh：中文标题（None=未翻译，""=翻译失败，字符串=成功）
  - 中文源（is_chinese=True）：title 直接存为 title_zh，不调用翻译 API
  - 英文源：仅翻译 priority <= 4 的条目，限 200 字符，timeout=5s
"""

import json
import logging
import os
import sqlite3
from datetime import datetime, timezone
from typing import Optional, Tuple
from urllib.parse import quote_plus

import feedparser
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

DB_PATH: str = os.getenv("DB_PATH", "fund_data.db")

# 只翻译此优先级及以上（数值越小越重要）
TRANSLATE_PRIORITY_THRESHOLD = 4

# ---------------------------------------------------------------------------
# Google News RSS URL 构造
# ---------------------------------------------------------------------------

def _google_news_rss(query: str) -> str:
    """生成 Google News RSS URL（英文，美国区域）。"""
    encoded = quote_plus(query)
    return f"https://news.google.com/rss/search?q={encoded}&hl=en-US&gl=US&ceid=US:en"


# ---------------------------------------------------------------------------
# RSS 源配置
# ---------------------------------------------------------------------------
RSS_FEEDS = [
    # ---- 原有来源（全量收录，高信噪比） ----
    {
        "name": "USNI News",
        "url": "https://news.usni.org/feed",
        "default_category": "conflict",
        "keywords": None,           # 全量收录（美国海军/中东行动专项媒体）
        "base_priority": 3,
    },
    {
        "name": "OilPrice.com",
        "url": "https://oilprice.com/rss/main",
        "default_category": "crude",
        "keywords": None,           # 全量收录（专属原油能源媒体）
        "base_priority": 5,
    },
    {
        "name": "Al Jazeera",
        "url": "https://www.aljazeera.com/xml/rss/all.xml",
        "default_category": "conflict",
        "keywords": [               # 仅保留命中关键词的条目
            "Iran", "Israel", "Hormuz", "crude", "oil price",
            "Houthi", "Yemen", "IRGC", "Gaza", "Lebanon",
            "sanctions", "nuclear", "Brent", "WTI", "OPEC",
            "Red Sea", "tanker", "petroleum",
        ],
        "base_priority": 5,
    },

    # ---- 新增：欧美官方来源 ----
    {
        "name": "IAEA",
        "url": "https://www.iaea.org/feeds/topnews",
        "default_category": "official_west",
        "keywords": None,           # 发布频率低但信噪比极高，全量收录
        "base_priority": 2,
    },
    {
        "name": "White House",
        "url": _google_news_rss("site:whitehouse.gov Iran OR sanctions OR oil OR energy OR Gulf when:3d"),
        "default_category": "official_west",
        "keywords": [
            "Iran", "sanctions", "oil", "energy", "Gulf",
            "nuclear", "OPEC", "Middle East", "Hormuz",
        ],
        "base_priority": 2,
    },
    {
        "name": "State Dept",
        "url": _google_news_rss("site:state.gov Iran OR sanctions OR oil OR energy OR Gulf when:3d"),
        "default_category": "official_west",
        "keywords": [
            "Iran", "sanctions", "oil", "energy", "Gulf",
            "nuclear", "JCPOA", "Middle East", "Hormuz",
        ],
        "base_priority": 2,
    },

    # ---- 新增：伊朗官方来源（替换 Iran International） ----
    {
        "name": "Press TV",
        "url": _google_news_rss("site:presstv.ir when:2d"),
        "default_category": "official_iran",
        "keywords": None,           # 伊朗国家电视台英文频道，全量收录
        "base_priority": 2,
    },
    {
        "name": "IRNA",
        "url": _google_news_rss("site:irna.ir when:2d"),
        "default_category": "official_iran",
        "keywords": None,           # 伊朗官方通讯社，全量收录
        "base_priority": 2,
    },
    {
        "name": "Tasnim News",
        "url": _google_news_rss("site:tasnimnews.com when:2d"),
        "default_category": "official_iran",
        "keywords": None,           # 伊朗半官方通讯社，全量收录
        "base_priority": 3,
    },

    # ---- 新增：中国官方来源 ----
    {
        "name": "新华社",
        "url": "https://news.google.com/rss/search?q=site:xinhuanet.com+%28Iran+OR+oil+OR+Hormuz+OR+Middle+East%29+when:2d&hl=zh-CN&gl=CN&ceid=CN:zh-Hans",
        "default_category": "official_china",
        "keywords": None,
        "base_priority": 2,
        "is_chinese": True,
    },
    {
        "name": "中国日报",
        "url": "https://news.google.com/rss/search?q=site:chinadaily.com.cn+%28Iran+OR+oil+OR+Hormuz+OR+Middle+East%29+when:2d&hl=zh-CN&gl=CN&ceid=CN:zh-Hans",
        "default_category": "official_china",
        "keywords": None,
        "base_priority": 2,
        "is_chinese": True,
    },
    {
        "name": "环球时报",
        "url": "https://news.google.com/rss/search?q=site:huanqiu.com+%28伊朗+OR+石油+OR+霍尔木兹+OR+中东%29+when:2d&hl=zh-CN&gl=CN&ceid=CN:zh-Hans",
        "default_category": "official_china",
        "keywords": None,
        "base_priority": 3,
        "is_chinese": True,
    },
    {
        "name": "CGTN",
        "url": "https://news.google.com/rss/search?q=site:cgtn.com+%28Iran+OR+oil+OR+Hormuz+OR+Middle+East%29+when:2d&hl=zh-CN&gl=CN&ceid=CN:zh-Hans",
        "default_category": "official_china",
        "keywords": None,
        "base_priority": 2,
        "is_chinese": True,
    },

    # ---- 新增：中东地区视角 ----
    {
        "name": "The National",
        "url": _google_news_rss(
            "site:thenationalnews.com Iran OR Israel OR Houthi OR Hormuz OR oil when:2d"
        ),
        "default_category": "conflict",
        "keywords": [
            "Iran", "Israel", "Houthi", "Hormuz", "oil",
            "IRGC", "Yemen", "Red Sea", "tanker", "sanctions",
        ],
        "base_priority": 4,
    },

    # ---- 新增：原油能源专项 ----
    {
        "name": "Reuters Energy",
        "url": _google_news_rss(
            "site:reuters.com (oil OR crude OR Brent OR WTI OR OPEC OR energy) when:3d"
        ),
        "default_category": "crude",
        "keywords": [
            "oil", "crude", "Brent", "WTI", "OPEC",
            "energy", "barrel", "petroleum", "LNG", "refinery",
        ],
        "base_priority": 4,
    },

    # ---- 新增：航运/霍尔木兹专项聚合 ----
    {
        "name": "Shipping & Hormuz",
        "url": _google_news_rss(
            'Hormuz OR tanker OR "oil shipment" OR "Red Sea shipping" OR "Strait of Hormuz" when:3d'
        ),
        "default_category": "shipping",
        "keywords": [
            "Hormuz", "tanker", "shipping", "shipment",
            "Red Sea", "Suez", "strait", "vessel", "oil cargo",
        ],
        "base_priority": 3,
    },
]

# ---------------------------------------------------------------------------
# priority 计算：分两层
# ---------------------------------------------------------------------------

# 话题得分规则：命中任意词累加 delta（负数 = 提升优先级）
_PRIORITY_RULES = [
    (["Hormuz", "tanker", "shipping", "strait", "Red Sea"], -2),
    (["missile", "strike", "airstrike", "drone strike", "attack", "sanctions", "nuclear"], -2),
    (["oil", "Brent", "WTI", "OPEC", "crude", "barrel"], -1),
]


def _source_weight(feed_cfg: dict) -> int:
    """按来源返回基础权重（数值越小越重要）。"""
    return feed_cfg.get("base_priority", 5)


def _topic_score(title: str, summary: str) -> int:
    """按标题/摘要关键词计算话题调整量（负数 = 提升优先级）。"""
    text = (title + " " + summary).lower()
    delta = 0
    for keywords, rule_delta in _PRIORITY_RULES:
        if any(kw.lower() in text for kw in keywords):
            delta += rule_delta
    return delta


def _calc_priority(feed_cfg: dict, title: str, summary: str) -> int:
    """合成最终 priority，clamp 到 [1, 10]。"""
    return max(1, min(10, _source_weight(feed_cfg) + _topic_score(title, summary)))


# ---------------------------------------------------------------------------
# 翻译
# ---------------------------------------------------------------------------

def _translate_to_zh(text: str) -> Optional[str]:
    """
    将英文标题翻译为中文。
    - 仅截取前 200 字符，避免超长标题导致失败
    - timeout=5s，避免阻塞整个同步流程
    - 返回 None 表示未安装依赖；返回 "" 表示翻译失败；返回字符串表示成功
    """
    try:
        from deep_translator import GoogleTranslator
    except ImportError:
        return None  # 依赖未安装，静默跳过

    text = text[:200].strip()
    if not text:
        return None

    try:
        result = GoogleTranslator(source="auto", target="zh-CN").translate(text)
        return result if result else ""
    except Exception as e:
        logger.debug("翻译失败: %s — %s", text[:40], e)
        return ""  # 区分：None=未尝试，""=尝试过但失败


# ---------------------------------------------------------------------------
# 数据库初始化
# ---------------------------------------------------------------------------

def init_news_db(conn: sqlite3.Connection):
    """创建 crude_news 表（若不存在），并向后兼容地添加新列。"""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS crude_news (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            title        TEXT    NOT NULL,
            url          TEXT    UNIQUE,
            source_name  TEXT    NOT NULL,
            published_at TEXT,
            summary      TEXT,
            category     TEXT    DEFAULT 'crude',
            priority     INTEGER DEFAULT 5,
            title_zh     TEXT    DEFAULT NULL,
            fetched_at   TEXT    NOT NULL
        )
    """)
    conn.commit()

    # 向后兼容：旧库中缺失的列，用 ALTER TABLE 添加（已存在时忽略）
    for col_def in [
        "ALTER TABLE crude_news ADD COLUMN priority INTEGER DEFAULT 5",
        "ALTER TABLE crude_news ADD COLUMN title_zh TEXT DEFAULT NULL",
    ]:
        try:
            conn.execute(col_def)
            conn.commit()
        except Exception:
            pass  # 列已存在，忽略

    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_crude_news_published
        ON crude_news(published_at DESC)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_crude_news_priority
        ON crude_news(priority ASC, published_at DESC)
    """)
    conn.commit()
    logger.info("crude_news 表初始化完毕")


# ---------------------------------------------------------------------------
# 工具：解析 feedparser 时间为 ISO 8601 字符串
# ---------------------------------------------------------------------------

def _parse_time(entry) -> str:
    """从 feedparser entry 提取发布时间，返回 ISO 8601 字符串。"""
    t = getattr(entry, "published_parsed", None) or getattr(entry, "updated_parsed", None)
    if t:
        try:
            return datetime(*t[:6], tzinfo=timezone.utc).isoformat()
        except Exception:
            pass
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# 关键词过滤
# ---------------------------------------------------------------------------

def _matches_keywords(title: str, summary: str, keywords: list) -> bool:
    text = (title + " " + summary).lower()
    return any(kw.lower() in text for kw in keywords)


# ---------------------------------------------------------------------------
# 单源抓取
# ---------------------------------------------------------------------------

def _fetch_feed(conn: sqlite3.Connection, feed_cfg: dict) -> Tuple[int, str]:
    """
    抓取单个 RSS 源。
    返回 (added: int, error: str)，失败时 added=0，error 为错误信息。
    """
    name = feed_cfg["name"]
    url = feed_cfg["url"]
    keywords = feed_cfg.get("keywords")
    category = feed_cfg["default_category"]
    is_chinese = feed_cfg.get("is_chinese", False)  # 中文源：title 直接作为 title_zh

    logger.info("[%s] 开始抓取: %s", name, url[:80])
    try:
        parsed = feedparser.parse(
            url,
            request_headers={"User-Agent": "Mozilla/5.0"},
        )
    except Exception as e:
        err = str(e)
        logger.error("[%s] feedparser 失败: %s", name, err)
        return 0, err

    entries = parsed.get("entries", [])
    if not entries:
        logger.warning("[%s] 返回 0 条，可能被封锁或 RSS 格式变化", name)
        return 0, ""

    fetched_at = datetime.now(timezone.utc).isoformat()
    added = 0

    for entry in entries:
        title = entry.get("title", "").strip()
        link = entry.get("link", "").strip()
        summary = entry.get("summary", "").strip()[:500]

        if not title or not link:
            continue

        # 关键词过滤（仅对 keywords 非 None 的源生效）
        if keywords and not _matches_keywords(title, summary, keywords):
            continue

        pub_at = _parse_time(entry)
        priority = _calc_priority(feed_cfg, title, summary)

        try:
            conn.execute(
                """
                INSERT OR IGNORE INTO crude_news
                    (title, url, source_name, published_at, summary, category, priority, fetched_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (title, link, name, pub_at, summary, category, priority, fetched_at),
            )
            if conn.execute("SELECT changes()").fetchone()[0] > 0:
                added += 1
                # 中文源：title 本身就是中文，直接存为 title_zh
                if is_chinese:
                    conn.execute(
                        "UPDATE crude_news SET title_zh = ? WHERE url = ?",
                        (title, link),
                    )
                # 英文源：仅翻译高优先级新闻（priority <= 阈值），控制 HTTP 请求量
                elif priority <= TRANSLATE_PRIORITY_THRESHOLD:
                    title_zh = _translate_to_zh(title)
                    if title_zh is not None:  # None 表示依赖未安装，不写入
                        conn.execute(
                            "UPDATE crude_news SET title_zh = ? WHERE url = ?",
                            (title_zh, link),
                        )
        except Exception as e:
            logger.warning("[%s] 插入失败: %s — %s", name, title[:40], e)

    conn.commit()
    logger.info("[%s] 新增 %d 条", name, added)
    return added, ""


# ---------------------------------------------------------------------------
# 主入口：供 api.py 调度器调用
# ---------------------------------------------------------------------------

def connect_and_fetch_news(db_path: str = DB_PATH) -> dict:
    """
    抓取全部 RSS 源，返回结果字典：
    {
      "total": 292,
      "per_source": {
        "USNI News":    {"added": 0,  "status": "ok",    "error": "", "time": "..."},
        "OilPrice.com": {"added": 5,  "status": "ok",    "error": "", "time": "..."},
        "IAEA":         {"added": 15, "status": "error", "error": "timeout", "time": "..."},
      }
    }
    """
    conn = sqlite3.connect(db_path, check_same_thread=False)
    try:
        init_news_db(conn)
        total = 0
        per_source = {}
        for cfg in RSS_FEEDS:
            fetch_time = datetime.now(timezone.utc).isoformat()
            added, error = _fetch_feed(conn, cfg)
            total += added
            per_source[cfg["name"]] = {
                "added":  added,
                "status": "error" if error else "ok",
                "error":  error,
                "time":   fetch_time,
            }
        logger.info("新闻抓取完毕，共新增 %d 条", total)
        return {"total": total, "per_source": per_source}
    except Exception as e:
        logger.error("新闻抓取整体失败: %s", e, exc_info=True)
        raise
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 直接运行入口
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("=== 原油新闻 RSS 抓取（直接运行）===")
    result = connect_and_fetch_news()
    print(f"完成，新增 {result['total']} 条")

    print("\n--- 各源状态 ---")
    for src, info in result["per_source"].items():
        status = info["status"]
        added = info["added"]
        err = f" [{info['error']}]" if info["error"] else ""
        print(f"  {src}: {status}, +{added}{err}")

    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute(
        "SELECT priority, source_name, category, title_zh, title "
        "FROM crude_news ORDER BY priority ASC, published_at DESC LIMIT 15"
    ).fetchall()
    print("\n--- 最高优先级 15 条 ---")
    for r in rows:
        display = r[3] if r[3] else r[4]
        print(f"  [{r[0]}] {r[1]} ({r[2]}): {display[:60]}")

    print("\n--- 各分类统计 ---")
    cats = conn.execute(
        "SELECT category, COUNT(*) FROM crude_news GROUP BY category"
    ).fetchall()
    for c in cats:
        print(f"  {c[0]}: {c[1]}")
    conn.close()
