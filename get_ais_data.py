#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
霍尔木兹海峡 AIS 船舶数据采集模块

数据源：AISStream.io（免费 WebSocket API，无需 AIS 接收站）
  WebSocket: wss://stream.aisstream.io/v0/stream
  注册地址: https://aisstream.io

配置：
  AISSTREAM_API_KEY — 在 .env 中设置，未设置时跳过采集

霍尔木兹海峡地理围栏：
  SW: (25.5°N, 56.0°E)  NE: (26.8°N, 57.5°E)

AIS 油轮类型代码（TYPE 80-89）：
  80 Tanker
  81 Tanker, Hazardous category X
  82 Tanker, Hazardous category Y
  83 Tanker, Hazardous category Z
  84 Tanker, Hazardous category OS
  89 Tanker, No additional information

数据库表：
  hormuz_vessels  — 最新一次采集的区域内船只列表
  hormuz_snapshot — 每次采集的聚合快照（用于24小时趋势图）
"""

import json
import logging
import os
import sqlite3
from datetime import datetime, timezone

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

DB_PATH: str = os.getenv("DB_PATH", "fund_data.db")
AISSTREAM_API_KEY: str = os.getenv("AISSTREAM_API_KEY", "")

# 霍尔木兹海峡地理围栏 [[lat_min, lon_min], [lat_max, lon_max]]
HORMUZ_BBOX = [[25.5, 56.0], [26.8, 57.5]]

# AIS 油轮类型代码范围
TANKER_TYPE_MIN = 80
TANKER_TYPE_MAX = 89

# 单次采集窗口（秒）：连接后收集这么长时间的数据再断开
COLLECT_SECONDS = 120


# ---------------------------------------------------------------------------
# 数据库初始化
# ---------------------------------------------------------------------------

def init_ais_db(conn: sqlite3.Connection):
    """创建 hormuz_vessels 和 hormuz_snapshot 表。"""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS hormuz_vessels (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            mmsi        TEXT    NOT NULL,
            ship_name   TEXT,
            vessel_type INTEGER,
            lat         REAL,
            lon         REAL,
            speed       REAL,
            course      REAL,
            nav_status  INTEGER,
            fetched_at  TEXT    NOT NULL
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_hormuz_vessels_fetched
        ON hormuz_vessels(fetched_at DESC)
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS hormuz_snapshot (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            snapshot_at   TEXT    NOT NULL UNIQUE,
            vessel_count  INTEGER DEFAULT 0,
            tanker_count  INTEGER DEFAULT 0,
            data_quality  TEXT    DEFAULT 'partial'
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_hormuz_snapshot_at
        ON hormuz_snapshot(snapshot_at DESC)
    """)
    conn.commit()
    logger.info("AIS 数据库表初始化完毕")


# ---------------------------------------------------------------------------
# 主采集函数
# ---------------------------------------------------------------------------

def collect_hormuz_ais(db_path: str = DB_PATH) -> dict:
    """
    连接 AISStream.io WebSocket，采集霍尔木兹海峡区域船舶数据。
    采集 COLLECT_SECONDS 秒后断开，将结果写入数据库。

    返回：
    {
      "vessel_count": 12,
      "tanker_count": 8,
      "data_quality": "partial",
      "error": ""
    }
    """
    if not AISSTREAM_API_KEY:
        logger.warning("AISSTREAM_API_KEY 未设置，跳过 AIS 采集")
        return {"vessel_count": 0, "tanker_count": 0, "data_quality": "unavailable",
                "error": "AISSTREAM_API_KEY not configured"}

    try:
        import websocket
        import threading
        import time
    except ImportError:
        logger.warning("websocket-client 未安装，请执行: pip install websocket-client")
        return {"vessel_count": 0, "tanker_count": 0, "data_quality": "unavailable",
                "error": "websocket-client not installed"}

    vessels = {}   # mmsi -> vessel_data
    errors = []
    done_event = threading.Event()

    def on_open(ws):
        sub_msg = json.dumps({
            "APIKey": AISSTREAM_API_KEY,
            "BoundingBoxes": [HORMUZ_BBOX],
            "FilterMessageTypes": ["PositionReport", "ShipStaticData"],
        })
        ws.send(sub_msg)
        logger.info("AISStream 已连接，开始采集 %d 秒", COLLECT_SECONDS)

        def _stop():
            time.sleep(COLLECT_SECONDS)
            ws.close()
            done_event.set()

        threading.Thread(target=_stop, daemon=True).start()

    def on_message(ws, message):
        try:
            data = json.loads(message)
            msg_type = data.get("MessageType", "")
            meta = data.get("MetaData", {})
            mmsi = str(meta.get("MMSI", ""))
            if not mmsi:
                return

            if msg_type == "PositionReport":
                msg = data.get("Message", {}).get("PositionReport", {})
                vessels.setdefault(mmsi, {"mmsi": mmsi})
                vessels[mmsi].update({
                    "lat":        msg.get("Latitude"),
                    "lon":        msg.get("Longitude"),
                    "speed":      msg.get("Sog"),       # Speed Over Ground
                    "course":     msg.get("Cog"),       # Course Over Ground
                    "nav_status": msg.get("NavigationalStatus"),
                    "ship_name":  meta.get("ShipName", "").strip() or vessels[mmsi].get("ship_name"),
                })

            elif msg_type == "ShipStaticData":
                msg = data.get("Message", {}).get("ShipStaticData", {})
                vessels.setdefault(mmsi, {"mmsi": mmsi})
                vessels[mmsi].update({
                    "vessel_type": msg.get("Type"),
                    "ship_name":   msg.get("Name", "").strip() or vessels[mmsi].get("ship_name"),
                })
        except Exception as e:
            logger.debug("AIS 消息解析错误: %s", e)

    def on_error(ws, error):
        errors.append(str(error))
        logger.error("AISStream 错误: %s", error)
        done_event.set()

    def on_close(ws, close_status_code, close_msg):
        logger.info("AISStream 连接关闭，共收集 %d 艘船", len(vessels))
        done_event.set()

    ws_app = websocket.WebSocketApp(
        "wss://stream.aisstream.io/v0/stream",
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )

    wst = threading.Thread(target=ws_app.run_forever, daemon=True)
    wst.start()
    done_event.wait(timeout=COLLECT_SECONDS + 30)

    # 写入数据库
    conn = sqlite3.connect(db_path, timeout=30)
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        init_ais_db(conn)

        fetched_at = datetime.now(timezone.utc).isoformat()

        # 清空旧的 vessels 记录（只保留最新一次）
        conn.execute("DELETE FROM hormuz_vessels")

        vessel_list = list(vessels.values())
        tanker_list = [
            v for v in vessel_list
            if v.get("vessel_type") is not None
            and TANKER_TYPE_MIN <= v["vessel_type"] <= TANKER_TYPE_MAX
        ]

        for v in vessel_list:
            conn.execute(
                """
                INSERT INTO hormuz_vessels
                    (mmsi, ship_name, vessel_type, lat, lon, speed, course, nav_status, fetched_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    v.get("mmsi"), v.get("ship_name"), v.get("vessel_type"),
                    v.get("lat"), v.get("lon"), v.get("speed"),
                    v.get("course"), v.get("nav_status"), fetched_at,
                ),
            )

        # 写快照（按小时聚合，同一小时内 upsert）
        snapshot_hour = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:00:00+00:00")
        data_quality = "full" if not errors else "partial"
        conn.execute(
            """
            INSERT OR REPLACE INTO hormuz_snapshot
                (snapshot_at, vessel_count, tanker_count, data_quality)
            VALUES (?, ?, ?, ?)
            """,
            (snapshot_hour, len(vessel_list), len(tanker_list), data_quality),
        )

        # 更新 sync_state
        conn.execute(
            "INSERT OR REPLACE INTO sync_state(key, value) VALUES(?, ?)",
            ("hormuz_last_status", "success" if not errors else "partial_error"),
        )
        conn.execute(
            "INSERT OR REPLACE INTO sync_state(key, value) VALUES(?, ?)",
            ("hormuz_last_time", fetched_at),
        )
        conn.execute(
            "INSERT OR REPLACE INTO sync_state(key, value) VALUES(?, ?)",
            ("hormuz_last_error", "; ".join(errors) if errors else ""),
        )
        conn.commit()

        result = {
            "vessel_count": len(vessel_list),
            "tanker_count": len(tanker_list),
            "data_quality": data_quality,
            "error": "; ".join(errors) if errors else "",
        }
        logger.info("AIS 采集完毕: %d 艘船，其中油轮 %d 艘", len(vessel_list), len(tanker_list))
        return result

    except Exception as e:
        logger.error("AIS 数据写入失败: %s", e, exc_info=True)
        return {"vessel_count": 0, "tanker_count": 0, "data_quality": "unavailable", "error": str(e)}
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 直接运行入口
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("=== 霍尔木兹海峡 AIS 采集（直接运行）===")
    if not AISSTREAM_API_KEY:
        print("错误：请在 .env 中设置 AISSTREAM_API_KEY")
    else:
        result = collect_hormuz_ais()
        print(f"完成：{result}")
