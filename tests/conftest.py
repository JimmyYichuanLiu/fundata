"""
测试夹具（conftest.py）

核心思路：
  - 用内存 SQLite（":memory:"）完全替换真实 DB，测试间零污染
  - 覆盖 api.py 的 get_db Depends，注入测试用连接
  - 通过 FastAPI TestClient 发 HTTP 请求，无需启动真实服务器
"""

import sqlite3
import pytest
from fastapi.testclient import TestClient

# 导入 app 和需要被替换的依赖
import api as api_module
from api import app, get_db


# ---------------------------------------------------------------------------
# 数据库 Schema（与 api._init_db_schema 保持同步）
# ---------------------------------------------------------------------------

def _create_schema(conn: sqlite3.Connection) -> None:
    """在给定连接上建立测试用 schema。"""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS funds (
            fund_id      INTEGER PRIMARY KEY AUTOINCREMENT,
            产品代码     TEXT NOT NULL UNIQUE,
            产品名称     TEXT,
            benchmark_index TEXT DEFAULT NULL,
            strategy1    TEXT DEFAULT NULL,
            strategy2    TEXT DEFAULT NULL,
            strategy3    TEXT DEFAULT NULL,
            is_show      TEXT DEFAULT NULL,
            setup_date   TEXT DEFAULT NULL,
            start_date   TEXT DEFAULT NULL,
            首次录入时间 DATETIME DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS fund_nav_data (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            fund_id          INTEGER REFERENCES funds(fund_id),
            产品名称         TEXT,
            产品代码         TEXT,
            净值日期         TEXT,
            单位净值         REAL,
            累计单位净值     REAL,
            adjusted_nav     REAL,
            插入时间         DATETIME DEFAULT CURRENT_TIMESTAMP,
            source_id        INTEGER,
            UNIQUE(产品代码, 净值日期)
        );

        CREATE TABLE IF NOT EXISTS extraction_failures (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            失败时间     DATETIME DEFAULT CURRENT_TIMESTAMP,
            邮件主题     TEXT,
            邮件发件人   TEXT,
            邮件日期     TEXT,
            附件文件名   TEXT,
            sheet名称    TEXT,
            失败原因     TEXT
        );

        CREATE TABLE IF NOT EXISTS sync_state (
            key   TEXT PRIMARY KEY,
            value TEXT
        );

        CREATE TABLE IF NOT EXISTS excel_conflicts (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            产品代码    TEXT NOT NULL,
            净值日期    TEXT NOT NULL,
            email_unit_nav  REAL,
            excel_unit_nav  REAL,
            detected_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(产品代码, 净值日期)
        );
    """)


def _seed_fund(conn: sqlite3.Connection, code: str, name: str, nav: float = 1.0) -> int:
    """插入一条基金 + 一条净值记录，返回 fund_id。"""
    conn.execute(
        "INSERT OR IGNORE INTO funds (产品代码, 产品名称) VALUES (?, ?)",
        (code, name),
    )
    fund_id = conn.execute(
        "SELECT fund_id FROM funds WHERE 产品代码 = ?", (code,)
    ).fetchone()[0]
    conn.execute(
        """INSERT OR IGNORE INTO fund_nav_data
           (fund_id, 产品代码, 产品名称, 净值日期, 单位净值, 累计单位净值)
           VALUES (?, ?, ?, '20240101', ?, ?)""",
        (fund_id, code, name, nav, nav),
    )
    conn.commit()
    return fund_id


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def mem_db() -> sqlite3.Connection:
    """返回一个带完整 schema 的内存 SQLite 连接（每个测试独立）。"""
    conn = sqlite3.connect(":memory:", check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    _create_schema(conn)
    yield conn
    conn.close()


@pytest.fixture()
def mem_db_with_data(mem_db: sqlite3.Connection) -> sqlite3.Connection:
    """在 mem_db 基础上预置两条基金数据。"""
    _seed_fund(mem_db, "F001", "测试基金A", nav=1.25)
    _seed_fund(mem_db, "F002", "测试基金B", nav=2.00)
    return mem_db


@pytest.fixture()
def client(mem_db: sqlite3.Connection) -> TestClient:
    """返回注入了内存 DB 的 FastAPI TestClient（空库）。"""
    def override_get_db():
        yield mem_db

    app.dependency_overrides[get_db] = override_get_db
    with TestClient(app, raise_server_exceptions=True) as c:
        yield c
    app.dependency_overrides.clear()


@pytest.fixture()
def client_with_data(mem_db_with_data: sqlite3.Connection) -> TestClient:
    """返回注入了内存 DB 的 FastAPI TestClient（含预置数据）。"""
    def override_get_db():
        yield mem_db_with_data

    app.dependency_overrides[get_db] = override_get_db
    with TestClient(app, raise_server_exceptions=True) as c:
        yield c
    app.dependency_overrides.clear()
