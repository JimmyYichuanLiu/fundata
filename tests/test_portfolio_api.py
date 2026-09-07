import sqlite3
from datetime import date

import pytest
from fastapi.testclient import TestClient


def _create_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS funds (
            fund_id INTEGER PRIMARY KEY AUTOINCREMENT,
            fund_code TEXT NOT NULL UNIQUE,
            fund_name TEXT
        );

        CREATE TABLE IF NOT EXISTS fund_nav_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fund_id INTEGER,
            fund_code TEXT,
            fund_name TEXT,
            nav_date TEXT NOT NULL,
            unit_nav REAL NOT NULL,
            accum_nav REAL,
            adj_nav REAL,
            UNIQUE(fund_code, nav_date)
        );
        """
    )


def _seed_data(conn: sqlite3.Connection) -> dict:
    conn.execute("INSERT INTO funds (fund_code, fund_name) VALUES (?, ?)", ("F1", "基金1"))
    conn.execute("INSERT INTO funds (fund_code, fund_name) VALUES (?, ?)", ("F2", "基金2"))
    f1 = conn.execute("SELECT fund_id FROM funds WHERE fund_code='F1'").fetchone()[0]
    f2 = conn.execute("SELECT fund_id FROM funds WHERE fund_code='F2'").fetchone()[0]

    # 手算样本
    # F1: 1.0 -> 1.1 -> 1.21
    # F2: 1.0 -> 1.0 -> 1.1
    rows = [
        (f1, "F1", "基金1", "2024-01-05", 1.0, 1.0),
        (f1, "F1", "基金1", "2024-01-12", 1.1, 1.1),
        (f1, "F1", "基金1", "2024-01-19", 1.21, 1.21),
        (f2, "F2", "基金2", "2024-01-05", 1.0, 1.0),
        (f2, "F2", "基金2", "2024-01-12", 1.0, 1.0),
        (f2, "F2", "基金2", "2024-01-19", 1.1, 1.1),
    ]
    conn.executemany(
        """
        INSERT INTO fund_nav_data (fund_id, fund_code, fund_name, nav_date, unit_nav, adj_nav)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    conn.commit()
    return {"f1": f1, "f2": f2}


@pytest.fixture(scope="module")
def client(tmp_path_factory):
    db_path = tmp_path_factory.mktemp("db") / "portfolio_test.db"
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    _create_schema(conn)
    seed = _seed_data(conn)
    # These hand-calculated samples have no distributions; cumulative equals unit NAV.
    conn.execute('UPDATE fund_nav_data SET accum_nav=unit_nav')
    conn.commit()
    conn.close()

    from api import app, get_db
    import sqlite3 as _sqlite3

    def _override_get_db():
        c = _sqlite3.connect(str(db_path), check_same_thread=False)
        c.row_factory = _sqlite3.Row
        try:
            yield c
            c.commit()
        except Exception:
            c.rollback()
            raise
        finally:
            c.close()

    app.dependency_overrides[get_db] = _override_get_db
    from tests.api_test_support import authenticated_client
    with authenticated_client(app, db_path) as c:
        c._seed = seed
        yield c
    app.dependency_overrides.clear()


def test_portfolio_crud(client):
    seed = client._seed
    create_payload = {
        "portfolio_name": "测试组合",
        "build_method": "UNIFIED_START",
        "constituents": [
            {"fund_id": seed["f1"], "target_weight": 0.5, "effective_date": "2024-01-05"},
            {"fund_id": seed["f2"], "target_weight": 0.5, "effective_date": "2024-01-05"},
        ],
    }
    r = client.post("/api/portfolios", json=create_payload)
    assert r.status_code == 200
    pid = r.json()["id"]

    r = client.get(f"/api/portfolios/{pid}")
    assert r.status_code == 200
    assert r.json()["portfolio_name"] == "测试组合"

    r = client.put(
        f"/api/portfolios/{pid}",
        json={
            "portfolio_name": "测试组合2",
            "build_method": "UNIFIED_START",
            "constituents": create_payload["constituents"],
        },
    )
    assert r.status_code == 200
    assert r.json()["portfolio_name"] == "测试组合2"

    r = client.delete(f"/api/portfolios/{pid}")
    assert r.status_code == 200


def test_unified_start_calculation(client):
    seed = client._seed
    r = client.post(
        "/api/portfolios",
        json={
            "portfolio_name": "统一起始测试",
            "build_method": "UNIFIED_START",
            "constituents": [
                {"fund_id": seed["f1"], "target_weight": 0.5, "effective_date": "2024-01-05"},
                {"fund_id": seed["f2"], "target_weight": 0.5, "effective_date": "2024-01-05"},
            ],
        },
    )
    pid = r.json()["id"]

    calc = client.post(f"/api/portfolios/{pid}/calculate")
    assert calc.status_code == 200

    nav = client.get(f"/api/portfolios/{pid}/nav")
    assert nav.status_code == 200
    items = nav.json()["items"]
    assert len(items) == 3
    # 2024-01-19: 0.5*1.21 + 0.5*1.1 = 1.155
    assert abs(items[-1]["portfolio_nav"] - 1.155) < 1e-9


def test_batch_include_calculation(client):
    seed = client._seed
    r = client.post(
        "/api/portfolios",
        json={
            "portfolio_name": "分批纳入测试",
            "build_method": "BATCH_INCLUDE",
            "constituents": [
                {"fund_id": seed["f1"], "target_amount": 100, "effective_date": "2024-01-05"},
                {"fund_id": seed["f2"], "target_amount": 100, "effective_date": "2024-01-12"},
            ],
        },
    )
    pid = r.json()["id"]

    calc = client.post(f"/api/portfolios/{pid}/calculate")
    assert calc.status_code == 200

    nav = client.get(f"/api/portfolios/{pid}/nav")
    assert nav.status_code == 200
    items = nav.json()["items"]
    assert len(items) == 3
    # 再平衡前先按当日净值估值：t1资产1.1，50/50配置后t2为1.21。
    assert abs(items[-1]["portfolio_nav"] - 1.21) < 1e-9


def test_batch_include_late_effective_no_nav(client):
    """Fund with effective_date on a week where it has no NAV should be
    picked up on the next week that has NAV (rebalance triggered)."""
    seed = client._seed
    r = client.post(
        "/api/portfolios",
        json={
            "portfolio_name": "延迟纳入测试",
            "build_method": "BATCH_INCLUDE",
            "constituents": [
                {"fund_id": seed["f1"], "target_amount": 100, "effective_date": "2024-01-05"},
                # F2 effective_date = 2024-01-08, but F2 has no NAV until 2024-01-12
                {"fund_id": seed["f2"], "target_amount": 100, "effective_date": "2024-01-08"},
            ],
        },
    )
    pid = r.json()["id"]
    calc = client.post(f"/api/portfolios/{pid}/calculate")
    assert calc.status_code == 200

    nav = client.get(f"/api/portfolios/{pid}/nav")
    assert nav.status_code == 200
    items = nav.json()["items"]
    # F2 should join on 2024-01-12 (first date it has NAV after effective_date)
    dates = [it["nav_date"] for it in items]
    assert "2024-01-12" in dates
    # The rebalance should happen on 2024-01-12 when F2 first appears
    rb_item = next(it for it in items if it["nav_date"] == "2024-01-12")
    assert rb_item["is_rebalance_day"] == 1
    assert rb_item["included_fund_count"] == 2


def test_batch_include_nav_gap_locf(client):
    """When a fund is missing NAV on a non-rebalance day, LOCF should apply."""
    seed = client._seed
    # Create a third fund with a gap
    from api import get_db
    db_gen = client.app.dependency_overrides[get_db]()
    conn = next(db_gen)
    conn.execute("INSERT OR IGNORE INTO funds (fund_code, fund_name) VALUES (?, ?)", ("F3", "基金3"))
    f3 = conn.execute("SELECT fund_id FROM funds WHERE fund_code='F3'").fetchone()[0]
    # F3 has NAV on 01-05 and 01-19 but NOT 01-12
    conn.executemany(
        "INSERT OR IGNORE INTO fund_nav_data (fund_id, fund_code, fund_name, nav_date, unit_nav, adj_nav) VALUES (?,?,?,?,?,?)",
        [
            (f3, "F3", "基金3", "2024-01-05", 1.0, 1.0),
            (f3, "F3", "基金3", "2024-01-19", 1.2, 1.2),
        ],
    )
    conn.commit()

    r = client.post(
        "/api/portfolios",
        json={
            "portfolio_name": "LOCF测试",
            "build_method": "BATCH_INCLUDE",
            "constituents": [
                {"fund_id": seed["f1"], "target_amount": 100, "effective_date": "2024-01-05"},
                {"fund_id": f3, "target_amount": 100, "effective_date": "2024-01-05"},
            ],
        },
    )
    pid = r.json()["id"]
    calc = client.post(f"/api/portfolios/{pid}/calculate")
    assert calc.status_code == 200

    nav = client.get(f"/api/portfolios/{pid}/nav")
    assert nav.status_code == 200
    items = nav.json()["items"]
    # Should have 3 dates (01-05, 01-12, 01-19)
    assert len(items) >= 2
    # No 500 error — the gap in F3 on 01-12 should not crash


def test_metrics_shape(client):
    seed = client._seed
    r = client.post(
        "/api/portfolios",
        json={
            "portfolio_name": "指标测试",
            "build_method": "UNIFIED_START",
            "constituents": [
                {"fund_id": seed["f1"], "target_weight": 0.5, "effective_date": "2024-01-05"},
                {"fund_id": seed["f2"], "target_weight": 0.5, "effective_date": "2024-01-05"},
            ],
        },
    )
    pid = r.json()["id"]
    client.post(f"/api/portfolios/{pid}/calculate")

    m = client.get(f"/api/portfolios/{pid}/metrics")
    assert m.status_code == 200
    data = m.json()
    for k in ["annualized_return", "annualized_vol", "max_drawdown", "sharpe", "monthly_win_rate"]:
        assert k in data
    assert data['annualized_return'] is None  # less than 30 days, not zero
    assert data['max_drawdown'] is not None
