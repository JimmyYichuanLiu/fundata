"""
tests/test_api_fund_endpoints.py
=================================
API integration tests for fund-related endpoints.

Purpose: Verify that api.py correctly reads/writes from the new English-column
schema (fund_code, fund_name, nav_date, unit_nav, accum_nav, adj_nav …).
Every test uses an in-memory SQLite database seeded with the new schema —
the real fund_data.db is never touched.

Before the api.py column-name fix these tests should be RED.
After the fix they should all be GREEN.
"""

import re
import sqlite3
from datetime import date, timedelta

import pytest
from fastapi.testclient import TestClient

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _create_schema(conn: sqlite3.Connection) -> None:
    """Build the post-migration schema (English columns) in conn."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS funds (
            fund_id         INTEGER PRIMARY KEY AUTOINCREMENT,
            fund_code       TEXT NOT NULL UNIQUE,
            fund_name       TEXT,
            created_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
            benchmark_index TEXT DEFAULT NULL,
            strategy_l1     TEXT DEFAULT NULL,
            strategy_l2     TEXT DEFAULT NULL,
            strategy_l3     TEXT DEFAULT NULL,
            manager         TEXT DEFAULT NULL,
            custodian       TEXT DEFAULT NULL,
            inception_date  TEXT DEFAULT NULL,
            start_date      TEXT DEFAULT NULL,
            display         TEXT DEFAULT '展示'
        );

        CREATE TABLE IF NOT EXISTS fund_nav_data (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            fund_id     INTEGER,
            fund_code   TEXT,
            fund_name   TEXT,
            nav_date    TEXT NOT NULL,
            unit_nav    REAL NOT NULL,
            accum_nav   REAL,
            "录入时间"  DATETIME DEFAULT CURRENT_TIMESTAMP,
            source_id   INTEGER,
            adj_nav     REAL,
            data_source TEXT,
            UNIQUE(fund_code, nav_date),
            FOREIGN KEY (fund_id) REFERENCES funds(fund_id)
        );

        CREATE TABLE IF NOT EXISTS sync_state (
            key   TEXT PRIMARY KEY,
            value TEXT
        );

        CREATE TABLE IF NOT EXISTS extraction_failures (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            失败时间 DATETIME,
            邮件主题 TEXT,
            邮件发件人 TEXT,
            邮件日期 TEXT,
            附件文件名 TEXT,
            sheet名称 TEXT,
            失败原因 TEXT
        );

        CREATE TABLE IF NOT EXISTS fund_tags (
            tag_id    INTEGER PRIMARY KEY AUTOINCREMENT,
            tag_name  TEXT NOT NULL UNIQUE,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS fund_tag_assignments (
            fund_id INTEGER NOT NULL,
            tag_id  INTEGER NOT NULL,
            PRIMARY KEY (fund_id, tag_id)
        );
    """)


def _insert_seed_data(conn: sqlite3.Connection) -> dict:
    """
    Insert two funds with nav records covering various scenarios.

    Returns mapping  {'fund1_id': int, 'fund2_id': int}
    so tests can reference stable IDs.
    """
    # --- funds ---
    conn.execute(
        "INSERT INTO funds (fund_code, fund_name, strategy_l1, display) VALUES (?,?,?,?)",
        ("TEST001", "测试基金一号", "股票多头", "展示"),
    )
    fund1_id = conn.execute(
        "SELECT fund_id FROM funds WHERE fund_code='TEST001'"
    ).fetchone()[0]

    conn.execute(
        "INSERT INTO funds (fund_code, fund_name, strategy_l1, display) VALUES (?,?,?,?)",
        ("TEST002", "测试基金二号", "管理期货", "展示"),
    )
    fund2_id = conn.execute(
        "SELECT fund_id FROM funds WHERE fund_code='TEST002'"
    ).fetchone()[0]

    # --- fund 1 nav: 10 weekly records, all normal ---
    base = date(2024, 1, 5)
    nav = 1.0
    for i in range(10):
        d = (base + timedelta(weeks=i)).strftime("%Y-%m-%d")
        nav = round(nav * 1.005, 6)
        accum = round(nav + 0.02 * i, 6)
        conn.execute(
            """INSERT INTO fund_nav_data
               (fund_id, fund_code, fund_name, nav_date, unit_nav, accum_nav, adj_nav,
                source_id, data_source)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (fund1_id, "TEST001", "测试基金一号", d,
             nav, accum, nav, i + 1, "email"),
        )

    # --- fund 1: one record with unit_nav > 5 (anomalous) ---
    conn.execute(
        """INSERT INTO fund_nav_data
           (fund_id, fund_code, fund_name, nav_date, unit_nav, accum_nav, adj_nav,
            source_id, data_source)
           VALUES (?,?,?,?,?,?,?,?,?)""",
        (fund1_id, "TEST001", "测试基金一号", "2024-03-29",
         10.5, 10.5, 10.5, 99, "email"),
    )

    # --- fund 1: one manually entered record (source_id NULL) ---
    conn.execute(
        """INSERT INTO fund_nav_data
           (fund_id, fund_code, fund_name, nav_date, unit_nav, accum_nav, adj_nav,
            source_id, data_source)
           VALUES (?,?,?,?,?,?,?,?,?)""",
        (fund1_id, "TEST001", "测试基金一号", "2024-04-05",
         1.08, 1.10, 1.08, None, "manual"),
    )

    # --- fund 2 nav: 8 weekly records + one large gap (gap detection) ---
    base2 = date(2024, 1, 5)
    nav2 = 1.0
    for i in range(4):
        d = (base2 + timedelta(weeks=i)).strftime("%Y-%m-%d")
        nav2 = round(nav2 * 1.003, 6)
        conn.execute(
            """INSERT INTO fund_nav_data
               (fund_id, fund_code, fund_name, nav_date, unit_nav, accum_nav, adj_nav,
                source_id, data_source)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (fund2_id, "TEST002", "测试基金二号", d,
             nav2, nav2, nav2, i + 100, "zx_excel"),
        )
    # Gap: jump ~120 days from last record
    gap_end = (base2 + timedelta(weeks=3) + timedelta(days=120)).strftime("%Y-%m-%d")
    conn.execute(
        """INSERT INTO fund_nav_data
           (fund_id, fund_code, fund_name, nav_date, unit_nav, accum_nav, adj_nav,
            source_id, data_source)
           VALUES (?,?,?,?,?,?,?,?,?)""",
        (fund2_id, "TEST002", "测试基金二号", gap_end,
         1.05, 1.05, 1.05, 104, "zx_excel"),
    )
    for i in range(3):
        d = (date.fromisoformat(gap_end) + timedelta(weeks=i + 1)).strftime("%Y-%m-%d")
        conn.execute(
            """INSERT INTO fund_nav_data
               (fund_id, fund_code, fund_name, nav_date, unit_nav, accum_nav, adj_nav,
                source_id, data_source)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (fund2_id, "TEST002", "测试基金二号", d,
             round(1.05 + 0.005 * (i + 1), 6), None, round(1.05 + 0.005 * (i + 1), 6),
             105 + i, "zx_excel"),
        )

    conn.commit()
    return {"fund1_id": fund1_id, "fund2_id": fund2_id}


@pytest.fixture(scope="module")
def seed_ids(tmp_path_factory):
    """Provide seed fund IDs from the shared test DB."""
    db_path = tmp_path_factory.mktemp("db") / "test_fund.db"
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    _create_schema(conn)
    ids = _insert_seed_data(conn)
    conn.close()
    return ids, db_path


@pytest.fixture(scope="module")
def client(seed_ids):
    """FastAPI TestClient wired to the test database via dependency override."""
    _, db_path = seed_ids

    from api import app, get_db
    import sqlite3 as _sqlite3
    from contextlib import contextmanager

    def _override_get_db():
        conn = _sqlite3.connect(str(db_path), check_same_thread=False)
        conn.row_factory = _sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    app.dependency_overrides[get_db] = _override_get_db
    with TestClient(app, raise_server_exceptions=False) as c:
        yield c
    app.dependency_overrides.clear()


@pytest.fixture(scope="module")
def fids(seed_ids):
    ids, _ = seed_ids
    return ids


# Helpers
ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def is_iso(s):
    return s is None or bool(ISO_DATE_RE.match(str(s)))


# ===========================================================================
# TestFundSchema — verify the test DB itself has the new English schema
# ===========================================================================

class TestFundSchema:
    def test_funds_has_english_columns(self, seed_ids):
        _, db_path = seed_ids
        conn = sqlite3.connect(str(db_path))
        cols = {r[1] for r in conn.execute("PRAGMA table_info(funds)").fetchall()}
        conn.close()
        assert "fund_code" in cols
        assert "fund_name" in cols
        assert "created_at" in cols
        assert "产品代码" not in cols
        assert "产品名称" not in cols
        assert "首次录入时间" not in cols

    def test_fund_nav_data_has_english_columns(self, seed_ids):
        _, db_path = seed_ids
        conn = sqlite3.connect(str(db_path))
        cols = {r[1] for r in conn.execute("PRAGMA table_info(fund_nav_data)").fetchall()}
        conn.close()
        assert "nav_date" in cols
        assert "unit_nav" in cols
        assert "accum_nav" in cols
        assert "adj_nav" in cols
        assert "净值日期" not in cols
        assert "单位净值" not in cols
        assert "累计单位净值" not in cols

    def test_nav_date_format_is_iso(self, seed_ids):
        _, db_path = seed_ids
        conn = sqlite3.connect(str(db_path))
        rows = conn.execute("SELECT nav_date FROM fund_nav_data").fetchall()
        conn.close()
        for (d,) in rows:
            assert ISO_DATE_RE.match(d), f"nav_date not ISO: {d!r}"

    def test_data_source_values(self, seed_ids):
        _, db_path = seed_ids
        conn = sqlite3.connect(str(db_path))
        rows = conn.execute(
            "SELECT DISTINCT data_source FROM fund_nav_data WHERE data_source IS NOT NULL"
        ).fetchall()
        conn.close()
        allowed = {"email", "manual", "zx_excel"}
        for (ds,) in rows:
            assert ds in allowed, f"Unexpected data_source: {ds!r}"


# ===========================================================================
# TestGetFunds — GET /api/funds
# ===========================================================================

class TestGetFunds:
    def test_returns_200(self, client):
        r = client.get("/api/funds")
        assert r.status_code == 200

    def test_response_has_total_and_items(self, client):
        data = client.get("/api/funds").json()
        assert "total" in data
        assert "items" in data

    def test_total_equals_items_length(self, client):
        data = client.get("/api/funds").json()
        assert data["total"] == len(data["items"])

    def test_item_has_product_code_field(self, client):
        items = client.get("/api/funds").json()["items"]
        assert len(items) >= 1
        for item in items:
            assert "product_code" in item

    def test_item_has_product_name_field(self, client):
        items = client.get("/api/funds").json()["items"]
        for item in items:
            assert "product_name" in item

    def test_fund_code_values_present(self, client):
        codes = {i["product_code"] for i in client.get("/api/funds").json()["items"]}
        assert "TEST001" in codes
        assert "TEST002" in codes

    def test_earliest_latest_date_format(self, client):
        items = client.get("/api/funds").json()["items"]
        for item in items:
            assert is_iso(item.get("earliest_date")), item
            assert is_iso(item.get("latest_date")), item

    def test_quality_filter_on_by_default(self, client, fids):
        """record_count with default apply_filter=true excludes unit_nav > 5."""
        items = client.get("/api/funds").json()["items"]
        f1 = next(i for i in items if i["product_code"] == "TEST001")
        # Fund1 has 12 total rows: 10 normal + 1 anomalous (10.5) + 1 manual (1.08)
        # With filter on, the anomalous row (10.5) should be excluded → 11
        assert f1["record_count"] == 11

    def test_filter_off_includes_anomalous(self, client):
        items = client.get("/api/funds?apply_filter=false").json()["items"]
        f1 = next(i for i in items if i["product_code"] == "TEST001")
        assert f1["record_count"] == 12


# ===========================================================================
# TestSearchFunds — GET /api/funds/search
# ===========================================================================

class TestSearchFunds:
    def test_search_by_code(self, client):
        data = client.get("/api/funds/search?q=TEST001").json()
        assert data["total"] >= 1
        assert any(i["product_code"] == "TEST001" for i in data["items"])

    def test_search_by_name(self, client):
        data = client.get("/api/funds/search?q=测试基金").json()
        assert data["total"] >= 1

    def test_no_match_returns_empty(self, client):
        data = client.get("/api/funds/search?q=NOTEXIST_XYZ").json()
        assert data["total"] == 0
        assert data["items"] == []

    def test_result_has_product_code(self, client):
        items = client.get("/api/funds/search?q=TEST").json()["items"]
        for item in items:
            assert "product_code" in item


# ===========================================================================
# TestGetFundDetail — GET /api/funds/{fund_id}
# ===========================================================================

class TestGetFundDetail:
    def test_returns_200_for_existing(self, client, fids):
        r = client.get(f"/api/funds/{fids['fund1_id']}")
        assert r.status_code == 200

    def test_returns_404_for_missing(self, client):
        r = client.get("/api/funds/99999")
        assert r.status_code == 404

    def test_product_code_correct(self, client, fids):
        data = client.get(f"/api/funds/{fids['fund1_id']}").json()
        assert data["product_code"] == "TEST001"

    def test_product_name_correct(self, client, fids):
        data = client.get(f"/api/funds/{fids['fund1_id']}").json()
        assert data["product_name"] == "测试基金一号"

    def test_dates_are_iso_format(self, client, fids):
        data = client.get(f"/api/funds/{fids['fund1_id']}").json()
        assert is_iso(data.get("earliest_date"))
        assert is_iso(data.get("latest_date"))

    def test_record_count_present(self, client, fids):
        data = client.get(f"/api/funds/{fids['fund1_id']}").json()
        assert isinstance(data["record_count"], int)
        assert data["record_count"] > 0


# ===========================================================================
# TestGetFundNav — GET /api/funds/{fund_id}/nav
# ===========================================================================

class TestGetFundNav:
    def test_returns_200(self, client, fids):
        r = client.get(f"/api/funds/{fids['fund1_id']}/nav")
        assert r.status_code == 200

    def test_response_has_total_fund_id_items(self, client, fids):
        data = client.get(f"/api/funds/{fids['fund1_id']}/nav").json()
        assert "total" in data
        assert "fund_id" in data
        assert "items" in data

    def test_nav_date_is_iso_format(self, client, fids):
        items = client.get(f"/api/funds/{fids['fund1_id']}/nav").json()["items"]
        assert len(items) > 0
        for item in items:
            assert ISO_DATE_RE.match(item["nav_date"]), f"Bad date: {item['nav_date']!r}"

    def test_unit_nav_is_float(self, client, fids):
        items = client.get(f"/api/funds/{fids['fund1_id']}/nav").json()["items"]
        for item in items:
            assert isinstance(item["unit_nav"], float)

    def test_accumulated_nav_field_present(self, client, fids):
        items = client.get(f"/api/funds/{fids['fund1_id']}/nav").json()["items"]
        for item in items:
            assert "accumulated_nav" in item

    def test_adjusted_nav_field_present(self, client, fids):
        items = client.get(f"/api/funds/{fids['fund1_id']}/nav").json()["items"]
        for item in items:
            assert "adjusted_nav" in item

    def test_records_ordered_asc(self, client, fids):
        items = client.get(f"/api/funds/{fids['fund1_id']}/nav").json()["items"]
        dates = [i["nav_date"] for i in items]
        assert dates == sorted(dates)

    def test_quality_filter_removes_high_nav(self, client, fids):
        """Default apply_filter=true: no unit_nav > 5 in results."""
        items = client.get(f"/api/funds/{fids['fund1_id']}/nav").json()["items"]
        assert all(i["unit_nav"] <= 5 for i in items)

    def test_filter_off_includes_high_nav(self, client, fids):
        items = client.get(
            f"/api/funds/{fids['fund1_id']}/nav?apply_filter=false"
        ).json()["items"]
        assert any(i["unit_nav"] > 5 for i in items)

    def test_date_from_filter(self, client, fids):
        items = client.get(
            f"/api/funds/{fids['fund1_id']}/nav?date_from=2024-02-01"
        ).json()["items"]
        assert all(i["nav_date"] >= "2024-02-01" for i in items)

    def test_date_to_filter(self, client, fids):
        items = client.get(
            f"/api/funds/{fids['fund1_id']}/nav?date_to=2024-02-28"
        ).json()["items"]
        assert all(i["nav_date"] <= "2024-02-28" for i in items)

    def test_date_range_filter(self, client, fids):
        items = client.get(
            f"/api/funds/{fids['fund1_id']}/nav?date_from=2024-01-15&date_to=2024-02-28"
        ).json()["items"]
        for item in items:
            assert "2024-01-15" <= item["nav_date"] <= "2024-02-28"

    def test_date_from_after_date_to_returns_400(self, client, fids):
        r = client.get(
            f"/api/funds/{fids['fund1_id']}/nav?date_from=2024-06-01&date_to=2024-01-01"
        )
        assert r.status_code == 400

    def test_manual_record_has_null_source_id(self, client, fids):
        items = client.get(
            f"/api/funds/{fids['fund1_id']}/nav?apply_filter=false"
        ).json()["items"]
        manual = [i for i in items if i["nav_date"] == "2024-04-05"]
        assert len(manual) == 1
        assert manual[0]["source_id"] is None

    def test_pagination_limit(self, client, fids):
        items = client.get(
            f"/api/funds/{fids['fund1_id']}/nav?limit=3&apply_filter=false"
        ).json()["items"]
        assert len(items) == 3

    def test_pagination_offset(self, client, fids):
        all_items = client.get(
            f"/api/funds/{fids['fund1_id']}/nav?apply_filter=false"
        ).json()["items"]
        offset_items = client.get(
            f"/api/funds/{fids['fund1_id']}/nav?offset=1&apply_filter=false"
        ).json()["items"]
        assert len(offset_items) == len(all_items) - 1
        assert offset_items[0]["nav_date"] == all_items[1]["nav_date"]

    def test_fund_not_found_returns_404(self, client):
        r = client.get("/api/funds/99999/nav")
        assert r.status_code == 404


# ===========================================================================
# TestCreateNav — POST /api/nav
# ===========================================================================

class TestCreateNav:
    def test_create_returns_201(self, client):
        r = client.post("/api/nav", json={
            "product_code": "TEST001",
            "product_name": "测试基金一号",
            "nav_date": "2024-05-10",
            "unit_nav": 1.15,
            "accumulated_nav": 1.20,
        })
        assert r.status_code == 201

    def test_created_record_has_iso_date(self, client):
        r = client.post("/api/nav", json={
            "product_code": "TEST001",
            "nav_date": "2024-05-17",
            "unit_nav": 1.16,
        })
        assert r.status_code == 201
        assert ISO_DATE_RE.match(r.json()["nav_date"])

    def test_created_record_values_correct(self, client):
        r = client.post("/api/nav", json={
            "product_code": "TEST001",
            "nav_date": "2024-05-24",
            "unit_nav": 1.17,
            "accumulated_nav": 1.22,
        })
        data = r.json()
        assert data["unit_nav"] == 1.17
        assert data["accumulated_nav"] == 1.22
        assert data["nav_date"] == "2024-05-24"

    def test_created_record_source_id_null(self, client):
        r = client.post("/api/nav", json={
            "product_code": "TEST001",
            "nav_date": "2024-05-31",
            "unit_nav": 1.18,
        })
        assert r.json()["source_id"] is None

    def test_duplicate_returns_409(self, client):
        # "2024-05-10" already created in test_create_returns_201
        r = client.post("/api/nav", json={
            "product_code": "TEST001",
            "nav_date": "2024-05-10",
            "unit_nav": 1.15,
        })
        assert r.status_code == 409

    def test_invalid_date_format_returns_422(self, client):
        r = client.post("/api/nav", json={
            "product_code": "TEST001",
            "nav_date": "20240101",
            "unit_nav": 1.0,
        })
        assert r.status_code == 422

    def test_zero_unit_nav_returns_422(self, client):
        r = client.post("/api/nav", json={
            "product_code": "TEST001",
            "nav_date": "2024-06-07",
            "unit_nav": 0,
        })
        assert r.status_code == 422

    def test_negative_unit_nav_returns_422(self, client):
        r = client.post("/api/nav", json={
            "product_code": "TEST001",
            "nav_date": "2024-06-07",
            "unit_nav": -0.5,
        })
        assert r.status_code == 422


# ===========================================================================
# TestUpdateNav — PUT /api/nav/{nav_id}
# ===========================================================================

class TestUpdateNav:
    @pytest.fixture(scope="class")
    def nav_id(self, client):
        """Create a fresh record for update tests."""
        r = client.post("/api/nav", json={
            "product_code": "TEST001",
            "nav_date": "2024-07-05",
            "unit_nav": 1.20,
            "accumulated_nav": 1.25,
        })
        assert r.status_code == 201
        return r.json()["id"]

    def test_update_unit_nav(self, client, nav_id):
        r = client.put(f"/api/nav/{nav_id}", json={"unit_nav": 1.99})
        assert r.status_code == 200
        assert r.json()["unit_nav"] == 1.99

    def test_update_date(self, client, nav_id):
        r = client.put(f"/api/nav/{nav_id}", json={"nav_date": "2024-07-12"})
        assert r.status_code == 200
        assert r.json()["nav_date"] == "2024-07-12"
        assert ISO_DATE_RE.match(r.json()["nav_date"])

    def test_update_nonexistent_returns_404(self, client):
        r = client.put("/api/nav/99999", json={"unit_nav": 1.5})
        assert r.status_code == 404

    def test_date_conflict_returns_409(self, client, fids):
        """Change date to one that already exists for same fund_code → 409."""
        # Create two records then try to make one collide with the other
        r1 = client.post("/api/nav", json={
            "product_code": "TEST002",
            "nav_date": "2024-08-02",
            "unit_nav": 1.01,
        })
        r2 = client.post("/api/nav", json={
            "product_code": "TEST002",
            "nav_date": "2024-08-09",
            "unit_nav": 1.02,
        })
        assert r1.status_code == 201
        assert r2.status_code == 201
        # Try to move r2 to r1's date
        r = client.put(f"/api/nav/{r2.json()['id']}", json={"nav_date": "2024-08-02"})
        assert r.status_code == 409


# ===========================================================================
# TestDeleteNav — DELETE /api/nav/{nav_id}
# ===========================================================================

class TestDeleteNav:
    @pytest.fixture()
    def deletable_nav_id(self, client):
        r = client.post("/api/nav", json={
            "product_code": "TEST001",
            "nav_date": "2024-09-13",
            "unit_nav": 1.30,
        })
        assert r.status_code == 201
        return r.json()["id"]

    def test_delete_returns_204(self, client, deletable_nav_id):
        r = client.delete(f"/api/nav/{deletable_nav_id}")
        assert r.status_code == 204

    def test_record_gone_after_delete(self, client, deletable_nav_id):
        client.delete(f"/api/nav/{deletable_nav_id}")
        r = client.get(f"/api/nav/{deletable_nav_id}")
        assert r.status_code == 404

    def test_delete_nonexistent_returns_404(self, client):
        r = client.delete("/api/nav/99999")
        assert r.status_code == 404


# ===========================================================================
# TestCompareFunds — GET /api/compare
# ===========================================================================

class TestCompareFunds:
    def test_compare_two_funds(self, client, fids):
        r = client.get(
            f"/api/compare?fund_ids={fids['fund1_id']}&fund_ids={fids['fund2_id']}"
        )
        assert r.status_code == 200
        funds = r.json()["funds"]
        assert str(fids["fund1_id"]) in funds
        assert str(fids["fund2_id"]) in funds

    def test_series_dates_are_iso(self, client, fids):
        r = client.get(
            f"/api/compare?fund_ids={fids['fund1_id']}&fund_ids={fids['fund2_id']}"
        )
        for _fid, fdata in r.json()["funds"].items():
            for point in fdata["series"]:
                assert ISO_DATE_RE.match(point["date"]), f"Bad date: {point['date']!r}"

    def test_compare_date_range_filter(self, client, fids):
        r = client.get(
            f"/api/compare?fund_ids={fids['fund1_id']}"
            f"&date_from=2024-01-15&date_to=2024-02-28"
        )
        assert r.status_code == 200
        for point in r.json()["funds"][str(fids["fund1_id"])]["series"]:
            assert "2024-01-15" <= point["date"] <= "2024-02-28"

    def test_too_many_fund_ids_returns_400(self, client, fids):
        ids_param = "&".join(f"fund_ids={i}" for i in range(1, 22))
        r = client.get(f"/api/compare?{ids_param}")
        assert r.status_code == 400

    def test_missing_fund_id_returns_404(self, client):
        r = client.get("/api/compare?fund_ids=99999")
        assert r.status_code == 404


# ===========================================================================
# TestFundIssues — GET /api/funds/{fund_id}/issues and /api/funds/issues
# ===========================================================================

class TestFundIssues:
    def test_no_issues_for_clean_fund_structure(self, client, fids):
        """Fund 2 has a deliberate gap; at minimum anomalous list should be empty."""
        data = client.get(f"/api/funds/{fids['fund2_id']}/issues").json()
        assert "anomalous" in data
        assert "gaps" in data
        assert data["anomalous"] == []

    def test_anomalous_nav_detected_for_fund1(self, client, fids):
        data = client.get(
            f"/api/funds/{fids['fund1_id']}/issues?apply_filter=false"
            if False  # issues endpoint has no apply_filter param; anomalous is computed internally
            else f"/api/funds/{fids['fund1_id']}/issues"
        ).json()
        assert len(data["anomalous"]) >= 1

    def test_anomalous_date_is_iso(self, client, fids):
        data = client.get(f"/api/funds/{fids['fund1_id']}/issues").json()
        for rec in data["anomalous"]:
            assert ISO_DATE_RE.match(rec["nav_date"]), rec

    def test_gap_detected_for_fund2(self, client, fids):
        """Fund 2 has a ~120-day gap; it must appear in gaps list."""
        data = client.get(f"/api/funds/{fids['fund2_id']}/issues").json()
        assert len(data["gaps"]) >= 1

    def test_gap_dates_are_iso(self, client, fids):
        data = client.get(f"/api/funds/{fids['fund2_id']}/issues").json()
        for gap in data["gaps"]:
            assert ISO_DATE_RE.match(gap["from_date"]), gap
            assert ISO_DATE_RE.match(gap["to_date"]), gap

    def test_all_issues_contains_all_funds(self, client, fids):
        data = client.get("/api/funds/issues").json()
        issues = data["issues"]
        assert str(fids["fund1_id"]) in issues
        assert str(fids["fund2_id"]) in issues

    def test_issues_returns_404_for_missing_fund(self, client):
        r = client.get("/api/funds/99999/issues")
        assert r.status_code == 404


# ===========================================================================
# TestFundReturns — GET /api/funds/returns
# ===========================================================================

class TestFundReturns:
    def test_returns_200(self, client):
        r = client.get("/api/funds/returns")
        assert r.status_code == 200

    def test_returns_items_for_seed_funds(self, client, fids):
        data = client.get("/api/funds/returns?periods=inception").json()
        items = data["items"]
        assert str(fids["fund1_id"]) in items
        assert str(fids["fund2_id"]) in items

    def test_period_return_is_numeric_or_null(self, client, fids):
        data = client.get("/api/funds/returns?periods=1w,1m,3m,inception").json()
        for fid_str, entry in data["items"].items():
            for period in ["1w", "1m", "3m", "inception"]:
                v = entry.get(period)
                assert v is None or isinstance(v, (int, float)), \
                    f"fund {fid_str} period {period}: {v!r}"

    def test_sparkline_is_list(self, client, fids):
        data = client.get("/api/funds/returns").json()
        for fid_str, entry in data["items"].items():
            assert isinstance(entry.get("sparkline"), list), fid_str

    def test_inception_return_computed_for_fund1(self, client, fids):
        data = client.get("/api/funds/returns?periods=inception").json()
        val = data["items"].get(str(fids["fund1_id"]), {}).get("inception")
        assert val is not None
        assert isinstance(val, float)
