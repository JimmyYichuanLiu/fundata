import sqlite3

import pytest

from fund_store import initialize_connection, initialize_database, normalize_nav_date, recalculate_adj_nav


def test_migration_preserves_conflicting_evidence_and_ids():
    conn = sqlite3.connect(':memory:')
    conn.executescript('''
    CREATE TABLE funds (fund_id INTEGER PRIMARY KEY, fund_code TEXT UNIQUE, fund_name TEXT);
    INSERT INTO funds VALUES (42, 'A', 'Alpha');
    CREATE TABLE fund_nav_data (id INTEGER PRIMARY KEY, fund_id INTEGER, fund_code TEXT,
        nav_date TEXT, unit_nav REAL, accum_nav REAL, UNIQUE(fund_code,nav_date));
    INSERT INTO fund_nav_data VALUES (1,42,'A','20260101',1,1);
    INSERT INTO fund_nav_data VALUES (2,42,'A','2026-01-01',1.1,1.1);
    INSERT INTO fund_nav_data VALUES (3,42,'A','bad',1,1);
    INSERT INTO fund_nav_data VALUES (4,42,'A','2026-01-02',-1,1);
    ''')
    initialize_connection(conn)
    assert conn.execute('SELECT count(*) FROM fund_nav_data').fetchone()[0] == 4
    assert conn.execute('SELECT id, fund_id FROM valid_fund_nav').fetchall() == [(2, 42)]
    assert conn.execute('SELECT count(*) FROM nav_quality_audit').fetchone()[0] >= 3
    initialize_connection(conn)
    assert conn.execute('SELECT count(*) FROM valid_fund_nav').fetchone()[0] == 1


def test_adjustment_uses_existing_formula_and_refuses_partial_series():
    conn = sqlite3.connect(':memory:')
    initialize_connection(conn)
    conn.execute("INSERT INTO funds(fund_code) VALUES ('A')")
    conn.executemany('INSERT INTO fund_nav_data(fund_id,fund_code,nav_date,unit_nav,accum_nav) VALUES(1,\'A\',?,?,?)',
                     [('2026-01-01',1,1),('2026-01-02',.9,1.1)])
    recalculate_adj_nav(conn)
    assert conn.execute('SELECT adj_nav FROM valid_fund_nav ORDER BY nav_date').fetchall() == [(1.0,), (pytest.approx(1.1),)]
    conn.execute("UPDATE fund_nav_data SET accum_nav=NULL WHERE nav_date='2026-01-02'")
    recalculate_adj_nav(conn)
    assert conn.execute('SELECT count(adj_nav) FROM valid_fund_nav').fetchone()[0] == 0


def test_existing_database_is_backed_up_before_migration(tmp_path):
    path = tmp_path / 'sample.db'
    with sqlite3.connect(path) as conn:
        conn.execute('CREATE TABLE unrelated (value TEXT)')
        conn.execute("INSERT INTO unrelated VALUES ('preserve')")
    conn = initialize_database(path)
    assert conn.execute('SELECT value FROM unrelated').fetchone()[0] == 'preserve'
    conn.close()
    assert list((tmp_path / 'backups').glob('*.db'))


def test_future_nav_dates_are_not_valid():
    from datetime import date,timedelta
    assert normalize_nav_date('9999-01-01') is None
    assert normalize_nav_date((date.today()+timedelta(days=1)).isoformat()) is None
    assert normalize_nav_date(date.today().isoformat()) == date.today().isoformat()
def test_schema_ddl_rollback_preserves_legacy_layout(monkeypatch):
    import fund_store
    conn = sqlite3.connect(':memory:')
    conn.execute('CREATE TABLE funds(fund_id INTEGER PRIMARY KEY, 产品代码 TEXT UNIQUE, 产品名称 TEXT)')
    conn.execute("INSERT INTO funds VALUES(1,'A','legacy')")
    conn.commit()
    before = conn.execute("SELECT name,sql FROM sqlite_master ORDER BY name").fetchall()
    def fail(*args):
        raise RuntimeError('injected recalculation failure')
    monkeypatch.setattr(fund_store, 'recalculate_adj_nav', fail)
    with pytest.raises(RuntimeError):
        fund_store.initialize_connection(conn)
    assert conn.execute("SELECT name,sql FROM sqlite_master ORDER BY name").fetchall() == before
    assert conn.execute('SELECT 产品代码 FROM funds').fetchone()[0] == 'A'
    conn.close()
