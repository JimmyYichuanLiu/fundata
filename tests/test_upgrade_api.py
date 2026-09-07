"""Integration regressions on isolated databases; never touch the real mailbox."""
import sqlite3

import pytest
from fastapi.testclient import TestClient
from admin_auth import create_admin
from fund_store import initialize_database, refresh_quality, recalculate_adj_nav


@pytest.fixture
def client(tmp_path, monkeypatch):
    import api
    path = tmp_path / 'upgrade.db'
    initialize_database(path).close()
    monkeypatch.setattr(api, 'DB_PATH', str(path))
    monkeypatch.setenv('FUNDATA_READONLY', '0')
    monkeypatch.setenv('FUNDATA_SCHEDULER_ENABLED', '0')
    monkeypatch.setenv('FUNDATA_ALLOWED_ORIGINS', 'https://testserver')
    with TestClient(api.app, base_url='https://testserver', raise_server_exceptions=False) as test:
        with sqlite3.connect(path) as conn:
            create_admin(conn, 'owner', 'correct-long-password')
            conn.executemany('INSERT INTO funds(fund_code,fund_name) VALUES (?,?)', [('A','基金甲'),('B','基金乙'),('BAD','错误基金')])
            conn.executemany('INSERT INTO fund_nav_data(fund_id,fund_code,nav_date,unit_nav,accum_nav,data_source) VALUES (?,?,?,?,?,?)', [
                (1,'A','2024-01-01',1,1,'email'),(1,'A','2024-02-01',1.1,1.1,'email'),
                (2,'B','2024-01-01',1,None,'zx_excel'),(2,'B','2024-02-01',1.2,None,'zx_excel'),
                (3,'BAD','无法识别的日期',1,1,'email')])
            refresh_quality(conn)
            recalculate_adj_nav(conn)
        test.db_path = path
        yield test


def authenticate(client):
    response = client.post('/api/auth/login', json={'username':'owner','password':'correct-long-password'})
    assert response.status_code == 200, response.text
    client.headers['X-CSRF-Token'] = response.json()['csrf_token']


def test_public_stats_filter_and_nav_quality(client):
    stats = client.get('/api/stats').json()
    assert stats['total_funds'] == 2
    assert stats['manual_records'] == 0
    assert stats['latest_nav_date'] == '2024-02-01'
    assert stats['quarantined_records'] == 1
    funds = client.get('/api/funds?source=zx_excel').json()['items']
    assert len(funds) == 1 and funds[0]['product_code'] == 'B'
    assert funds[0]['sources'] == ['zx_excel']
    assert client.get('/api/funds/3/nav?apply_filter=false').json()['items'] == []
    nav = client.get('/api/funds/2/nav').json()['items']
    assert all(row['adj_nav'] is None and row['adj_nav_reason'] for row in nav)
    assert client.get('/api/funds?source=invalid').status_code == 422


def test_sensitive_routes_and_writes_protected(client):
    for url in ('/api/failures','/api/sync/history','/api/export/email.xlsx'):
        assert client.get(url).status_code == 401
    for url in ('/api/nav','/api/sync/trigger','/api/portfolios'):
        assert client.post(url, json={}).status_code == 401
    with sqlite3.connect(client.db_path) as conn:
        conn.execute("INSERT INTO sync_state VALUES ('sync_last_error','SECRET_MAIL_DETAIL')")
        conn.execute("INSERT INTO sync_state VALUES ('sync_last_added','')")
    response = client.get('/api/sync/status')
    assert response.status_code == 200, response.text
    assert 'SECRET_MAIL_DETAIL' not in response.text


def test_admin_nav_validation_export_and_retry(client):
    authenticate(client)
    assert client.post('/api/nav', json={'product_code':'A','nav_date':'2024-02-31','unit_nav':1}).status_code == 422
    response = client.post('/api/nav', json={'product_code':'C','product_name':'基金丙','nav_date':'2024-01-01','unit_nav':1,'accumulated_nav':1})
    assert response.status_code == 201, response.text
    assert response.json()['data_source'] == 'manual'
    response = client.get('/api/export/email.xlsx')
    assert response.status_code == 200, response.text
    assert response.content[:2] == b'PK'
    with sqlite3.connect(client.db_path) as conn:
        conn.execute("INSERT INTO extraction_failures(失败原因) VALUES ('legacy')")
    failures = client.get('/api/failures').json()['items']
    assert failures[0]['retryable'] is False
    assert client.post('/api/failures/1/retry').status_code == 409


def test_compare_limit_portfolio_validation_and_missing(client):
    assert client.get('/api/compare?' + '&'.join(f'fund_ids={i}' for i in range(1,10))).status_code == 400
    authenticate(client)
    payload = {'portfolio_name':'组合','build_method':'UNIFIED_START','constituents':[
        {'fund_id':1,'target_weight':0.5,'effective_date':'2024-01-01'},
        {'fund_id':2,'target_weight':0.4,'effective_date':'2024-01-01'}]}
    assert client.post('/api/portfolios', json=payload).status_code == 422
    payload['constituents'][1]['target_weight'] = 0.5
    result = client.post('/api/portfolios', json=payload)
    assert result.status_code == 200, result.text
    pid = result.json()['id']
    assert client.post(f'/api/portfolios/{pid}/calculate').status_code == 409
    metrics = client.get(f'/api/portfolios/{pid}/metrics').json()
    assert all(value is None for value in metrics.values())


def test_readonly_disables_admin_but_public_data_works(client, monkeypatch):
    authenticate(client)
    monkeypatch.setenv('FUNDATA_READONLY','1')
    assert client.get('/api/funds').status_code == 200
    assert client.get('/api/failures').status_code == 403
    assert client.post('/api/auth/login', json={'username':'owner','password':'correct-long-password'}).status_code == 403


def test_portfolio_missing_observations_do_not_erase_holdings():
    from api import _calculate_batch_include, _calculate_unified_start
    constituents = [dict(fund_id=i, target_weight=0.5, target_amount=100, effective_date='2024-01-01') for i in (1,2)]
    navs = {1:{'2024-01-01':1,'2024-01-02':1,'2024-01-03':1}, 2:{'2024-01-01':1,'2024-01-03':1}}
    assert [row['portfolio_nav'] for row in _calculate_unified_start(constituents, navs)] == [1,1]
    assert [row['portfolio_nav'] for row in _calculate_batch_include(constituents, navs)] == [1,1,1]


def test_batch_rebalance_keeps_incumbent_current_day_return():
    from api import _calculate_batch_include
    constituents = [dict(fund_id=1,target_amount=100,effective_date='2024-01-01'),
                    dict(fund_id=2,target_amount=100,effective_date='2024-01-02')]
    navs = {1:{'2024-01-01':1,'2024-01-02':2}, 2:{'2024-01-02':1}}
    assert [row['portfolio_nav'] for row in _calculate_batch_include(constituents, navs)] == [1,2]


def test_manual_edit_recalculates_entire_adjusted_series(client):
    authenticate(client)
    first = client.get('/api/funds/1/nav').json()['items'][0]['id']
    response = client.put(f'/api/nav/{first}',json={'unit_nav':2,'accumulated_nav':2})
    assert response.status_code == 200, response.text
    nav = client.get('/api/funds/1/nav').json()['items']
    assert nav[-1]['adj_nav'] == pytest.approx(0.55)
    assert client.delete(f'/api/nav/{first}').status_code == 204
    assert client.get('/api/funds/1/nav').json()['items'][0]['adj_nav'] == 1


def test_portfolio_cache_is_marked_stale_after_nav_change(client):
    authenticate(client)
    with sqlite3.connect(client.db_path) as conn:
        conn.execute('UPDATE fund_nav_data SET accum_nav=unit_nav WHERE fund_id=2')
        recalculate_adj_nav(conn, 'B')
    body = {'portfolio_name':'缓存测试','build_method':'UNIFIED_START','constituents':[
        {'fund_id':fid,'target_weight':0.5,'effective_date':'2024-01-01'} for fid in (1,2)]}
    pid = client.post('/api/portfolios',json=body).json()['id']
    assert client.post(f'/api/portfolios/{pid}/calculate').status_code == 200
    assert len(client.get(f'/api/portfolios/{pid}/nav').json()['items']) == 2
    first = client.get('/api/funds/1/nav').json()['items'][0]['id']
    assert client.put(f'/api/nav/{first}',json={'unit_nav':2,'accumulated_nav':2}).status_code == 200
    result = client.get(f'/api/portfolios/{pid}/nav').json()
    assert result['stale'] is True and result['items'] == [] and result['reason']
