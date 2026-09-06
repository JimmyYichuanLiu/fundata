import sqlite3

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from admin_auth import configure_auth, create_admin, initialize_auth


@pytest.fixture
def auth_client(tmp_path, monkeypatch):
    path = tmp_path / 'auth.db'
    with sqlite3.connect(path) as conn:
        initialize_auth(conn)
        create_admin(conn, 'owner', 'correct-long-password')
    monkeypatch.setenv('FUNDATA_READONLY', '0')
    monkeypatch.setenv('FUNDATA_ALLOWED_ORIGINS', 'https://testserver')
    app = FastAPI()
    configure_auth(app, lambda: str(path))

    @app.post('/api/nav')
    def write():
        return {'saved': True}

    @app.get('/api/failures')
    def private_read():
        return {'private': True}

    @app.get('/api/funds')
    def public_read():
        return {'items': []}

    with TestClient(app, base_url='https://testserver') as client:
        yield client, path


def login(client):
    return client.post('/api/auth/login', json={
        'username': 'owner', 'password': 'correct-long-password',
    }, headers={'Origin': 'https://testserver'})


def test_anonymous_reads_but_cannot_write_or_read_mail(auth_client):
    client, _ = auth_client
    assert client.get('/api/funds').status_code == 200
    assert client.post('/api/nav').status_code == 401
    assert client.get('/api/failures').status_code == 401


def test_session_cookie_csrf_and_logout(auth_client):
    client, path = auth_client
    response = login(client)
    assert response.status_code == 200
    assert response.json()['authenticated'] is True
    cookie = response.headers['set-cookie'].lower()
    assert 'httponly' in cookie and 'secure' in cookie and 'samesite=strict' in cookie
    csrf = response.json()['csrf_token']
    assert client.post('/api/nav').status_code == 403
    assert client.post('/api/nav', headers={'X-CSRF-Token': csrf}).status_code == 200
    assert client.post('/api/nav', headers={'X-CSRF-Token': csrf, 'Origin': 'https://evil.example'}).status_code == 403
    with sqlite3.connect(path) as conn:
        token_hash = conn.execute('SELECT token_hash FROM admin_sessions').fetchone()[0]
    assert token_hash != client.cookies.get('fundtrack_session')
    assert client.post('/api/auth/logout', headers={'X-CSRF-Token': csrf}).status_code == 200
    assert client.get('/api/auth/session').json()['authenticated'] is False


def test_public_readonly_disables_login_and_existing_sessions(auth_client, monkeypatch):
    client, _ = auth_client
    csrf = login(client).json()['csrf_token']
    monkeypatch.setenv('FUNDATA_READONLY', '1')
    assert login(client).status_code == 403
    assert client.post('/api/nav', headers={'X-CSRF-Token': csrf}).status_code == 403
    session = client.get('/api/auth/session').json()
    assert session['readonly'] is True and session['authenticated'] is False


def test_wrong_password_rate_limit_and_expiry(auth_client):
    client, path = auth_client
    for _ in range(10):
        response = client.post('/api/auth/login', json={'username': 'owner', 'password': 'wrong'})
        assert response.status_code == 401
    assert login(client).status_code == 429
    with sqlite3.connect(path) as conn:
        conn.execute('DELETE FROM admin_login_attempts')
    assert login(client).status_code == 200
    with sqlite3.connect(path) as conn:
        conn.execute('UPDATE admin_sessions SET expires_at = 0')
    assert client.get('/api/auth/session').json()['authenticated'] is False


def test_reject_cross_origin_login_and_short_password(auth_client):
    client, path = auth_client
    assert client.post('/api/auth/login', json={'username': 'owner', 'password': 'correct-long-password'}, headers={'Origin': 'https://evil.example'}).status_code == 403
    with sqlite3.connect(path) as conn:
        with pytest.raises(ValueError):
            create_admin(conn, 'unsafe', 'short')
