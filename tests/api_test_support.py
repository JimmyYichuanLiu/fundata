"""Isolated authenticated client for legacy endpoint regression suites."""
from contextlib import contextmanager
import sqlite3

import pytest
from fastapi.testclient import TestClient
from admin_auth import create_admin


@contextmanager
def authenticated_client(app, db_path):
    import api
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(api, 'DB_PATH', str(db_path))
        patch.setenv('FUNDATA_READONLY', '0')
        patch.setenv('FUNDATA_SCHEDULER_ENABLED', '0')
        patch.setenv('FUNDATA_ALLOWED_ORIGINS', 'https://testserver')
        with TestClient(app, base_url='https://testserver', raise_server_exceptions=False) as client:
            with sqlite3.connect(db_path) as conn:
                create_admin(conn, 'test-admin', 'isolated-test-password')
            response = client.post('/api/auth/login', json={'username':'test-admin','password':'isolated-test-password'})
            assert response.status_code == 200, response.text
            client.headers['X-CSRF-Token'] = response.json()['csrf_token']
            yield client
