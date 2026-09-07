"""Single-administrator authentication. Public deployments are read-only by default."""
import argparse
import getpass
import hashlib
import hmac
import ipaddress
import os
import secrets
import sqlite3
import time
from contextlib import contextmanager
from typing import Callable

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from starlette.concurrency import run_in_threadpool

COOKIE = 'fundtrack_session'
SESSION_SECONDS = 8 * 60 * 60


def readonly() -> bool:
    return os.getenv('FUNDATA_READONLY', '1') != '0'


def allowed_origins() -> list[str]:
    default = 'http://localhost:5173,http://127.0.0.1:5173,http://localhost:8000,http://127.0.0.1:8000'
    return [value.strip().rstrip('/') for value in os.getenv('FUNDATA_ALLOWED_ORIGINS', default).split(',') if value.strip()]


def initialize_auth(conn: sqlite3.Connection) -> None:
    conn.executescript('''
        CREATE TABLE IF NOT EXISTS admin_users (
            username TEXT PRIMARY KEY, password_hash TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS admin_sessions (
            token_hash TEXT PRIMARY KEY, username TEXT NOT NULL,
            csrf_token TEXT NOT NULL, expires_at REAL NOT NULL,
            FOREIGN KEY(username) REFERENCES admin_users(username) ON DELETE CASCADE
        );
        CREATE TABLE IF NOT EXISTS admin_login_attempts (
            client TEXT NOT NULL, attempted_at REAL NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_login_attempt_time ON admin_login_attempts(attempted_at);
    ''')


def _password_hash(password: str, salt: str | None = None) -> str:
    salt = salt or secrets.token_hex(16)
    digest = hashlib.scrypt(password.encode(), salt=bytes.fromhex(salt), n=16384, r=8, p=1).hex()
    return f'scrypt${salt}${digest}'


def create_admin(conn: sqlite3.Connection, username: str, password: str) -> None:
    if not username or len(username) > 100 or len(password) < 12 or len(password) > 512:
        raise ValueError('Use a username of 1–100 characters and a password of 12–512 characters')
    conn.execute('INSERT INTO admin_users VALUES (?, ?) ON CONFLICT(username) DO UPDATE SET password_hash=excluded.password_hash',
                 (username, _password_hash(password)))
    conn.execute('DELETE FROM admin_sessions WHERE username=?', (username,))


def _loopback(value: str) -> bool:
    if value == 'localhost':
        return True
    try:
        return ipaddress.ip_address(value).is_loopback
    except ValueError:
        return False


def _secure_transport(request: Request) -> bool:
    if request.url.scheme == 'https':
        return True
    return (os.getenv('FUNDATA_COOKIE_SECURE', '1') == '0'
            and not any(name in request.headers for name in ('forwarded', 'x-forwarded-for', 'x-forwarded-proto'))
            and _loopback(request.url.hostname or '')
            and request.client is not None and _loopback(request.client.host))


def configure_auth(app: FastAPI, db_path_getter: Callable[[], str]) -> None:
    @contextmanager
    def database():
        conn = sqlite3.connect(db_path_getter(), timeout=15)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def error(code, message):
        return JSONResponse({'detail': message}, status_code=code, headers={'Cache-Control': 'no-store'})

    def session(request):
        token = request.cookies.get(COOKIE, '')
        if readonly() or not token or not _secure_transport(request):
            return None
        with database() as conn:
            return conn.execute('SELECT * FROM admin_sessions WHERE token_hash=? AND expires_at>?',
                                (hashlib.sha256(token.encode()).hexdigest(), time.time())).fetchone()

    def origin_ok(request):
        origin = request.headers.get('origin')
        # Explicit configured origins, never trust a client-supplied Host header.
        return origin is None or origin.rstrip('/') in allowed_origins()

    def payload(record=None):
        return {'authenticated': record is not None, 'admin_enabled': not readonly(),
                'readonly': readonly(), 'csrf_token': record['csrf_token'] if record else None,
                'username': record['username'] if record else None}

    @app.middleware('http')
    async def protect_management(request: Request, call_next):
        path = request.url.path.rstrip('/')
        safe = request.method in ('GET', 'HEAD', 'OPTIONS')
        private = (path in ('/api/admin', '/api/failures', '/api/sync/history', '/api/export/email.xlsx')
                   or path.startswith('/api/failures/') or path.startswith('/api/admin/'))
        if request.method == 'OPTIONS':
            return await call_next(request)
        if path.startswith('/api/') and (not safe or private) and path != '/api/auth/login':
            if readonly():
                return error(403, '公网只读模式：管理功能已关闭')
            record = await run_in_threadpool(session, request)
            if record is None:
                return error(401, '请先以管理员身份登录')
            if not safe and (not origin_ok(request) or not hmac.compare_digest(
                    request.headers.get('x-csrf-token', ''), record['csrf_token'])):
                return error(403, '请求校验失败，请刷新后重试')
        return await call_next(request)

    @app.get('/api/auth/session')
    def get_session(request: Request):
        return JSONResponse(payload(session(request)), headers={'Cache-Control': 'no-store'})

    @app.post('/api/auth/login')
    async def login(request: Request):
        if readonly():
            return error(403, '公网只读模式：登录已关闭')
        if not _secure_transport(request) or not origin_ok(request):
            return error(403, '管理员登录需要 HTTPS 或显式启用的本机开发环境')
        try:
            values = await request.json()
            username, password = values.get('username', ''), values.get('password', '')
            if not isinstance(username, str) or not isinstance(password, str) or len(username) > 100 or len(password) > 512:
                return error(400, '无效登录请求')
        except (ValueError, AttributeError):
            return error(400, '无效登录请求')
        return await run_in_threadpool(authenticate, request, username, password)

    def authenticate(request, username, password):
        now = time.time()
        client = request.client.host if request.client else 'unknown'
        with database() as conn:
            # Serialize the attempt check so simultaneous requests cannot bypass the limit.
            conn.execute('BEGIN IMMEDIATE')
            conn.execute('DELETE FROM admin_login_attempts WHERE attempted_at<?', (now - 900,))
            conn.execute('DELETE FROM admin_sessions WHERE expires_at<=?', (now,))
            if conn.execute('SELECT COUNT(*) FROM admin_login_attempts WHERE client=?', (client,)).fetchone()[0] >= 10:
                return error(429, '尝试次数过多，请 15 分钟后重试')
            user = conn.execute('SELECT password_hash FROM admin_users WHERE username=?', (username,)).fetchone()
            stored = user[0] if user else _password_hash('dummy-unmatched-password', '00' * 16)
            try:
                valid = hmac.compare_digest(_password_hash(password, stored.split('$')[1]), stored)
            except (ValueError, IndexError):
                valid = False
            if not user or not valid:
                conn.execute('INSERT INTO admin_login_attempts VALUES (?, ?)', (client, now))
                return error(401, '用户名或密码不正确')
            token, csrf = secrets.token_urlsafe(32), secrets.token_urlsafe(32)
            # Re-login rotates any session presented by this browser.
            old = request.cookies.get(COOKIE, '')
            conn.execute('DELETE FROM admin_sessions WHERE token_hash=?', (hashlib.sha256(old.encode()).hexdigest(),))
            conn.execute('INSERT INTO admin_sessions VALUES (?, ?, ?, ?)',
                         (hashlib.sha256(token.encode()).hexdigest(), username, csrf, now + SESSION_SECONDS))
        response = JSONResponse(payload({'username': username, 'csrf_token': csrf}), headers={'Cache-Control': 'no-store'})
        response.set_cookie(COOKIE, token, max_age=SESSION_SECONDS, httponly=True,
                            secure=request.url.scheme == 'https', samesite='strict', path='/')
        return response

    @app.post('/api/auth/logout')
    def logout(request: Request):
        token = request.cookies.get(COOKIE, '')
        with database() as conn:
            conn.execute('DELETE FROM admin_sessions WHERE token_hash=?', (hashlib.sha256(token.encode()).hexdigest(),))
        response = JSONResponse(payload(), headers={'Cache-Control': 'no-store'})
        response.delete_cookie(COOKIE, path='/', httponly=True, samesite='strict', secure=request.url.scheme == 'https')
        return response


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Create/reset a local administrator without storing a plaintext password')
    parser.add_argument('--db', required=True)
    parser.add_argument('--username', required=True)
    args = parser.parse_args()
    password = getpass.getpass('Administrator password (12+ characters): ')
    if password != getpass.getpass('Confirm password: '):
        raise SystemExit('Passwords do not match')
    with sqlite3.connect(args.db) as conn:
        initialize_auth(conn)
        create_admin(conn, args.username, password)
    print('Administrator saved; old sessions revoked.')
