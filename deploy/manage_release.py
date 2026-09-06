#!/usr/bin/env python3
"""Explicit, in-repository Linux/systemd deployment and recovery tooling.

Preflight is read-only. Other subcommands are operator-invoked mutations.
No shell commands, server paths, deployment revisions or credentials are guessed.
"""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import shlex
import sqlite3
import subprocess
import sys
import time
from datetime import datetime, timezone
from urllib.request import urlopen
from urllib.request import Request
from urllib.error import HTTPError
from urllib.parse import urlsplit


class ReleaseError(RuntimeError):
    pass


def contained(project, value, *, exists=True):
    path = Path(value)
    path = (project / path).resolve() if not path.is_absolute() else path.resolve()
    if path == project or project not in path.parents:
        raise ReleaseError('Path must be strictly inside the selected project')
    if exists and not path.exists():
        raise ReleaseError(f'Required path does not exist: {path}')
    return path


def exact_commit(value):
    if not re.fullmatch(r'[0-9a-f]{40}',value):
        raise ReleaseError('--commit must be a full lowercase 40-character Git commit')
    return value


def run(args, project, *, check=True):
    result = subprocess.run([str(a) for a in args],cwd=project,check=False,
                            capture_output=True,text=True)
    if check and result.returncode:
        raise ReleaseError(f'{args[0]} exited {result.returncode}: {result.stderr.strip()}')
    return result.stdout.strip()


def fingerprint(path):
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda:stream.read(1024*1024),b''):
            digest.update(chunk)
    return digest.hexdigest()


def backup_database(db, destination):
    if destination.exists():
        raise ReleaseError('Backup destination already exists')
    with sqlite3.connect(db.as_uri()+'?mode=ro',uri=True) as source:
        with sqlite3.connect(destination) as target:
            source.backup(target)
            if target.execute('PRAGMA quick_check').fetchone()[0] != 'ok':
                raise ReleaseError('Backup integrity check failed')
    return fingerprint(destination)


def restore_database(backup, db):
    """Caller must stop the service and back up current DB first."""
    with sqlite3.connect(backup.as_uri()+'?mode=ro',uri=True) as source:
        if source.execute('PRAGMA quick_check').fetchone()[0] != 'ok':
            raise ReleaseError('Rollback backup integrity check failed')
        with sqlite3.connect(db) as target:
            target.execute('PRAGMA wal_checkpoint(TRUNCATE)')
            source.backup(target)
            if target.execute('PRAGMA quick_check').fetchone()[0] != 'ok':
                raise ReleaseError('Restored database integrity check failed')


def environment_config(project, service_properties):
    env_file = contained(project,'.env')
    files = service_properties.get('EnvironmentFiles','')
    if re.findall(r'(/[^ ;()]+)',files) != [str(env_file)]:
        raise ReleaseError('Service must explicitly load the selected project .env file')
    values = {}
    for line in env_file.read_text(encoding='utf-8').splitlines():
        line = line.strip()
        if line and not line.startswith('#') and '=' in line:
            key,value = line.split('=',1)
            values[key.strip()] = value.strip().strip('\"\'')
    for entry in shlex.split(service_properties.get('Environment','')):
        if '=' in entry:
            key,value = entry.split('=',1)
            values[key] = value
    # systemd EnvironmentFile overrides Environment entries.
    for line in env_file.read_text(encoding='utf-8').splitlines():
        line = line.strip()
        if line and not line.startswith('#') and '=' in line:
            key,value = line.split('=',1)
            values[key.strip()] = value.strip().strip('\"\'')
    return values


def health(url):
    try:
        with urlopen(url,timeout=10) as response:
            body = json.loads(response.read())
            if response.status != 200 or body.get('status') not in ('ok','healthy'):
                raise ReleaseError('API health is not healthy')
            return body
    except Exception as exc:
        return {'status':'unavailable','reason':type(exc).__name__}


def inspect(args, *, require_head=False):
    project = Path(args.project).resolve(strict=True)
    if not project.is_dir() or project.parent == project:
        raise ReleaseError('A concrete project directory is required')
    db = contained(project,args.db)
    if not db.is_file():
        raise ReleaseError('Database must be a file')
    if not re.fullmatch(r'[A-Za-z0-9_.@-]+\.service',args.service):
        raise ReleaseError('--service must name one systemd .service unit')
    sha = exact_commit(args.commit)
    top = Path(run(['git','rev-parse','--show-toplevel'],project)).resolve()
    if top != project:
        raise ReleaseError('--project must be the Git repository root')
    if run(['git','status','--porcelain'],project):
        raise ReleaseError('Worktree must be clean, including untracked files; preserve them before release')
    if run(['git','cat-file','-t',sha],project) != 'commit':
        raise ReleaseError('Requested revision is not a locally available commit')
    current = run(['git','rev-parse','HEAD'],project)
    if require_head and current != sha:
        raise ReleaseError('Checked-out revision does not equal --commit')
    props_text = run(['systemctl','show',args.service,'--property=LoadState,ActiveState,WorkingDirectory,ExecStart,EnvironmentFiles,Environment,MainPID'],project)
    props = dict(line.split('=',1) for line in props_text.splitlines() if '=' in line)
    if props.get('LoadState') != 'loaded':
        raise ReleaseError('Systemd service is not loaded')
    if Path(props.get('WorkingDirectory','')).resolve() != project:
        raise ReleaseError('Service working directory does not match selected project')
    command = props.get('ExecStart','')
    if re.findall(r'--workers(?:=|\s+)(\d+)',command) != ['1']:
        raise ReleaseError('Service must explicitly run exactly --workers 1')
    if not re.search(r'--host(?:=|\s+)127\.0\.0\.1(?:\s|;|\})',command):
        raise ReleaseError('API must bind 127.0.0.1 behind nginx')
    config = environment_config(project,props)
    if config.get('FUNDATA_READONLY') != '1':
        raise ReleaseError('Public HTTP deployment requires explicit FUNDATA_READONLY=1')
    if config.get('FUNDATA_COOKIE_SECURE','1') != '1':
        raise ReleaseError('Public deployment requires FUNDATA_COOKIE_SECURE=1')
    if config.get('FUNDATA_SCHEDULER_ENABLED','1') != '1':
        raise ReleaseError('Public deployment requires the scheduler enabled')
    configured_db = contained(project,config.get('DB_PATH','fund_data.db'))
    if configured_db != db:
        raise ReleaseError('--db does not match the service DB_PATH')
    with sqlite3.connect(db.as_uri()+'?mode=ro',uri=True) as conn:
        integrity = conn.execute('PRAGMA quick_check').fetchone()[0]
    if integrity != 'ok':
        raise ReleaseError('Database integrity check failed')
    report = dict(project=str(project),database=str(db),service=args.service,
                  current_commit=current,target_commit=sha,service_state=props.get('ActiveState'),
                  readonly=True,workers=1,database_check=integrity,health=health(args.health_url))
    return project,db,report


def stop_service(args, project):
    run(['systemctl','stop',args.service],project)
    state = run(['systemctl','show',args.service,'--property=ActiveState','--value'],project)
    if state not in ('inactive','failed'):
        raise ReleaseError('Service did not stop; no database migration/restore was performed')


def load_store(project):
    spec = importlib.util.spec_from_file_location('release_fund_store',contained(project,'fund_store.py'))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def build(project, python):
    # A venv Python is commonly a symlink to /usr/bin/python. Its invocation
    # must be inside the venv, while the actual system interpreter may be outside.
    executable = Path(python)
    executable = project / executable if not executable.is_absolute() else executable
    contained(project,executable.parent)
    if not executable.is_file():
        raise ReleaseError('Project virtual-environment Python does not exist')
    env = os.environ.copy()
    env['PIP_CACHE_DIR'] = str(contained(project,'.cache/pip',exists=False))
    env['npm_config_cache'] = str(contained(project,'.cache/npm',exists=False))
    for argv,cwd in [([str(executable),'-m','pip','install','-r','requirements.txt'],project),
                     (['npm','ci'],project/'web'),(['npm','run','build'],project/'web')]:
        completed = subprocess.run(argv,cwd=cwd,env=env,check=False)
        if completed.returncode:
            raise ReleaseError(f'Build failed: {argv[0]} (service remains stopped)')


def restart_and_check(args,project):
    run(['systemctl','start',args.service],project)
    for _ in range(12):
        result = health(args.health_url)
        if result.get('status') in ('ok','healthy'):
            try:
                verify_readonly_endpoints(args.health_url)
            except ReleaseError:
                stop_service(args, project)
                raise
            return result
        time.sleep(2)
    raise ReleaseError('New service did not become healthy; use recorded rollback manifest')


def require_protected_revision(project, commit):
    source = run(['git', 'show', f'{commit}:api.py'], project)
    if 'configure_auth(app' not in source:
        raise ReleaseError('Refusing automatic deployment/rollback to an unprotected API revision. Establish a separately maintained nginx readonly gate before any manual legacy recovery.')
    run(['git', 'cat-file', '-e', f'{commit}:admin_auth.py'], project)


def verify_readonly_endpoints(health_url):
    parsed = urlsplit(health_url)
    base = f'{parsed.scheme}://{parsed.netloc}'
    for path, method in [('/api/nav','POST'), ('/api/auth/login','POST'), ('/api/failures','GET')]:
        request = Request(base + path, data=b'{}' if method == 'POST' else None,
                          headers={'Content-Type':'application/json'}, method=method)
        try:
            with urlopen(request, timeout=10) as response:
                code = response.status
        except HTTPError as exc:
            code = exc.code
        except OSError as exc:
            raise ReleaseError(f'Readonly verification unavailable: {path}; service stopped') from exc
        if code != 403:
            raise ReleaseError(f'Readonly verification failed: {method} {path} returned {code}; service stopped')


def execute(args):
    project,db,report = inspect(args,require_head=args.action=='migrate')
    print(json.dumps(report,ensure_ascii=False,indent=2))
    if args.action == 'preflight':
        return
    require_protected_revision(project, args.commit)
    backup_dir = contained(project,'backups',exists=False)
    backup_dir.mkdir(exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%f')
    if args.action == 'rollback':
        manifest_path = contained(project,args.manifest)
        manifest = json.loads(manifest_path.read_text(encoding='utf-8'))
        if (manifest['project'],manifest['database'],manifest['service']) != (str(project),str(db),args.service):
            raise ReleaseError('Rollback manifest does not match the selected deployment')
        if args.commit != manifest['previous_commit']:
            raise ReleaseError('--commit must equal manifest previous_commit for rollback')
        backup = contained(project,manifest['backup'])
        if backup_dir not in backup.parents or fingerprint(backup) != manifest['backup_sha256']:
            raise ReleaseError('Backup location or fingerprint differs from release manifest')
        stop_service(args,project)
        recovery = backup_dir/f'{db.stem}-before-rollback-{stamp}.db'
        backup_database(db,recovery)
        print(f'Current database preserved: {recovery}')
        run(['git','checkout','--detach',args.commit],project)
        build(project,args.python)
        restore_database(backup,db)
        print(json.dumps(restart_and_check(args,project),ensure_ascii=False))
        return
    stop_service(args,project)
    backup = backup_dir/f'{db.stem}-release-{stamp}.db'
    manifest = dict(project=str(project),database=str(db),service=args.service,previous_commit=report['current_commit'],
                    target_commit=args.commit,backup=str(backup),backup_sha256=backup_database(db,backup),created_at=stamp)
    manifest_path = backup_dir/f'release-{stamp}.json'
    manifest_path.write_text(json.dumps(manifest,ensure_ascii=False,indent=2),encoding='utf-8')
    print(f'Rollback manifest: {manifest_path}')
    if args.action == 'release':
        run(['git','checkout','--detach',args.commit],project)
        build(project,args.python)
    conn = load_store(project).initialize_database(db,backup=True)
    conn.close()
    if args.action == 'release':
        print(json.dumps(restart_and_check(args,project),ensure_ascii=False))
    else:
        print('Migration complete. Service remains stopped for inspection.')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action',choices=('preflight','migrate','release','rollback'))
    for name in ('project','db','service','commit'):
        parser.add_argument('--'+name,required=True)
    parser.add_argument('--python',default='venv/bin/python',help='Python interpreter inside selected project')
    parser.add_argument('--health-url',default='http://127.0.0.1:8000/api/health')
    parser.add_argument('--manifest',help='Exact release manifest required for rollback')
    args = parser.parse_args()
    if args.action == 'rollback' and not args.manifest:
        parser.error('rollback requires --manifest')
    try:
        execute(args)
    except (ReleaseError,OSError,sqlite3.Error) as exc:
        print(f'Deployment stopped: {exc}',file=sys.stderr)
        raise SystemExit(1) from exc


if __name__ == '__main__':
    main()
