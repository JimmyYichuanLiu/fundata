"""One observable, restart-safe email sync entry for CLI/API/scheduler."""
import email
import imaplib
import json
import threading
from datetime import datetime, timedelta, timezone

from fund_store import initialize_database, recalculate_adj_nav

_lock = threading.Lock()


class SyncError(RuntimeError):
    def __init__(self, message, result=None):
        super().__init__(message)
        self.result = result


def _now():
    return datetime.now(timezone.utc).isoformat()


def _state(conn, key, value):
    conn.execute('INSERT OR REPLACE INTO sync_state(key,value) VALUES(?,?)', (key,str(value)))


def get_sync_summary(conn):
    state = dict(conn.execute('SELECT key,value FROM sync_state'))
    try:
        last_added = int(state.get('sync_last_added') or 0)
    except (TypeError, ValueError):
        last_added = 0
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
    active = bool(conn.execute("SELECT 1 FROM sync_runs WHERE status='running' AND COALESCE(heartbeat_at,started_at)>?", (cutoff,)).fetchone())
    last_status = state.get('sync_last_status', 'never')
    if active:
        last_status = 'running'
    elif last_status == 'running':
        last_status = 'interrupted'
    return {'last_sync_time':state.get('sync_last_time'), 'last_status':last_status,
            'last_added':last_added, 'last_error':state.get('sync_last_error',''),
            'last_attempt_time':state.get('sync_last_time'),'last_success_time':state.get('sync_last_success_time'),
            'latest_nav_date':conn.execute("SELECT MAX(nav_date) FROM valid_fund_nav WHERE data_source='email'").fetchone()[0],
            'is_running':active}


def _require_ok(response, stage):
    status, data = response
    if status != 'OK':
        raise RuntimeError(f'{stage} failed ({status})')
    return data


def _log_failure(conn, msg, failure, uid, validity):
    conn.execute('''INSERT INTO extraction_failures
        (邮件主题,邮件发件人,邮件日期,附件文件名,sheet名称,失败原因,mailbox_uid,uidvalidity,raw_json)
        VALUES(?,?,?,?,?,?,?,?,?)''',
        (msg.get('Subject'),msg.get('From'),msg.get('Date'),failure.get('filename',''),failure.get('sheet_name',''),
         failure.get('reason','unknown'),uid,validity,json.dumps(failure,ensure_ascii=False,default=str)))


def run_email_sync(email_user, email_password, db_path, *, trigger='manual', retry_failure_id=None,
                   retry_uid=None, imap_factory=None):
    """Sync INBOX incrementally, or retry exactly one UID without moving checkpoint.

    Connection/login/search/fetch failures raise SyncError with a persisted result.
    Recognized attachment failures produce partial_success with replay coordinates.
    A stale run is recoverable after a two-hour lease; single-process overlap is
    rejected immediately, and a DB transaction claims the lease across processes.
    """
    if not _lock.acquire(blocking=False):
        raise SyncError('Email synchronization is already running')
    conn = None
    client = None
    result = None
    counts_before_message = None
    try:
        conn = initialize_database(db_path)
        retry = None
        if retry_failure_id is not None:
            cur = conn.execute('SELECT * FROM extraction_failures WHERE id=?',(retry_failure_id,))
            row = cur.fetchone()
            if not row:
                raise SyncError('Failure record does not exist')
            retry = dict(zip([d[0] for d in cur.description],row))
            if not retry['mailbox_uid'] or not retry['uidvalidity']:
                raise SyncError('Historical record has no mailbox UID/UIDVALIDITY; automatic retry is unavailable')
            if retry['status'] == 'resolved':
                raise SyncError('Failure already resolved')
            retry_uid = retry['mailbox_uid']
        if retry_uid is not None and (isinstance(retry_uid,bool) or int(retry_uid) < 1):
            raise SyncError('Retry UID must be positive')
        conn.execute('BEGIN IMMEDIATE')
        cutoff = (datetime.now(timezone.utc)-timedelta(hours=2)).isoformat()
        if conn.execute("SELECT 1 FROM sync_runs WHERE status='running' AND COALESCE(heartbeat_at,started_at)>?",(cutoff,)).fetchone():
            conn.rollback()
            raise SyncError('Email synchronization is already running')
        conn.execute("UPDATE sync_runs SET status='error',ended_at=?,error='Interrupted run; lease expired' WHERE status='running'",(_now(),))
        started = _now()
        run_id = conn.execute("INSERT INTO sync_runs(trigger,started_at,heartbeat_at,status) VALUES(?,?,?,'running')",(trigger,started,started)).lastrowid
        result = dict(id=run_id,status='running',started_at=started,ended_at=None,processed=0,added=0,duplicates=0,failed=0,error=None)
        _state(conn,'sync_last_time',started)
        _state(conn,'sync_last_status','running')
        _state(conn,'sync_last_error','')
        if retry:
            conn.execute('UPDATE extraction_failures SET retry_count=retry_count+1,last_retry_at=? WHERE id=?',(started,retry_failure_id))
        conn.commit()
        if not email_user or not email_password:
            raise RuntimeError('Email credentials are not configured')
        client = (imap_factory or imaplib.IMAP4_SSL)('imap.163.com',993,timeout=60)
        _require_ok(client.login(email_user,email_password),'IMAP login')
        if 'ID' not in imaplib.Commands:
            imaplib.Commands['ID'] = ('AUTH',)
        _require_ok(client._simple_command('ID','("name" "FundTrack" "version" "2.0")'),'IMAP identification')
        _require_ok(client.select('INBOX',readonly=True),'IMAP select')
        validity_data = client.untagged_responses.get('UIDVALIDITY')
        if not validity_data or not validity_data[0]:
            raise RuntimeError('Mailbox UIDVALIDITY is missing')
        validity = validity_data[0].decode() if isinstance(validity_data[0],bytes) else str(validity_data[0])
        state = dict(conn.execute('SELECT key,value FROM sync_state'))
        last_uid = int(state.get('last_uid','0')) if state.get('uidvalidity') == validity else 0
        if retry:
            if retry['uidvalidity'] != validity:
                raise RuntimeError('Mailbox UIDVALIDITY changed; old failure cannot be safely replayed')
        if retry_uid is not None:
            uids = [int(retry_uid)]
        else:
            data = _require_ok(client.uid('search',None,'ALL' if not last_uid else f'UID {last_uid+1}:*'),'IMAP search')
            if not data or data[0] is None:
                raise RuntimeError('IMAP search returned malformed data')
            uids = sorted({int(v) for v in data[0].split() if int(v)>last_uid})
        from get_163_email import extract_excel_attachments, insert_data_to_db, insert_email_source
        for uid in uids:
            affected = set()
            data = _require_ok(client.uid('fetch',str(uid),'(BODY.PEEK[])'),'IMAP fetch')
            payload = next((p[1] for p in data if isinstance(p,tuple) and isinstance(p[1],bytes)),None)
            if payload is None:
                raise RuntimeError(f'Mailbox UID {uid} has no message body')
            msg = email.message_from_bytes(payload)
            failures = []
            frames, _ = extract_excel_attachments(msg,failures)
            counts_before_message = {key:result[key] for key in ('added','duplicates','failed','processed')}
            conn.execute('BEGIN IMMEDIATE')
            found_retry_target = retry is None
            if retry and retry['附件文件名']:
                frames = [f for f in frames if f['filename']==retry['附件文件名'] and
                          (not retry['sheet名称'] or f['sheet_name']==retry['sheet名称'])]
                failures = [f for f in failures if f['filename']==retry['附件文件名'] and
                            (not retry['sheet名称'] or f['sheet_name']==retry['sheet名称'])]
                found_retry_target = bool(frames or failures)
            if not found_retry_target:
                failures.append({'filename':retry['附件文件名'],'sheet_name':retry['sheet名称'],'reason':'Replay target attachment or worksheet no longer found'})
            for frame in frames:
                # Reuse source identity on retries instead of creating empty duplicates.
                existing = conn.execute('''SELECT id FROM email_sources WHERE mailbox_uid=? AND uidvalidity=?
                    AND 附件文件名=? AND sheet名称=? LIMIT 1''',(uid,validity,frame['filename'],frame['sheet_name'])).fetchone()
                source = existing[0] if existing else insert_email_source(conn,msg.get('Subject'),msg.get('From'),msg.get('Date'),frame['filename'],frame['sheet_name'])
                conn.execute('UPDATE email_sources SET mailbox_uid=?,uidvalidity=? WHERE id=?',(uid,validity,source))
                insertion_failures = []
                added,skipped = insert_data_to_db(conn,frame['data'],insertion_failures,source)
                result['added'] += added
                result['duplicates'] += max(0,skipped-len(insertion_failures))
                for failure in insertion_failures:
                    failures.append(dict(failure,filename=frame['filename'],sheet_name=frame['sheet_name']))
                if '产品代码' in frame['data']:
                    affected.update(str(v) for v in frame['data']['产品代码'].dropna())
            for failure in failures:
                if retry:
                    conn.execute('UPDATE extraction_failures SET 失败原因=?,raw_json=? WHERE id=?',
                                 (failure['reason'],json.dumps(failure,ensure_ascii=False,default=str),retry_failure_id))
                else:
                    _log_failure(conn,msg,failure,uid,validity)
            result['failed'] += len(failures)
            result['processed'] += 1
            if retry and not failures:
                conn.execute("UPDATE extraction_failures SET status='resolved',resolved_at=? WHERE id=?",(_now(),retry_failure_id))
            if retry_uid is None:
                _state(conn,'last_uid',uid)
                _state(conn,'uidvalidity',validity)
            for code in affected:
                recalculate_adj_nav(conn,code)
            conn.execute('UPDATE sync_runs SET processed=?,added=?,duplicates=?,failed=?,heartbeat_at=? WHERE id=?',
                         (result['processed'],result['added'],result['duplicates'],result['failed'],_now(),run_id))
            conn.commit()  # durable per-message checkpoint; failed fetch does not skip a UID
            counts_before_message = None
        if retry_uid is None:
            _state(conn,'uidvalidity',validity)
            if not uids:
                _state(conn,'last_uid',last_uid)
        result['status'] = 'partial_success' if result['failed'] else 'success'
        if result['status'] == 'success':
            _state(conn,'sync_last_success_time',_now())
        return result
    except Exception as exc:
        if conn is not None:
            conn.rollback()
        if result is not None:
            if counts_before_message is not None:
                result.update(counts_before_message)
            result['status'] = 'error'
            # Private diagnostics never include configured credentials.
            error = str(exc)
            for secret in (email_password,email_user):
                if secret:
                    error = error.replace(secret,'[redacted]')
            result['error'] = error
        if isinstance(exc,SyncError):
            raise
        raise SyncError(result['error'] if result else str(exc),result) from exc
    finally:
        try:
            if conn is not None and result is not None:
                result['ended_at'] = _now()
                conn.execute('''UPDATE sync_runs SET ended_at=?,status=?,processed=?,added=?,duplicates=?,failed=?,error=? WHERE id=?''',
                             tuple(result[k] for k in ('ended_at','status','processed','added','duplicates','failed','error','id')))
                _state(conn,'sync_last_status',result['status'])
                _state(conn,'sync_last_added',result['added'])
                _state(conn,'sync_last_error',result['error'] or '')
                conn.commit()
        finally:
            if conn is not None:
                conn.close()
            if client is not None:
                try:
                    client.logout()
                except Exception:
                    pass  # Cleanup cannot change the persisted processing outcome.
            _lock.release()
