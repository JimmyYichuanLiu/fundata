import { formatTimestamp } from '../utils/display.js'
import { useEffect, useState } from 'react'
import { Link } from 'react-router-dom'
import { useAuth } from '../context/AuthContext.jsx'
import { fetchSyncStatus, fetchSyncHistory, fetchFailures, triggerSync, retryFailure, downloadEmailExport } from '../api.js'
import PageState from '../components/PageState.jsx'

export default function AdminPage() {
  const auth = useAuth()
  const [username, setUsername] = useState(''), [password, setPassword] = useState('')
  const [status, setStatus] = useState({}), [history, setHistory] = useState([]), [failures, setFailures] = useState([])
  const [error, setError] = useState(''), [message, setMessage] = useState(''), [busy, setBusy] = useState(false), [version, setVersion] = useState(0)
  useEffect(() => {
    if (!auth.canManage) return
    const c = new AbortController()
    Promise.all([fetchSyncStatus(c.signal), fetchSyncHistory(c.signal), fetchFailures(c.signal)])
      .then(([s, h, f]) => { setStatus(s); setHistory(h.items || []); setFailures(f.items || []) })
      .catch(err => { if (err.name !== 'AbortError') setError(err.message) })
    return () => c.abort()
  }, [auth.canManage, version])
  useEffect(() => {
    if (!auth.canManage || !(status.is_running || status.last_status === 'running')) return
    const timer = setInterval(() => setVersion(v => v + 1), 5000)
    return () => clearInterval(timer)
  }, [auth.canManage, status.is_running, status.last_status])
  const action = async fn => { setBusy(true); setError(''); setMessage(''); try { await fn(); setVersion(v => v + 1) } catch (err) { setError(err.message) } finally { setBusy(false) } }
  if (auth.loading) return <PageState loading />
  if (auth.sessionError) return <PageState error={auth.sessionError} onRetry={auth.retrySession} />
  if (!auth.admin_enabled || auth.readonly) return <PageState title="公开只读看板">此站点未开放管理功能。<Link className="text-primary" to="/"> 返回基金概览</Link></PageState>
  if (!auth.canManage) return <div className="page-wrap"><div className="page-heading"><div><div className="eyebrow">ADMIN / 数据管理</div><h1>管理员登录</h1><p>登录后可管理同步、异常数据与净值记录。</p></div></div><form className="panel max-w-md p-6 space-y-5" onSubmit={e => { e.preventDefault(); action(() => auth.login(username, password)).finally(() => setPassword('')) }}><label className="block"><span className="field-label">用户名</span><input className="control w-full" required autoComplete="username" value={username} onChange={e => setUsername(e.target.value)} /></label><label className="block"><span className="field-label">密码</span><input className="control w-full" required type="password" autoComplete="current-password" value={password} onChange={e => setPassword(e.target.value)} /></label>{error && <div className="notice notice-error" role="alert">{error}</div>}<button className="button-primary w-full" disabled={busy}>{busy ? '正在登录…' : '登录工作空间'}</button></form></div>
  return <div className="page-wrap"><div className="page-heading"><div><div className="eyebrow">DATA OPERATIONS / 数据管理</div><h1>数据工作空间</h1><p>核对更新状态，重试失败附件，导出有效基金净值。</p></div><button className="button-secondary" disabled={busy} onClick={() => action(auth.logout)}>退出登录</button></div>
    {error && <div className="notice notice-error" role="alert">{error}</div>}{message && <div className="notice" role="status">{message}</div>}
    <div className="metric-grid"><div className="metric-card"><div className="metric-label">最后尝试</div><strong className="!text-base">{formatTimestamp(status.last_attempt_time)}</strong></div><div className="metric-card"><div className="metric-label">最后成功</div><strong className="!text-base">{formatTimestamp(status.last_success_time)}</strong></div><div className="metric-card"><div className="metric-label">邮件净值截止</div><strong className="!text-base">{status.latest_nav_date || '—'}</strong></div><div className="metric-card"><div className="metric-label">下次计划</div><strong className="!text-base">{status.next_scheduled_at ? formatTimestamp(status.next_scheduled_at) : '未配置'}</strong></div></div>
    <div className="toolbar panel mb-5"><button className="button-primary" disabled={busy || status.is_running} onClick={() => action(async () => { await triggerSync(); setStatus(previous => ({ ...previous, is_running: true })); setMessage('邮件同步已启动，状态将在此自动更新。') })}>{status.is_running ? '同步进行中…' : '增量同步邮件'}</button><button className="button-secondary" disabled={busy} onClick={() => action(downloadEmailExport)}>导出邮件净值 Excel</button><Link className="button-secondary" to="/">管理基金与净值</Link><button className="button-secondary ml-auto" onClick={() => setVersion(v => v + 1)}>刷新状态</button></div>
    <section className="panel mb-5"><div className="panel-heading"><h2>同步历史</h2><small className="text-slate-500">时间均为北京时间</small><span className="badge">最近运行</span></div>{!history.length ? <PageState title="暂无运行记录" /> : <div className="table-scroll"><table className="research-table"><thead><tr><th>开始时间</th><th>结束时间</th><th>状态</th><th>处理邮件</th><th>新增净值</th><th>重复</th><th>失败</th></tr></thead><tbody>{history.map((run, i) => <tr key={run.id || i}><td>{formatTimestamp(run.started_at)}</td><td>{formatTimestamp(run.ended_at || run.finished_at)}</td><td>{({ success: '成功', partial_success: '部分成功', error: '失败', running: '进行中', interrupted: '中断' })[run.status] || run.status}</td><td>{run.processed ?? run.processed_emails ?? run.processed_count ?? '—'}</td><td>{run.added ?? run.added_records ?? run.added_count ?? '—'}</td><td>{run.duplicates ?? run.duplicate_records ?? run.duplicate_count ?? '—'}</td><td>{run.failed ?? run.failed_count ?? run.failure_count ?? '—'}</td></tr>)}</tbody></table></div>}</section>
    <section className="panel"><div className="panel-heading"><h2>附件处理异常</h2><span className="badge">{failures.length} 条</span></div>{!failures.length ? <PageState title="暂无待检查的附件异常" /> : <div className="table-scroll"><table className="research-table"><thead><tr><th>附件</th><th>邮件 UID / 工作表</th><th>失败原因</th><th>状态</th><th>操作</th></tr></thead><tbody>{failures.map((failure, i) => <tr key={failure.id || i}><td className="!whitespace-normal max-w-xs">{failure['附件文件名'] || failure['附件名称'] || failure.attachment_name || '—'}<details className="mt-2 text-xs text-slate-500"><summary className="cursor-pointer">邮件详情</summary><p className="mt-2">主题：{failure['邮件主题'] || '—'}<br />发件人：{failure['邮件发件人'] || '—'}<br />邮件日期：{failure['邮件日期'] || '—'}</p></details></td><td>{failure.mailbox_uid || '无法定位'} / {failure.sheet_name || failure['sheet名称'] || failure['工作表'] || '—'}</td><td className="!whitespace-normal max-w-sm">{failure['失败原因'] || failure.error_message || failure.retry_reason || '—'}</td><td>{failure.status || '待检查'}</td><td><button className="button-secondary" disabled={busy || !failure.retryable} title={failure.retry_reason || ''} onClick={() => action(async () => { await retryFailure(failure.id); setStatus(previous => ({ ...previous, is_running: true })); setMessage('重试请求已处理，请核对异常状态与入库结果。') })}>重试附件</button>{!failure.retryable && <small className="block mt-2 text-slate-500">{failure.retry_reason || '历史定位信息缺失，无法重试'}</small>}</td></tr>)}</tbody></table></div>}</section>
  </div>
}
