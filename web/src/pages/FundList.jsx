import { formatTimestamp } from '../utils/display.js'
import { useEffect, useMemo, useState } from 'react'
import { Link } from 'react-router-dom'
import { fetchFunds, fetchStats, fetchSyncStatus, fetchFundReturns, fetchFundMetrics } from '../api.js'
import { useCompare } from '../context/CompareContext.jsx'
import { useAuth } from '../context/AuthContext.jsx'
import Icon from '../components/Icon.jsx'
import PageState from '../components/PageState.jsx'

const SOURCES = { all: '所有来源', email: '邮件采集', zx_excel: 'ZX 数据', manual: '手动录入' }
const COLUMNS = [
  ['1w', '近一周'], ['1m', '近一月'], ['3m', '近三月'], ['6m', '近六月'], ['1y', '近一年'], ['ytd', '今年以来'],
  ['annualized_return', '年化收益'], ['max_drawdown', '最大回撤'], ['annualized_vol', '年化波动'], ['sharpe', '夏普比率'], ['monthly_win_rate', '月胜率'],
]
const STATUS = { success: '同步完成', running: '同步进行中', error: '同步失败', partial_success: '部分成功', partial_error: '部分成功', interrupted: '同步中断' }
function number(value, digits = 2) { return Number.isFinite(value) ? value.toFixed(digits) : '—' }
function Percent({ value, ratio = false }) { return <span className={value > 0 ? 'positive' : value < 0 ? 'negative' : ''}>{value == null || !Number.isFinite(value) ? '—' : `${value > 0 ? '+' : ''}${number(value, ratio ? 3 : 2)}${ratio ? '' : '%'}`}</span> }
function Sparkline({ values }) {
  const points = (values || []).filter(Number.isFinite)
  if (points.length < 2) return <span>—</span>
  const low = Math.min(...points), range = Math.max(...points) - low || 1
  return <svg width="78" height="28" viewBox="0 0 78 28" aria-label="近期净值走势"><polyline points={points.map((v, i) => `${i / (points.length - 1) * 78},${25 - (v - low) / range * 22}`).join(' ')} fill="none" stroke={points.at(-1) >= points[0] ? '#c9454c' : '#188272'} strokeWidth="1.5" /></svg>
}
export default function FundList() {
  const [funds, setFunds] = useState([]), [stats, setStats] = useState({}), [sync, setSync] = useState({})
  const [returns, setReturns] = useState({}), [metrics, setMetrics] = useState({})
  const [source, setSource] = useState('all'), [strategy, setStrategy] = useState(''), [strategy2, setStrategy2] = useState(''), [search, setSearch] = useState('')
  const [loading, setLoading] = useState(true), [error, setError] = useState(''), [notice, setNotice] = useState(''), [version, setVersion] = useState(0)
  const [page, setPage] = useState(1), [size, setSize] = useState(20), [sort, setSort] = useState({ key: 'latest_date', dir: -1 })
  const [showColumns, setShowColumns] = useState(false)
  const [columns, setColumns] = useState(() => { try { const saved = JSON.parse(localStorage.getItem('fundtrack.columns')); return Array.isArray(saved) ? saved.filter(k => COLUMNS.some(c => c[0] === k)) : ['1w', '1m', '3m'] } catch { return ['1w', '1m', '3m'] } })
  const { compareList, toggle, isSelected, clear } = useCompare()
  const { canManage } = useAuth()
  useEffect(() => {
    const controller = new AbortController()
    setLoading(true); setError(''); setNotice('')
    Promise.all([fetchFunds(controller.signal, { source }), fetchStats(controller.signal), fetchSyncStatus(controller.signal)])
      .then(([rows, aggregate, status]) => { setFunds(rows); setStats(aggregate); setSync(status) })
      .catch(err => { if (err.name !== 'AbortError') setError(err.message) })
      .finally(() => { if (!controller.signal.aborted) setLoading(false) })
    Promise.all([fetchFundReturns({ periods: '1w,1m,3m,6m,1y,ytd' }, controller.signal), fetchFundMetrics({ period: 'all' }, controller.signal)])
      .then(([r, m]) => { setReturns(r.items || {}); setMetrics(m.items || {}) })
      .catch(err => { if (err.name !== 'AbortError') setNotice('部分业绩指标暂时无法读取。净值记录仍可浏览，请刷新重试。') })
    return () => controller.abort()
  }, [source, version])
  useEffect(() => { setPage(1) }, [source, strategy, strategy2, search, size])
  useEffect(() => { try { localStorage.setItem('fundtrack.columns', JSON.stringify(columns)) } catch {} }, [columns])
  const strategies = [...new Set(funds.map(f => f.strategy_l1).filter(Boolean))].sort()
  const strategies2 = [...new Set(funds.filter(f => !strategy || f.strategy_l1 === strategy).map(f => f.strategy_l2).filter(Boolean))].sort()
  const getValue = (fund, key) => fund[key] ?? returns[fund.fund_id]?.[key] ?? metrics[fund.fund_id]?.[key]
  const filtered = useMemo(() => {
    const query = search.trim().toLocaleLowerCase()
    return funds.filter(f => (!strategy || f.strategy_l1 === strategy) && (!strategy2 || f.strategy_l2 === strategy2) && (!query || `${f.product_name} ${f.product_code}`.toLocaleLowerCase().includes(query))).sort((a, b) => {
      const av = getValue(a, sort.key), bv = getValue(b, sort.key)
      if (av == null) return bv == null ? 0 : 1
      if (bv == null) return -1
      return (typeof av === 'number' ? av - bv : String(av).localeCompare(String(bv), 'zh')) * sort.dir
    })
  }, [funds, search, strategy, strategy2, sort, returns, metrics])
  const pages = Math.max(1, Math.ceil(filtered.length / size)), currentPage = Math.min(page, pages)
  const visible = filtered.slice((currentPage - 1) * size, currentPage * size)
  const latest = stats.latest_nav_date || funds.reduce((date, f) => (f.latest_date || '') > date ? f.latest_date : date, '')
  const updateSort = key => setSort(previous => ({ key, dir: previous.key === key ? -previous.dir : -1 }))
  const selectionParams = new URLSearchParams(compareList.map(f => ['fund_ids', f.fund_id])).toString()
  return <div className="page-wrap">
    <div className="page-heading"><div><div className="eyebrow">FUND UNIVERSE / 基金研究</div><h1>基金净值概览</h1><p>从每一笔净值出发，持续追踪基金表现与投资策略。</p></div><div className="heading-aside"><span className="status-dot" /> 数据来源：邮件采集 · ZX 数据<br /><span className="inline-block mt-2">有效净值截止 {latest || '—'}</span></div></div>
    <div className="metric-grid">
      <div className="metric-card featured"><div className="metric-label">有效基金 <Icon name="analytics" /></div><strong>{loading ? '—' : (stats.total_funds ?? funds.length).toLocaleString()}</strong><small>已入库且具有有效净值的基金</small></div>
      <div className="metric-card"><div className="metric-label">净值记录 <Icon name="show_chart" /></div><strong>{stats.total_records?.toLocaleString() ?? '—'}</strong><small>可追溯的有效历史净值</small></div>
      <div className="metric-card"><div className="metric-label">最新净值日期 <Icon name="trending_up" /></div><strong>{latest || '—'}</strong><small>基金披露频率不同，以各基金日期为准</small></div>
      <div className="metric-card"><div className="metric-label">邮件更新状态 <Icon name="sync" /></div><strong className="!text-xl">{STATUS[sync.last_status || sync.sync_last_status] || '尚无同步记录'}</strong><small>最后成功：{formatTimestamp(sync.last_success_time || stats.last_success_time).slice(0, 16)}</small></div>
    </div>
    {notice && <div className="notice" role="status">{notice}</div>}
    <section className="panel">
      <div className="panel-heading"><div><h2>基金池 <span className="badge ml-2">{filtered.length} 只</span></h2><p>筛选、比较和跟踪你的研究标的</p></div><button className="button-secondary" onClick={() => setVersion(v => v + 1)}><Icon name="refresh" /> 刷新</button></div>
      <div className="toolbar"><label className="search-field"><Icon name="search" /><input aria-label="搜索基金名称或代码" placeholder="搜索基金名称或代码…" value={search} onChange={e => setSearch(e.target.value)} /></label>
        <select className="control" aria-label="数据来源" value={source} onChange={e => setSource(e.target.value)}>{Object.entries(SOURCES).map(([key, label]) => <option key={key} value={key}>{label}</option>)}</select>
        <select className="control" aria-label="一级策略" value={strategy} onChange={e => { setStrategy(e.target.value); setStrategy2('') }}><option value="">全部策略</option>{strategies.map(s => <option key={s}>{s}</option>)}</select>
        {strategy && <select className="control" aria-label="二级策略" value={strategy2} onChange={e => setStrategy2(e.target.value)}><option value="">全部子策略</option>{strategies2.map(s => <option key={s}>{s}</option>)}</select>}
        <button className="button-secondary ml-auto" aria-expanded={showColumns} onClick={() => setShowColumns(v => !v)}><Icon name="tune" /> 显示列</button>
      </div>
      {showColumns && <div className="column-options">{COLUMNS.map(([key, label]) => <label key={key}><input type="checkbox" checked={columns.includes(key)} onChange={() => setColumns(cols => cols.includes(key) ? cols.filter(k => k !== key) : [...cols, key])} />{label}</label>)}</div>}
      {loading ? <PageState loading /> : error ? <PageState error={error} onRetry={() => setVersion(v => v + 1)} /> : !filtered.length ? <PageState title="未找到匹配基金">尝试更改名称、策略或数据来源。</PageState> : <div className="table-scroll"><table className="research-table"><thead><tr>
        <th className="fund-identity"><button onClick={() => updateSort('product_name')}>基金名称 / 代码 ↕</button></th><th>投资策略</th><th><button onClick={() => updateSort('latest_nav')}>单位净值 ↕</button></th><th><button onClick={() => updateSort('latest_date')}>净值日期 ↕</button></th><th>数据来源</th><th>近期走势</th>
        {COLUMNS.filter(([key]) => columns.includes(key)).map(([key, label]) => <th key={key}><button onClick={() => updateSort(key)}>{label} {sort.key === key ? sort.dir === 1 ? '↑' : '↓' : '↕'}</button></th>)}<th>明细</th></tr></thead><tbody>
        {visible.map(fund => <tr key={fund.fund_id}><td className="fund-identity"><div className="flex items-start"><input className="fund-check mt-1" aria-label={`选择 ${fund.product_name}`} type="checkbox" checked={isSelected(fund.fund_id)} disabled={!isSelected(fund.fund_id) && compareList.length >= 8} onChange={() => toggle(fund)} /><div><Link to={`/fund/${fund.fund_id}`}>{fund.product_name || '未命名基金'}</Link><small>{fund.product_code}</small></div></div></td>
          <td><span className="badge">{fund.strategy_l2 || fund.strategy_l1 || '未分类'}</span></td><td className="font-semibold">{number(fund.latest_nav, 4)}</td><td>{fund.latest_date || '—'}</td><td><span className="badge">{(fund.sources || fund.data_sources || [fund.data_source]).filter(Boolean).map(s => SOURCES[s] || s).join(' / ') || '—'}</span></td><td><Sparkline values={returns[fund.fund_id]?.sparkline} /></td>
          {COLUMNS.filter(([key]) => columns.includes(key)).map(([key]) => <td key={key}><Percent value={getValue(fund, key)} ratio={key === 'sharpe'} /></td>)}<td><Link className="text-primary" to={`/fund/${fund.fund_id}`}>查看 <span aria-hidden="true">↗</span></Link></td></tr>)}
      </tbody></table></div>}
      <div className="pagination"><span>共 {filtered.length} 只 · 第 {currentPage} / {pages} 页</span><div className="flex items-center gap-2"><select className="control" aria-label="每页数量" value={size} onChange={e => setSize(Number(e.target.value))}>{[20, 50, 100].map(n => <option key={n} value={n}>{n} 条 / 页</option>)}</select><button className="button-secondary" disabled={currentPage === 1} onClick={() => setPage(currentPage - 1)} aria-label="上一页">←</button><button className="button-secondary" disabled={currentPage === pages} onClick={() => setPage(currentPage + 1)} aria-label="下一页">→</button></div></div>
    </section>
    <p className="text-xs mt-4 text-slate-500">收益以各基金可用净值区间计算；“—”表示缺少数据。不同基金的净值日期可能不同。</p>
    {compareList.length > 0 && <div className="selection-bar"><span className="text-sm">已选择 <strong>{compareList.length}</strong> / 8 只基金</span><button className="text-xs text-slate-500" onClick={clear}>清空</button><Link className="button-primary ml-auto" to={`/compare?${selectionParams}`}>对比基金 →</Link>{canManage && compareList.length >= 2 && <Link className="button-secondary" to={`/portfolios/new?${selectionParams}`}>构建组合</Link>}</div>}
  </div>
}
