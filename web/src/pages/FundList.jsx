import { useState, useEffect, useCallback, useRef, useMemo } from 'react'
import { useNavigate } from 'react-router-dom'
import {
  fetchStats, fetchFunds, fetchAllIssues,
  fetchSyncStatus, triggerSync, fetchFailures, fetchFundReturns, fetchFundMetrics,
  setFundStrategy,
} from '../api.js'
import { useCompare } from '../context/CompareContext.jsx'

// ── Strategy taxonomy ──
const STRATEGY_TAXONOMY = [
  { l1: '期货策略',   l2: ['量化期货', '主观期货'] },
  { l1: '股票对冲',   l2: ['股票市场中性', '股票多空', '择时对冲', '股票T0'] },
  { l1: '股票多头',   l2: ['主观多头', '300指增', '500指增', 'A500指增', '1000指增', '2000指增', '转债指增', '红利指增', '行业指增', '风格指增', '量化选股', '可转债多头', '另类多头'] },
  { l1: '套利策略',   l2: ['股票套利', '期货套利', '期权套利', '基金套利', '可转债套利', '混合套利'] },
  { l1: '期权策略',   l2: ['场内期权', '场外期权'] },
  { l1: '多资产策略', l2: ['宏观策略', '复合策略'] },
  { l1: '债券策略',   l2: ['利率债', '信用债', '债券复合'] },
  { l1: '组合策略',   l2: ['FOF', 'MOM'] },
  { l1: '其他',       l2: [] },
]

const L2_MAP = Object.fromEntries(STRATEGY_TAXONOMY.map(s => [s.l1, s.l2]))

const PAGE_SIZE = 20

// ── Optional column definitions ──
const COLUMN_DEFS = [
  { key: 'sparkline',          label: '走势',     defaultVisible: true,  category: 'return',  sortable: false },
  { key: '1w',                 label: '近一周',   defaultVisible: true,  category: 'return',  sortable: true },
  { key: '1m',                 label: '近一月',   defaultVisible: true,  category: 'return',  sortable: true },
  { key: '3m',                 label: '近三月',   defaultVisible: true,  category: 'return',  sortable: true },
  { key: '6m',                 label: '近六月',   defaultVisible: false, category: 'return',  sortable: true },
  { key: '1y',                 label: '近一年',   defaultVisible: false, category: 'return',  sortable: true },
  { key: 'ytd',                label: '年初至今', defaultVisible: false, category: 'return',  sortable: true },
  { key: 'annualized_return',  label: '年化收益', defaultVisible: false, category: 'metrics', sortable: true },
  { key: 'max_drawdown',       label: '最大回撤', defaultVisible: false, category: 'metrics', sortable: true },
  { key: 'annualized_vol',     label: '年化波动', defaultVisible: false, category: 'metrics', sortable: true },
  { key: 'sharpe',             label: '夏普比率', defaultVisible: false, category: 'metrics', sortable: true },
  { key: 'monthly_win_rate',   label: '月胜率',   defaultVisible: false, category: 'metrics', sortable: true },
]

const DEFAULT_VISIBLE = new Set(COLUMN_DEFS.filter(c => c.defaultVisible).map(c => c.key))
const METRICS_KEYS = new Set(COLUMN_DEFS.filter(c => c.category === 'metrics').map(c => c.key))
const RETURN_PERIOD_KEYS = ['1w', '1m', '3m', '6m', '1y', 'ytd']

function loadVisibleCols() {
  try {
    const raw = localStorage.getItem('fundlist_visible_cols')
    if (raw) return new Set(JSON.parse(raw))
  } catch {}
  return new Set(DEFAULT_VISIBLE)
}

function saveVisibleCols(set) {
  try { localStorage.setItem('fundlist_visible_cols', JSON.stringify([...set])) } catch {}
}

function tagColor(tagId) {
  const COLORS = ['#6366f1','#3b82f6','#10b981','#f59e0b','#ef4444','#8b5cf6','#06b6d4','#f97316','#84cc16','#ec4899']
  return COLORS[tagId % COLORS.length]
}

// ── Strategy color by L1 ──
const L1_COLORS = {
  '期货策略':   { bg: '#fef3c7', text: '#92400e', border: '#fbbf24' },
  '股票对冲':   { bg: '#dbeafe', text: '#1e40af', border: '#60a5fa' },
  '股票多头':   { bg: '#fee2e2', text: '#991b1b', border: '#f87171' },
  '套利策略':   { bg: '#d1fae5', text: '#065f46', border: '#34d399' },
  '期权策略':   { bg: '#ede9fe', text: '#5b21b6', border: '#a78bfa' },
  '多资产策略': { bg: '#fce7f3', text: '#9d174d', border: '#f472b6' },
  '债券策略':   { bg: '#e0f2fe', text: '#0c4a6e', border: '#38bdf8' },
  '组合策略':   { bg: '#f0fdf4', text: '#14532d', border: '#4ade80' },
  '其他':       { bg: '#f1f5f9', text: '#475569', border: '#94a3b8' },
}

function strategyColor(l1) {
  return L1_COLORS[l1] || { bg: '#f1f5f9', text: '#475569', border: '#94a3b8' }
}

// ── Strategy Assigner floating panel ──
function StrategyAssigner({ fund, onClose, onSave }) {
  const [selL1, setSelL1] = useState(fund.strategy_l1 || '')
  const [selL2, setSelL2] = useState(fund.strategy_l2 || '')
  const [saving, setSaving] = useState(false)

  const l2Options = selL1 ? (L2_MAP[selL1] || []) : []

  function handleL1(v) {
    setSelL1(v)
    setSelL2('')
  }

  async function handleSave() {
    setSaving(true)
    try {
      await onSave(fund.fund_id, selL1 || null, selL2 || null)
      onClose()
    } catch {
      setSaving(false)
    }
  }

  return (
    <div
      className="absolute z-50 bg-white dark:bg-slate-800 rounded-xl shadow-xl border border-slate-200 dark:border-slate-700 p-3 w-64"
      onClick={e => e.stopPropagation()}
    >
      <div className="flex items-center justify-between mb-2">
        <span className="text-xs font-semibold text-slate-600 dark:text-slate-300">策略分类</span>
        <button onClick={onClose} className="text-slate-400 hover:text-slate-600 text-base leading-none">×</button>
      </div>
      <div className="mb-2">
        <p className="text-[10px] text-slate-400 mb-1">一级策略</p>
        <div className="flex flex-wrap gap-1">
          {STRATEGY_TAXONOMY.map(s => {
            const c = strategyColor(s.l1)
            const active = selL1 === s.l1
            return (
              <button
                key={s.l1}
                onClick={() => handleL1(s.l1)}
                className="px-2 py-0.5 rounded text-[11px] font-medium border transition-colors"
                style={active
                  ? { backgroundColor: c.border, color: '#fff', borderColor: c.border }
                  : { backgroundColor: c.bg, color: c.text, borderColor: c.border }
                }
              >{s.l1}</button>
            )
          })}
          {selL1 && (
            <button
              onClick={() => { setSelL1(''); setSelL2('') }}
              className="px-2 py-0.5 rounded text-[11px] border border-slate-200 text-slate-400 hover:text-rose-500"
            >清除</button>
          )}
        </div>
      </div>
      {l2Options.length > 0 && (
        <div className="mb-3">
          <p className="text-[10px] text-slate-400 mb-1">二级策略</p>
          <div className="flex flex-wrap gap-1">
            {l2Options.map(l2 => (
              <button
                key={l2}
                onClick={() => setSelL2(selL2 === l2 ? '' : l2)}
                className={`px-2 py-0.5 rounded text-[11px] border transition-colors ${
                  selL2 === l2
                    ? 'bg-slate-700 text-white border-slate-700'
                    : 'bg-slate-50 text-slate-600 border-slate-200 hover:border-slate-400'
                }`}
              >{l2}</button>
            ))}
          </div>
        </div>
      )}
      <button
        onClick={handleSave}
        disabled={saving}
        className="w-full mt-1 py-1.5 bg-primary text-white text-xs rounded-lg hover:bg-primary/90 disabled:opacity-50 transition-colors"
      >{saving ? '保存…' : '确认'}</button>
    </div>
  )
}
function SkeletonRow() {
  return (
    <tr>
      {[32, 260, 70, 70, 80, 60, 50].map((w, i) => (
        <td key={i} className="px-3 py-3 md:px-6 md:py-4">
          <div className="shimmer rounded h-4" style={{ width: w }} />
        </td>
      ))}
    </tr>
  )
}

// ── Percentage cell ──
function PctCell({ pct }) {
  if (pct == null) return <span className="text-slate-400 text-sm">—</span>
  const color = pct > 0 ? 'text-rose-500' : pct < 0 ? 'text-emerald-600' : 'text-slate-400'
  const sign = pct > 0 ? '+' : ''
  return <span className={`font-medium text-sm ${color}`}>{sign}{pct.toFixed(2)}%</span>
}

// ── Sparkline SVG ──
function Sparkline({ data, width = 80, height = 24 }) {
  if (!data || data.length < 2) return <span className="text-slate-300 dark:text-slate-600">—</span>
  const min = Math.min(...data)
  const max = Math.max(...data)
  const range = max - min || 1
  const points = data.map((v, i) => {
    const x = (i / (data.length - 1)) * width
    const y = height - ((v - min) / range) * (height - 2) - 1
    return `${x.toFixed(1)},${y.toFixed(1)}`
  }).join(' ')
  const lastVal = data[data.length - 1]
  const firstVal = data[0]
  const color = lastVal >= firstVal ? '#ef4444' : '#10b981'
  return (
    <svg width={width} height={height} className="inline-block align-middle">
      <polyline
        points={points}
        fill="none"
        stroke={color}
        strokeWidth="1.5"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </svg>
  )
}

// ── Sort indicator ──
function SortIcon({ active, dir }) {
  if (!active) return <span className="text-slate-300 text-xs ml-0.5">↕</span>
  return <span className="text-primary text-xs ml-0.5">{dir === 'asc' ? '↑' : '↓'}</span>
}

// ── Metric value cell ──
function MetricCell({ val, format }) {
  if (val == null) return <span className="text-slate-300 text-sm">—</span>
  if (format === 'pct') {
    const color = val > 0 ? 'text-rose-500' : val < 0 ? 'text-emerald-600' : 'text-slate-400'
    const sign = val > 0 ? '+' : ''
    return <span className={`font-medium text-sm ${color}`}>{sign}{val.toFixed(2)}%</span>
  }
  if (format === 'ratio') {
    const color = val > 0 ? 'text-rose-500' : val < 0 ? 'text-emerald-600' : 'text-slate-400'
    return <span className={`font-medium text-sm ${color}`}>{val.toFixed(3)}</span>
  }
  return <span className="text-sm">{val}</span>
}

function buildTooltip(issue) {
  const lines = []
  if (issue.anomalous.length > 0) {
    const parts = issue.anomalous.map(a => `${a.nav_date}(NAV=${a.unit_nav.toFixed(2)})`)
    lines.push(`异常净值: ${parts.join(', ')}`)
  }
  if (issue.gaps.length > 0) {
    const parts = issue.gaps.map(g => `${g.from_date} ~ ${g.to_date} (${g.gap_days}天)`)
    lines.push(`日期断层: ${parts.join('; ')}`)
  }
  return lines.join('\n')
}

// ── Skeleton row for loading state ──
export default function FundList() {
  const navigate = useNavigate()
  const { compareList, toggle, remove, clear, isSelected } = useCompare()

  const [stats, setStats] = useState(null)
  const [funds, setFunds] = useState([])
  const [fundReturns, setFundReturns] = useState({})
  const [issues, setIssues] = useState({})
  const [syncStatus, setSyncStatus] = useState(null)
  const [syncing, setSyncing] = useState(false)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [search, setSearch] = useState('')
  const [debouncedSearch, setDebouncedSearch] = useState('')
  const [retryCount, setRetryCount] = useState(0)
  const [failures, setFailures] = useState({ total: 0, items: [] })
  const [showFailures, setShowFailures] = useState(false)
  const [page, setPage] = useState(1)

  // Strategy filter state
  const [activeL1, setActiveL1] = useState(null)
  const [activeL2, setActiveL2] = useState(null)

  // Strategy assigner state
  const [stratAssigner, setStratAssigner] = useState(null) // { fundId }

  const syncPollRef = useRef(null)

  // ── Sort + column visibility state ──
  const [sortKey, setSortKey] = useState('latest_date')
  const [sortDir, setSortDir] = useState('desc')
  const [visibleCols, setVisibleCols] = useState(loadVisibleCols)
  const [showColPicker, setShowColPicker] = useState(false)
  const [fundMetrics, setFundMetrics] = useState({})
  const colPickerRef = useRef(null)

  // ── Load funds, stats, issues, returns, and sync status ──
  useEffect(() => {
    const controller = new AbortController()
    const { signal } = controller

    setError(null)
    setLoading(true)

    Promise.all([fetchStats(signal), fetchFunds(signal)])
      .then(([s, items]) => {
        setStats(s)
        setFunds(items)
        setLoading(false)
      })
      .catch(err => {
        if (err.name === 'AbortError') return
        setError(err.message)
        setLoading(false)
      })

    const neededPeriods = RETURN_PERIOD_KEYS.filter(k => visibleCols.has(k))
    const periodsStr = neededPeriods.length > 0 ? neededPeriods.join(',') : '1w,1m,3m'
    fetchFundReturns({ periods: periodsStr }, signal)
      .then(data => setFundReturns(data.items || {}))
      .catch(err => { if (err.name !== 'AbortError') console.warn('returns load failed', err) })

    fetchAllIssues(signal)
      .then(data => {
        const map = {}
        for (const [k, v] of Object.entries(data.issues || {})) {
          map[Number(k)] = v
        }
        setIssues(map)
      })
      .catch(err => { if (err.name !== 'AbortError') console.warn('issues load failed', err) })

    fetchSyncStatus(signal)
      .then(data => setSyncStatus(data))
      .catch(err => { if (err.name !== 'AbortError') console.warn('sync status load failed', err) })

    fetchFailures(signal)
      .then(data => setFailures(data))
      .catch(err => { if (err.name !== 'AbortError') console.warn('failures load failed', err) })

    return () => controller.abort()
  }, [retryCount, visibleCols])

  // ── Fetch metrics when any metrics column is visible ──
  useEffect(() => {
    const needsMetrics = COLUMN_DEFS.some(c => c.category === 'metrics' && visibleCols.has(c.key))
    if (!needsMetrics) return
    const controller = new AbortController()
    fetchFundMetrics({ period: 'all' }, controller.signal)
      .then(data => setFundMetrics(data.items || {}))
      .catch(err => { if (err.name !== 'AbortError') console.warn('metrics load failed', err) })
    return () => controller.abort()
  }, [visibleCols])

  // ── Close col picker on outside click ──
  useEffect(() => {
    if (!showColPicker) return
    function handler(e) {
      if (colPickerRef.current && !colPickerRef.current.contains(e.target)) setShowColPicker(false)
    }
    document.addEventListener('mousedown', handler)
    return () => document.removeEventListener('mousedown', handler)
  }, [showColPicker])

  // ── Close strategy assigner on outside click ──
  useEffect(() => {
    if (!stratAssigner) return
    const handler = () => setStratAssigner(null)
    document.addEventListener('click', handler)
    return () => document.removeEventListener('click', handler)
  }, [stratAssigner])
  // ── Debounce search ──
  useEffect(() => {
    const t = setTimeout(() => {
      setDebouncedSearch(search)
      setPage(1)
    }, 200)
    return () => clearTimeout(t)
  }, [search])

  useEffect(() => {
    return () => {
      if (syncPollRef.current) clearTimeout(syncPollRef.current)
    }
  }, [])

  const handleSearchChange = useCallback((e) => setSearch(e.target.value), [])

  const handleRetry = useCallback(() => {
    setFunds([])
    setFundReturns({})
    setRetryCount(c => c + 1)
  }, [])

  const handleSync = useCallback(async () => {
    if (syncing) return
    setSyncing(true)
    try {
      await triggerSync()
      function poll() {
        syncPollRef.current = setTimeout(async () => {
          try {
            const data = await fetchSyncStatus()
            setSyncStatus(data)
            if (data.sync_last_status === 'running') {
              poll()
            } else {
              setSyncing(false)
              setRetryCount(c => c + 1)
            }
          } catch {
            setSyncing(false)
          }
        }, 2000)
      }
      poll()
    } catch (err) {
      console.warn('trigger sync failed', err)
      setSyncing(false)
    }
  }, [syncing])

  // ── Strategy assignment handler ──
  const handleSaveStrategy = useCallback(async (fundId, l1, l2) => {
    await setFundStrategy(fundId, l1, l2)
    setFunds(prev => prev.map(f =>
      f.fund_id === fundId ? { ...f, strategy_l1: l1, strategy_l2: l2 } : f
    ))
  }, [])

  const openStratAssigner = useCallback((e, fundId) => {
    e.stopPropagation()
    setStratAssigner(prev => prev?.fundId === fundId ? null : { fundId })
  }, [])

  // ── Sort handler ──
  const handleSort = useCallback((key) => {
    setSortKey(prev => {
      if (prev === key) {
        setSortDir(d => d === 'asc' ? 'desc' : 'asc')
        return key
      }
      setSortDir('desc')
      return key
    })
    setPage(1)
  }, [])

  // ── Column visibility toggle ──
  const toggleCol = useCallback((key) => {
    setVisibleCols(prev => {
      const next = new Set(prev)
      if (next.has(key)) next.delete(key)
      else next.add(key)
      saveVisibleCols(next)
      return next
    })
  }, [])

  // ── Filtered & sorted & paginated data ──
  const filtered = useMemo(() => {
    let base = funds
    if (debouncedSearch) {
      base = base.filter(f =>
        (f.product_name || '').includes(debouncedSearch) ||
        (f.product_code || '').includes(debouncedSearch)
      )
    }
    if (activeL1) {
      base = base.filter(f => f.strategy_l1 === activeL1)
    }
    if (activeL2) {
      base = base.filter(f => f.strategy_l2 === activeL2)
    }
    return [...base].sort((a, b) => {
      const ret_a = fundReturns[a.fund_id]
      const ret_b = fundReturns[b.fund_id]
      const met_a = fundMetrics[a.fund_id]
      const met_b = fundMetrics[b.fund_id]

      if (sortKey === 'latest_date') {
        const dc = (b.latest_date || '').localeCompare(a.latest_date || '')
        if (dc !== 0) return dc
        return (a.product_name || '').localeCompare(b.product_name || '', 'zh')
      }
      if (sortKey === 'product_name') {
        const cmp = (a.product_name || '').localeCompare(b.product_name || '', 'zh')
        return sortDir === 'asc' ? cmp : -cmp
      }
      if (sortKey === 'latest_nav') {
        const va = a.latest_nav ?? null, vb = b.latest_nav ?? null
        if (va === null && vb === null) return 0
        if (va === null) return 1
        if (vb === null) return -1
        return sortDir === 'asc' ? va - vb : vb - va
      }
      // Return period keys
      if (RETURN_PERIOD_KEYS.includes(sortKey)) {
        const va = ret_a?.[sortKey] ?? null, vb = ret_b?.[sortKey] ?? null
        if (va === null && vb === null) return 0
        if (va === null) return 1
        if (vb === null) return -1
        return sortDir === 'asc' ? va - vb : vb - va
      }
      // Metrics keys
      if (METRICS_KEYS.has(sortKey)) {
        const va = met_a?.[sortKey] ?? null, vb = met_b?.[sortKey] ?? null
        if (va === null && vb === null) return 0
        if (va === null) return 1
        if (vb === null) return -1
        return sortDir === 'asc' ? va - vb : vb - va
      }
      return 0
    })
  }, [funds, debouncedSearch, sortKey, sortDir, fundReturns, fundMetrics, activeL1, activeL2])

  const totalPages = Math.max(1, Math.ceil(filtered.length / PAGE_SIZE))
  const currentPage = Math.min(page, totalPages)
  const pageStart = (currentPage - 1) * PAGE_SIZE
  const pageEnd = Math.min(pageStart + PAGE_SIZE, filtered.length)
  const paged = filtered.slice(pageStart, pageEnd)

  // ── Compute summary stats ──
  const issueCount = Object.values(issues).reduce((sum, v) => sum + v.anomalous.length + v.gaps.length, 0)

  // Build page numbers
  const pageNumbers = []
  if (totalPages <= 5) {
    for (let i = 1; i <= totalPages; i++) pageNumbers.push(i)
  } else {
    pageNumbers.push(1)
    if (currentPage > 3) pageNumbers.push('...')
    for (let i = Math.max(2, currentPage - 1); i <= Math.min(totalPages - 1, currentPage + 1); i++) {
      pageNumbers.push(i)
    }
    if (currentPage < totalPages - 2) pageNumbers.push('...')
    pageNumbers.push(totalPages)
  }

  return (
    <>
      {/* Failures modal */}
      {showFailures && (
        <div
          className="fixed inset-0 bg-black/40 z-50 flex items-center justify-center p-4"
          onClick={() => setShowFailures(false)}
        >
          <div
            className="bg-white dark:bg-slate-900 rounded-2xl shadow-xl max-w-3xl w-full max-h-[80vh] flex flex-col border border-slate-200 dark:border-slate-800"
            onClick={e => e.stopPropagation()}
          >
            <div className="flex items-center justify-between px-6 py-4 border-b border-slate-200 dark:border-slate-800">
              <h2 className="text-base font-semibold">
                附件提取失败记录（{failures.total} 条）
              </h2>
              <button
                onClick={() => setShowFailures(false)}
                className="text-slate-400 hover:text-slate-600 dark:hover:text-slate-200 text-xl leading-none"
              >
                ×
              </button>
            </div>
            <div className="overflow-auto flex-1 custom-scrollbar">
              <table className="w-full text-xs">
                <thead className="bg-slate-50 dark:bg-slate-800/50 sticky top-0">
                  <tr className="text-left text-slate-500 border-b border-slate-200 dark:border-slate-800">
                    <th className="px-4 py-2 font-medium">失败时间</th>
                    <th className="px-4 py-2 font-medium">邮件主题</th>
                    <th className="px-4 py-2 font-medium">附件文件名</th>
                    <th className="px-4 py-2 font-medium">失败原因</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-slate-100 dark:divide-slate-800">
                  {failures.items.map(item => (
                    <tr key={item.id} className="hover:bg-slate-50 dark:hover:bg-slate-800/50">
                      <td className="px-4 py-2 whitespace-nowrap text-slate-500">
                        {item['失败时间']?.slice(0, 16) || '—'}
                      </td>
                      <td className="px-4 py-2 max-w-[200px] truncate" title={item['邮件主题']}>
                        {item['邮件主题'] || '—'}
                      </td>
                      <td className="px-4 py-2 max-w-[180px] truncate" title={item['附件文件名']}>
                        {item['附件文件名'] || '—'}
                      </td>
                      <td className="px-4 py-2 max-w-[220px] truncate text-rose-500" title={item['失败原因']}>
                        {item['失败原因'] || '—'}
                      </td>
                    </tr>
                  ))}
                  {failures.items.length === 0 && (
                    <tr>
                      <td colSpan={4} className="px-4 py-8 text-center text-slate-400">暂无记录</td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      )}

      {/* ─── Header ─── */}
      <header className="h-16 border-b border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-900 px-4 md:px-8 flex items-center justify-between sticky top-14 lg:top-0 z-10">
        <div className="flex items-center gap-8">
          {stats && (
            <>
              <div className="flex flex-col">
                <span className="text-xs text-slate-400 uppercase font-semibold tracking-wider">基金数量</span>
                <span className="text-lg font-bold">{stats.total_funds} <span className="text-sm font-normal text-slate-400">active</span></span>
              </div>
              <div className="h-8 w-px bg-slate-200 dark:bg-slate-800" />
              <div className="flex flex-col">
                <span className="text-xs text-slate-400 uppercase font-semibold tracking-wider">净值记录</span>
                <span className="text-lg font-bold">{stats.total_records?.toLocaleString()}</span>
              </div>
            </>
          )}
        </div>
        <div className="flex items-center gap-4">
          {failures.total > 0 && (
            <button
              onClick={() => setShowFailures(true)}
              className="flex items-center gap-1.5 px-3 py-1.5 text-xs bg-amber-50 dark:bg-amber-900/20 text-amber-600 border border-amber-200 dark:border-amber-800 rounded-lg hover:bg-amber-100 dark:hover:bg-amber-900/40 transition-colors"
            >
              <span className="material-symbols-outlined text-[16px]">warning</span>
              提取失败 {failures.total}
            </button>
          )}
          {syncStatus && (
            <div className="text-right hidden sm:block">
              <p className="text-xs text-slate-400">上次同步</p>
              <p className="text-sm font-medium">
                {syncStatus.sync_last_time ? syncStatus.sync_last_time.slice(0, 16).replace('T', ' ') : '—'}
                {syncStatus.sync_last_status === 'error' && (
                  <span className="ml-1 text-rose-500 text-xs">失败</span>
                )}
              </p>
            </div>
          )}
          <button
            onClick={handleSync}
            disabled={syncing}
            className="bg-primary hover:bg-primary/90 text-white px-5 py-2 rounded-lg font-medium shadow-md shadow-primary/20 transition-all flex items-center gap-2 disabled:opacity-50 disabled:cursor-not-allowed"
          >
            <span className={`material-symbols-outlined text-[18px] ${syncing ? 'animate-spin' : ''}`}>sync</span>
            {syncing ? '同步中…' : '立即同步'}
          </button>
        </div>
      </header>

      {/* ─── Content ─── */}
      <div className="p-4 md:p-8 overflow-y-auto custom-scrollbar flex-1">
        {/* Error banner */}
        {error && (
          <div className="mb-6 flex items-center gap-3 bg-rose-50 dark:bg-rose-900/20 border border-rose-200 dark:border-rose-800 rounded-xl px-5 py-3 text-sm">
            <span className="material-symbols-outlined text-rose-500">error</span>
            <span className="text-rose-600 dark:text-rose-400 font-medium">连接后端失败</span>
            <span className="text-rose-400 text-xs flex-1 truncate">{error}</span>
            <button
              onClick={handleRetry}
              className="shrink-0 px-3 py-1 bg-rose-500 text-white rounded-lg text-xs hover:bg-rose-600 transition-colors"
            >
              重试
            </button>
          </div>
        )}

        {/* ─── Strategy Filter ─── */}
        <div className="mb-4 space-y-2">
          {/* 一级策略 */}
          <div className="flex items-center gap-2 flex-wrap">
            <span className="text-sm font-medium text-slate-500 w-16 shrink-0">一级策略：</span>
            <button
              onClick={() => { setActiveL1(null); setActiveL2(null); setPage(1) }}
              className={`px-3 py-1 rounded text-sm font-medium transition-colors ${
                activeL1 === null
                  ? 'bg-rose-100 text-rose-600 border border-rose-300'
                  : 'text-slate-500 hover:text-slate-700 dark:text-slate-400 dark:hover:text-slate-200'
              }`}
            >不限</button>
            {STRATEGY_TAXONOMY.map(s => {
              const c = strategyColor(s.l1)
              const active = activeL1 === s.l1
              return (
                <button
                  key={s.l1}
                  onClick={() => {
                    if (active) { setActiveL1(null); setActiveL2(null) }
                    else { setActiveL1(s.l1); setActiveL2(null) }
                    setPage(1)
                  }}
                  className="px-3 py-1 rounded text-sm font-medium transition-colors flex items-center gap-1"
                  style={active
                    ? { backgroundColor: c.bg, color: c.text, border: `1px solid ${c.border}` }
                    : { color: '#64748b' }
                  }
                >
                  {s.l1}
                  {active && (
                    <span className="text-xs leading-none" style={{ color: c.text }}>×</span>
                  )}
                </button>
              )
            })}
          </div>

          {/* 二级策略 — only shown when L1 is selected and has sub-categories */}
          {activeL1 && L2_MAP[activeL1]?.length > 0 && (
            <div className="flex items-center gap-2 flex-wrap">
              <span className="text-sm font-medium text-slate-500 w-16 shrink-0">二级策略：</span>
              <button
                onClick={() => { setActiveL2(null); setPage(1) }}
                className={`px-3 py-1 rounded text-sm font-medium transition-colors ${
                  activeL2 === null
                    ? 'bg-rose-100 text-rose-600 border border-rose-300'
                    : 'text-slate-500 hover:text-slate-700 dark:text-slate-400'
                }`}
              >不限</button>
              {L2_MAP[activeL1].map(l2 => (
                <button
                  key={l2}
                  onClick={() => { setActiveL2(activeL2 === l2 ? null : l2); setPage(1) }}
                  className={`px-3 py-1 rounded text-sm font-medium transition-colors ${
                    activeL2 === l2
                      ? 'bg-slate-700 text-white'
                      : 'text-slate-500 hover:text-slate-700 dark:text-slate-400 dark:hover:text-slate-200'
                  }`}
                >{l2}</button>
              ))}
            </div>
          )}
        </div>

        {/* ─── Search & col picker ─── */}
        <div className="flex items-center justify-end mb-4 gap-2">
          <div className="relative group">
            <span className="material-symbols-outlined absolute left-3 top-1/2 -translate-y-1/2 text-slate-400 group-focus-within:text-primary transition-colors">search</span>
            <input
              type="text"
              value={search}
              onChange={handleSearchChange}
              placeholder="搜索基金名称或代码…"
              className="pl-10 pr-4 py-2 bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-lg text-sm focus:ring-2 focus:ring-primary/20 focus:border-primary outline-none w-full md:w-64 transition-all"
            />
          </div>
          {/* Column visibility picker */}
          <div className="relative" ref={colPickerRef}>
            <button
              onClick={() => setShowColPicker(v => !v)}
              className={`w-9 h-9 flex items-center justify-center rounded-lg border transition-colors ${showColPicker ? 'bg-primary text-white border-primary' : 'bg-white dark:bg-slate-800 border-slate-200 dark:border-slate-700 text-slate-500 hover:text-primary'}`}
              title="显示/隐藏列"
            >
              <span className="material-symbols-outlined text-[18px]">view_column</span>
            </button>
            {showColPicker && (
              <div className="absolute right-0 top-10 z-50 bg-white dark:bg-slate-900 rounded-xl shadow-xl border border-slate-200 dark:border-slate-700 p-3 w-52">
                <p className="text-xs font-semibold text-slate-500 mb-2 uppercase tracking-wider">可见列</p>
                <div className="space-y-0.5">
                  {['return', 'metrics'].map(cat => (
                    <div key={cat}>
                      <p className="text-[10px] font-medium text-slate-400 mt-2 mb-1 uppercase">{cat === 'return' ? '收益率' : '业绩指标'}</p>
                      {COLUMN_DEFS.filter(c => c.category === cat).map(col => (
                        <label key={col.key} className="flex items-center gap-2 px-1 py-1 rounded cursor-pointer hover:bg-slate-50 dark:hover:bg-slate-800">
                          <input
                            type="checkbox"
                            checked={visibleCols.has(col.key)}
                            onChange={() => toggleCol(col.key)}
                            className="w-3.5 h-3.5 accent-primary"
                          />
                          <span className="text-xs text-slate-700 dark:text-slate-200">{col.label}</span>
                        </label>
                      ))}
                    </div>
                  ))}
                </div>
              </div>
            )}
          </div>
        </div>

        {/* ─── Table ─── */}
        <div className="bg-white dark:bg-slate-900 rounded-2xl border border-slate-200 dark:border-slate-800 shadow-sm overflow-hidden">
          <div className="overflow-x-auto">
            <table className="w-full text-left border-collapse">
              <thead>
                <tr className="bg-slate-50 dark:bg-slate-800/50 border-b border-slate-200 dark:border-slate-800">
                  <th className="px-3 py-3 md:px-4 md:py-4 text-xs font-semibold text-slate-500 uppercase tracking-wider w-10 text-center">对比</th>
                  <th className="px-3 py-3 md:px-6 md:py-4 text-xs font-semibold text-slate-500 uppercase tracking-wider w-12">#</th>
                  <th
                    className="px-3 py-3 md:px-6 md:py-4 text-xs font-semibold text-slate-500 uppercase tracking-wider min-w-[260px] cursor-pointer select-none hover:text-primary"
                    onClick={() => handleSort('product_name')}
                  >
                    基金名称 <SortIcon active={sortKey === 'product_name'} dir={sortDir} />
                  </th>
                  <th className="px-3 py-3 md:px-6 md:py-4 text-xs font-semibold text-slate-500 uppercase tracking-wider">代码</th>
                  <th
                    className="px-3 py-3 md:px-6 md:py-4 text-xs font-semibold text-slate-500 uppercase tracking-wider cursor-pointer select-none hover:text-primary"
                    onClick={() => handleSort('latest_nav')}
                  >
                    最新净值 <SortIcon active={sortKey === 'latest_nav'} dir={sortDir} />
                  </th>
                  <th
                    className="px-3 py-3 md:px-6 md:py-4 text-xs font-semibold text-slate-500 uppercase tracking-wider cursor-pointer select-none hover:text-primary"
                    onClick={() => handleSort('latest_date')}
                  >
                    净值日期 <SortIcon active={sortKey === 'latest_date'} dir={sortDir} />
                  </th>
                  {COLUMN_DEFS.filter(c => visibleCols.has(c.key)).map(col => (
                    <th
                      key={col.key}
                      className={`px-3 py-3 md:px-6 md:py-4 text-xs font-semibold text-slate-500 uppercase tracking-wider ${col.key === 'sparkline' ? 'text-center' : 'text-right'} ${col.sortable ? 'cursor-pointer select-none hover:text-primary' : ''}`}
                      onClick={col.sortable ? () => handleSort(col.key) : undefined}
                    >
                      {col.label}{col.sortable && <SortIcon active={sortKey === col.key} dir={sortDir} />}
                    </th>
                  ))}
                  <th className="px-3 py-3 md:px-6 md:py-4 text-xs font-semibold text-slate-500 uppercase tracking-wider text-center">状态</th>
                  <th className="px-3 py-3 md:px-6 md:py-4 text-xs font-semibold text-slate-500 uppercase tracking-wider text-right">操作</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-100 dark:divide-slate-800">
                {loading
                  ? Array.from({ length: 8 }, (_, i) => <SkeletonRow key={i} />)
                  : paged.map((fund, idx) => {
                      const fundIssue = issues[fund.fund_id]
                      const fundIssueCount = fundIssue
                        ? fundIssue.anomalous.length + fundIssue.gaps.length
                        : 0
                      const hasIssue = fundIssueCount > 0
                      const isAssigning = stratAssigner?.fundId === fund.fund_id
                      const globalIdx = pageStart + idx

                      return (
                        <tr
                          key={fund.fund_id}
                          className={`hover:bg-slate-50 dark:hover:bg-slate-800/50 cursor-pointer transition-colors ${
                            globalIdx % 2 === 1 ? 'bg-slate-50/30 dark:bg-slate-800/20' : ''
                          } ${isSelected(fund.fund_id) ? 'bg-blue-50/40 dark:bg-blue-900/10' : ''}`}
                          onClick={() => navigate(`/fund/${fund.fund_id}`)}
                        >
                          <td className="px-3 py-3 md:px-4 md:py-4 text-center" onClick={e => e.stopPropagation()}>
                            <input
                              type="checkbox"
                              checked={isSelected(fund.fund_id)}
                              onChange={() => toggle(fund)}
                              className="w-4 h-4 accent-blue-600 cursor-pointer"
                            />
                          </td>
                          <td className="px-3 py-3 md:px-6 md:py-4 text-sm text-slate-400">
                            {String(globalIdx + 1).padStart(2, '0')}
                          </td>
                          <td className="px-3 py-3 md:px-6 md:py-4">
                            <div className="flex flex-col">
                              <span className="font-medium text-slate-900 dark:text-white">
                                {fund.product_name || '—'}
                              </span>
                              <div className="flex items-center gap-1 mt-0.5 relative" onClick={e => e.stopPropagation()}>
                                {fund.strategy_l1 ? (() => {
                                  const c = strategyColor(fund.strategy_l1)
                                  return (
                                    <button
                                      className="inline-flex items-center px-1.5 py-0.5 rounded text-[10px] font-medium border"
                                      style={{ backgroundColor: c.bg, color: c.text, borderColor: c.border }}
                                      onClick={e => openStratAssigner(e, fund.fund_id)}
                                    >
                                      {fund.strategy_l1}{fund.strategy_l2 ? ` · ${fund.strategy_l2}` : ''}
                                      <span className="ml-1 opacity-60">✎</span>
                                    </button>
                                  )
                                })() : (
                                  <button
                                    className="inline-flex items-center px-1.5 py-0.5 rounded text-[10px] border border-dashed border-slate-300 text-slate-400 hover:border-primary hover:text-primary"
                                    onClick={e => openStratAssigner(e, fund.fund_id)}
                                  >+ 策略</button>
                                )}
                                {isAssigning && (
                                  <div className="absolute top-5 left-0">
                                    <StrategyAssigner
                                      fund={fund}
                                      onClose={() => setStratAssigner(null)}
                                      onSave={handleSaveStrategy}
                                    />
                                  </div>
                                )}
                              </div>
                            </div>
                          </td>
                          <td className="px-3 py-3 md:px-6 md:py-4 text-sm font-mono text-slate-500">
                            {fund.product_code}
                          </td>
                          <td className="px-3 py-3 md:px-6 md:py-4">
                            <span className="text-sm font-semibold">
                              {fund.latest_nav != null ? fund.latest_nav.toFixed(4) : '—'}
                            </span>
                          </td>
                          <td className="px-3 py-3 md:px-6 md:py-4 text-sm text-slate-500">
                            {fund.latest_date || '—'}
                          </td>
                          {COLUMN_DEFS.filter(c => visibleCols.has(c.key)).map(col => {
                            const ret = fundReturns[fund.fund_id]
                            const met = fundMetrics[fund.fund_id]
                            if (col.key === 'sparkline') return (
                              <td key="sparkline" className="px-3 py-3 md:px-6 md:py-4 text-center">
                                <Sparkline data={ret?.sparkline} />
                              </td>
                            )
                            if (col.key === '1w') return <td key="1w" className="px-3 py-3 md:px-6 md:py-4 text-right"><PctCell pct={ret?.['1w']} /></td>
                            if (col.key === '1m') return <td key="1m" className="px-3 py-3 md:px-6 md:py-4 text-right"><PctCell pct={ret?.['1m']} /></td>
                            if (col.key === '3m') return <td key="3m" className="px-3 py-3 md:px-6 md:py-4 text-right"><PctCell pct={ret?.['3m']} /></td>
                            if (col.key === '6m') return <td key="6m" className="px-3 py-3 md:px-6 md:py-4 text-right"><PctCell pct={ret?.['6m']} /></td>
                            if (col.key === '1y') return <td key="1y" className="px-3 py-3 md:px-6 md:py-4 text-right"><PctCell pct={ret?.['1y']} /></td>
                            if (col.key === 'ytd') return <td key="ytd" className="px-3 py-3 md:px-6 md:py-4 text-right"><PctCell pct={ret?.['ytd']} /></td>
                            if (col.key === 'annualized_return') return <td key="annualized_return" className="px-3 py-3 md:px-6 md:py-4 text-right"><MetricCell val={met?.annualized_return} format="pct" /></td>
                            if (col.key === 'max_drawdown') return <td key="max_drawdown" className="px-3 py-3 md:px-6 md:py-4 text-right"><MetricCell val={met?.max_drawdown} format="pct" /></td>
                            if (col.key === 'annualized_vol') return <td key="annualized_vol" className="px-3 py-3 md:px-6 md:py-4 text-right"><MetricCell val={met?.annualized_vol} format="pct" /></td>
                            if (col.key === 'sharpe') return <td key="sharpe" className="px-3 py-3 md:px-6 md:py-4 text-right"><MetricCell val={met?.sharpe} format="ratio" /></td>
                            if (col.key === 'monthly_win_rate') return <td key="monthly_win_rate" className="px-3 py-3 md:px-6 md:py-4 text-right"><MetricCell val={met?.monthly_win_rate} format="pct" /></td>
                            return null
                          })}
                          <td className="px-3 py-3 md:px-6 md:py-4 text-center">
                            {hasIssue ? (
                              <span
                                title={buildTooltip(fundIssue)}
                                className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-[10px] font-medium bg-amber-100 dark:bg-amber-900/30 text-amber-700 dark:text-amber-400 cursor-help"
                              >
                                <span className="material-symbols-outlined text-[12px]">warning</span>
                                {fundIssueCount}
                              </span>
                            ) : (
                              fundIssue
                                ? <span className="text-xs text-slate-300 dark:text-slate-600">—</span>
                                : null
                            )}
                          </td>
                          <td className="px-3 py-3 md:px-6 md:py-4 text-right">
                            <button
                              className="text-primary hover:text-primary/80 text-sm font-medium"
                              onClick={e => { e.stopPropagation(); navigate(`/fund/${fund.fund_id}`) }}
                            >
                              详情
                            </button>
                          </td>
                        </tr>
                      )
                    })
                }
                {!loading && filtered.length === 0 && (
                  <tr>
                    <td colSpan={6 + visibleCols.size + 2} className="px-6 py-12 text-center text-slate-400">
                      {error ? '请点击上方"重试"按钮重新加载' : '没有匹配的基金'}
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>

          {/* Pagination */}
          {!loading && filtered.length > 0 && (
            <div className="px-6 py-4 border-t border-slate-200 dark:border-slate-800 bg-slate-50/50 dark:bg-slate-800/30 flex items-center justify-between">
              <span className="text-sm text-slate-500">
                显示 {pageStart + 1} - {pageEnd}，共 {filtered.length} 条
              </span>
              <div className="flex items-center gap-1">
                <button
                  onClick={() => setPage(p => Math.max(1, p - 1))}
                  disabled={currentPage <= 1}
                  className="w-8 h-8 flex items-center justify-center rounded border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-800 text-slate-400 hover:text-primary transition-colors disabled:opacity-50"
                >
                  <span className="material-symbols-outlined text-sm">chevron_left</span>
                </button>
                {pageNumbers.map((n, i) =>
                  n === '...' ? (
                    <span key={`dot-${i}`} className="text-slate-400 px-1">...</span>
                  ) : (
                    <button
                      key={n}
                      onClick={() => setPage(n)}
                      className={`w-8 h-8 flex items-center justify-center rounded text-sm font-medium transition-colors ${
                        n === currentPage
                          ? 'bg-primary text-white'
                          : 'border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-800 text-slate-600 dark:text-slate-300 hover:text-primary'
                      }`}
                    >
                      {n}
                    </button>
                  )
                )}
                <button
                  onClick={() => setPage(p => Math.min(totalPages, p + 1))}
                  disabled={currentPage >= totalPages}
                  className="w-8 h-8 flex items-center justify-center rounded border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-800 text-slate-400 hover:text-primary transition-colors disabled:opacity-50"
                >
                  <span className="material-symbols-outlined text-sm">chevron_right</span>
                </button>
              </div>
            </div>
          )}
        </div>

        {/* ─── Summary cards ─── */}
        <div className="grid grid-cols-1 md:grid-cols-3 gap-6 mt-8">
          <div className="bg-white dark:bg-slate-900 p-6 rounded-2xl border border-slate-200 dark:border-slate-800 shadow-sm">
            <div className="flex items-center gap-4">
              <div className="w-12 h-12 bg-blue-50 dark:bg-blue-900/20 text-blue-600 rounded-xl flex items-center justify-center">
                <span className="material-symbols-outlined">analytics</span>
              </div>
              <div>
                <p className="text-sm text-slate-500">基金总数</p>
                <p className="text-2xl font-bold">{stats?.total_funds ?? '—'}</p>
              </div>
            </div>
          </div>
          <div className="bg-white dark:bg-slate-900 p-6 rounded-2xl border border-slate-200 dark:border-slate-800 shadow-sm">
            <div className="flex items-center gap-4">
              <div className="w-12 h-12 bg-amber-50 dark:bg-amber-900/20 text-amber-600 rounded-xl flex items-center justify-center">
                <span className="material-symbols-outlined">warning</span>
              </div>
              <div>
                <p className="text-sm text-slate-500">数据异常</p>
                <p className="text-2xl font-bold">{issueCount > 0 ? issueCount : '0'}</p>
              </div>
            </div>
          </div>
          <div className="bg-white dark:bg-slate-900 p-6 rounded-2xl border border-slate-200 dark:border-slate-800 shadow-sm">
            <div className="flex items-center gap-4">
              <div className="w-12 h-12 bg-purple-50 dark:bg-purple-900/20 text-purple-600 rounded-xl flex items-center justify-center">
                <span className="material-symbols-outlined">category</span>
              </div>
              <div>
                <p className="text-sm text-slate-500">已设策略</p>
                <p className="text-2xl font-bold">{funds.filter(f => f.strategy_l1).length}</p>
              </div>
            </div>
          </div>
        </div>
      </div>

      {/* ─── Compare float panel ─── */}
      {compareList.length > 0 && (
        <ComparePanel
          compareList={compareList}
          onRemove={remove}
          onClear={clear}
          onCompare={() => navigate('/compare/v2')}
        />
      )}
    </>
  )
}

function ComparePanel({ compareList, onRemove, onClear, onCompare }) {
  return (
    <div className="fixed bottom-6 right-6 z-50 w-72 bg-white dark:bg-slate-900 rounded-2xl shadow-2xl border border-slate-200 dark:border-slate-700 flex flex-col overflow-hidden">
      <div className="px-4 py-3 border-b border-slate-100 dark:border-slate-800 flex items-center justify-between">
        <span className="text-sm font-semibold text-slate-700 dark:text-slate-200">
          已选 {compareList.length} / 8 只基金
        </span>
      </div>
      <div className="px-4 py-2 space-y-1.5 max-h-52 overflow-y-auto custom-scrollbar">
        {compareList.map(f => (
          <div key={f.fund_id} className="flex items-center gap-2">
            <span className="flex-1 text-xs text-slate-700 dark:text-slate-300 truncate">{f.product_name}</span>
            <button
              onClick={() => onRemove(f.fund_id)}
              className="shrink-0 text-slate-300 hover:text-rose-500 transition-colors text-base leading-none"
            >×</button>
          </div>
        ))}
      </div>
      <div className="px-4 py-3 border-t border-slate-100 dark:border-slate-800 flex items-center gap-2">
        <button
          onClick={onClear}
          className="flex-1 py-2 text-xs text-slate-500 hover:text-rose-500 border border-slate-200 dark:border-slate-700 rounded-lg transition-colors"
        >
          清空
        </button>
        <button
          onClick={onCompare}
          disabled={compareList.length < 2}
          className="flex-1 py-2 text-xs bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:opacity-40 disabled:cursor-not-allowed transition-colors font-medium"
        >
          基金对比
        </button>
      </div>
    </div>
  )
}
