import { useState, useEffect, useCallback, useRef, useMemo } from 'react'
import { useNavigate } from 'react-router-dom'
import {
  fetchStats, fetchFunds, fetchAllIssues,
  fetchSyncStatus, triggerSync, fetchFailures, fetchFundReturns, fetchFundMetrics,
  updateFundStrategy, triggerExcelImport, fetchExcelConflicts,
} from '../api.js'
import { useCompare } from '../context/CompareContext.jsx'

// 三级策略枚举（来自臻选货架实际数据）
const STRATEGY1_OPTIONS = [
  'ETF','FOF策略','主观期货','主观股票','债券','债券增强','可转债',
  '商品套利','复合策略','复合策略-低波动','复合策略-高波动','宏观对冲策略',
  '打板','打板+强势股','期权','股票对冲','量化期货','量化股票',
]
const STRATEGY2_OPTIONS = [
  '1000指增','2000指增','300指增','500指增','A500指增','Delta中性套利',
  '多策略FOF','量化股票中性','量化多头','套利','价值','成长','行业','全市场选股',
]
const STRATEGY3_OPTIONS = [
  '0-100','T0','wind小市值','中证2000','中证500','低位板','低波动','医疗',
  '反转','周期成长','套利系列','时序','杠杆','消费','混合对冲','港股',
  '短线交易','精选个股','行业轮动','趋势系列','首板','高频',
]

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

// ── Strategy badge ──
function StrategyBadge({ label, color = 'slate' }) {
  const colors = {
    blue: 'bg-blue-50 text-blue-700 dark:bg-blue-900/30 dark:text-blue-300',
    violet: 'bg-violet-50 text-violet-700 dark:bg-violet-900/30 dark:text-violet-300',
    slate: 'bg-slate-100 text-slate-600 dark:bg-slate-700 dark:text-slate-300',
  }
  return (
    <span className={`inline-flex items-center px-1.5 py-0.5 rounded text-[10px] font-medium ${colors[color] || colors.slate}`}>
      {label}
    </span>
  )
}

// ── Strategy editor floating panel ──
function StrategyEditor({ fund, onClose, onSave }) {
  const [s1, setS1] = useState(fund.strategy1 || '')
  const [s2, setS2] = useState(fund.strategy2 || '')
  const [s3, setS3] = useState(() => {
    const raw = fund.strategy3 || ''
    return new Set(raw ? raw.split(',').map(s => s.trim()).filter(Boolean) : [])
  })
  const [saving, setSaving] = useState(false)

  const toggleS3 = (val) => {
    setS3(prev => {
      const next = new Set(prev)
      if (next.has(val)) next.delete(val)
      else next.add(val)
      return next
    })
  }

  const handleSave = async () => {
    setSaving(true)
    try {
      await onSave(fund.fund_id, {
        strategy1: s1 || null,
        strategy2: s2 || null,
        strategy3: [...s3].join(',') || null,
      })
      onClose()
    } catch (err) {
      alert(err.message)
    } finally {
      setSaving(false)
    }
  }

  return (
    <div
      className="absolute z-50 bg-white dark:bg-slate-800 rounded-xl shadow-xl border border-slate-200 dark:border-slate-700 p-4 w-72"
      onClick={e => e.stopPropagation()}
    >
      <div className="flex items-center justify-between mb-3">
        <span className="text-xs font-semibold text-slate-600 dark:text-slate-300">策略标签</span>
        <button onClick={onClose} className="text-slate-400 hover:text-slate-600 dark:hover:text-slate-200 text-base leading-none">×</button>
      </div>
      <div className="space-y-3">
        <div>
          <label className="text-[10px] font-medium text-slate-500 uppercase tracking-wider">一级（单选）</label>
          <select
            value={s1}
            onChange={e => setS1(e.target.value)}
            className="mt-1 w-full text-xs border border-slate-200 dark:border-slate-600 rounded-lg px-2 py-1.5 bg-white dark:bg-slate-700 focus:outline-none focus:ring-2 focus:ring-primary/20"
          >
            <option value="">— 不设置 —</option>
            {STRATEGY1_OPTIONS.map(o => <option key={o} value={o}>{o}</option>)}
          </select>
        </div>
        <div>
          <label className="text-[10px] font-medium text-slate-500 uppercase tracking-wider">二级（单选）</label>
          <select
            value={s2}
            onChange={e => setS2(e.target.value)}
            className="mt-1 w-full text-xs border border-slate-200 dark:border-slate-600 rounded-lg px-2 py-1.5 bg-white dark:bg-slate-700 focus:outline-none focus:ring-2 focus:ring-primary/20"
          >
            <option value="">— 不设置 —</option>
            {STRATEGY2_OPTIONS.map(o => <option key={o} value={o}>{o}</option>)}
          </select>
        </div>
        <div>
          <label className="text-[10px] font-medium text-slate-500 uppercase tracking-wider">三级（多选）</label>
          <div className="mt-1 flex flex-wrap gap-1 max-h-28 overflow-y-auto">
            {STRATEGY3_OPTIONS.map(o => (
              <button
                key={o}
                type="button"
                onClick={() => toggleS3(o)}
                className={`px-1.5 py-0.5 rounded text-[10px] font-medium border transition-colors ${
                  s3.has(o)
                    ? 'bg-primary text-white border-primary'
                    : 'bg-white dark:bg-slate-700 text-slate-600 dark:text-slate-300 border-slate-200 dark:border-slate-600 hover:border-primary'
                }`}
              >
                {o}
              </button>
            ))}
          </div>
        </div>
      </div>
      <div className="flex justify-end gap-2 mt-3">
        <button onClick={onClose} className="px-3 py-1.5 text-xs text-slate-500 hover:text-slate-700 dark:hover:text-slate-200">取消</button>
        <button
          onClick={handleSave}
          disabled={saving}
          className="px-3 py-1.5 text-xs bg-primary text-white rounded-lg hover:bg-primary/90 disabled:opacity-50"
        >
          {saving ? '保存中…' : '保存'}
        </button>
      </div>
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

// ── Main component ──
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

  // 策略筛选 state
  const [filterS1, setFilterS1] = useState('')
  const [filterS2, setFilterS2] = useState('')
  const [filterS3, setFilterS3] = useState('')
  const [showStrategyFilter, setShowStrategyFilter] = useState(false)

  // 策略编辑 state
  const [strategyEditor, setStrategyEditor] = useState(null) // { fundId }

  // Excel 导入 state
  const [importing, setImporting] = useState(false)
  const [conflicts, setConflicts] = useState({ total: 0, items: [] })
  const [showConflicts, setShowConflicts] = useState(false)

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

    const filters = {}
    if (filterS1) filters.strategy1 = filterS1
    if (filterS2) filters.strategy2 = filterS2
    if (filterS3) filters.strategy3 = filterS3

    Promise.all([fetchStats(signal), fetchFunds(signal, filters)])
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
    fetchFundReturns({ periods: periodsStr, strategy1: filterS1 || undefined }, signal)
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

    fetchExcelConflicts(signal)
      .then(data => setConflicts(data))
      .catch(() => {})

    return () => controller.abort()
  }, [retryCount, filterS1, filterS2, filterS3, visibleCols])

  // ── Fetch metrics when any metrics column is visible ──
  useEffect(() => {
    const needsMetrics = COLUMN_DEFS.some(c => c.category === 'metrics' && visibleCols.has(c.key))
    if (!needsMetrics) return
    const controller = new AbortController()
    fetchFundMetrics({ period: 'all', strategy1: filterS1 || undefined }, controller.signal)
      .then(data => setFundMetrics(data.items || {}))
      .catch(err => { if (err.name !== 'AbortError') console.warn('metrics load failed', err) })
    return () => controller.abort()
  }, [visibleCols, filterS1])

  // ── Close col picker on outside click ──
  useEffect(() => {
    if (!showColPicker) return
    function handler(e) {
      if (colPickerRef.current && !colPickerRef.current.contains(e.target)) setShowColPicker(false)
    }
    document.addEventListener('mousedown', handler)
    return () => document.removeEventListener('mousedown', handler)
  }, [showColPicker])

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

  useEffect(() => {
    if (!strategyEditor) return
    const handler = () => setStrategyEditor(null)
    document.addEventListener('click', handler)
    return () => document.removeEventListener('click', handler)
  }, [strategyEditor])

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

  // ── Excel 导入 ──
  const handleExcelImport = useCallback(async () => {
    if (importing) return
    setImporting(true)
    try {
      await triggerExcelImport()
      // 轮询等待完成
      setTimeout(() => {
        setImporting(false)
        setRetryCount(c => c + 1)
      }, 3000)
    } catch (err) {
      console.warn('excel import failed', err)
      setImporting(false)
    }
  }, [importing])

  // ── 策略标签保存 ──
  const handleSaveStrategy = useCallback(async (fundId, strategy) => {
    await updateFundStrategy(fundId, strategy)
    setFunds(prev => prev.map(f =>
      f.fund_id === fundId ? { ...f, ...strategy } : f
    ))
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
    const base = debouncedSearch
      ? funds.filter(f =>
          (f.product_name || '').includes(debouncedSearch) ||
          (f.product_code || '').includes(debouncedSearch)
        )
      : funds
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
  }, [funds, debouncedSearch, sortKey, sortDir, fundReturns, fundMetrics])

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

      {/* Conflicts modal */}
      {showConflicts && (
        <div
          className="fixed inset-0 bg-black/40 z-50 flex items-center justify-center p-4"
          onClick={() => setShowConflicts(false)}
        >
          <div
            className="bg-white dark:bg-slate-900 rounded-2xl shadow-xl max-w-3xl w-full max-h-[80vh] flex flex-col border border-slate-200 dark:border-slate-800"
            onClick={e => e.stopPropagation()}
          >
            <div className="flex items-center justify-between px-6 py-4 border-b border-slate-200 dark:border-slate-800">
              <h2 className="text-base font-semibold">
                数据冲突记录（{conflicts.total} 条）— Excel 数据已覆盖邮件数据
              </h2>
              <button onClick={() => setShowConflicts(false)} className="text-slate-400 hover:text-slate-600 dark:hover:text-slate-200 text-xl leading-none">×</button>
            </div>
            <div className="overflow-auto flex-1 custom-scrollbar">
              <table className="w-full text-xs">
                <thead className="bg-slate-50 dark:bg-slate-800/50 sticky top-0">
                  <tr className="text-left text-slate-500 border-b border-slate-200 dark:border-slate-800">
                    <th className="px-4 py-2 font-medium">基金代码</th>
                    <th className="px-4 py-2 font-medium">净值日期</th>
                    <th className="px-4 py-2 font-medium">邮件净值</th>
                    <th className="px-4 py-2 font-medium">Excel净值</th>
                    <th className="px-4 py-2 font-medium">检测时间</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-slate-100 dark:divide-slate-800">
                  {conflicts.items.map(item => (
                    <tr key={item.id} className="hover:bg-slate-50 dark:hover:bg-slate-800/50">
                      <td className="px-4 py-2 font-mono">{item['产品代码']}</td>
                      <td className="px-4 py-2">{item['净值日期']}</td>
                      <td className="px-4 py-2 text-slate-500">{item.email_unit_nav?.toFixed(4) || '—'}</td>
                      <td className="px-4 py-2 text-rose-600 font-medium">{item.excel_unit_nav?.toFixed(4) || '—'}</td>
                      <td className="px-4 py-2 text-slate-400">{item.detected_at?.slice(0, 16) || '—'}</td>
                    </tr>
                  ))}
                  {conflicts.items.length === 0 && (
                    <tr><td colSpan={5} className="px-4 py-8 text-center text-slate-400">暂无冲突记录</td></tr>
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
          {conflicts.total > 0 && (
            <button
              onClick={() => setShowConflicts(true)}
              className="flex items-center gap-1.5 px-3 py-1.5 text-xs bg-rose-50 dark:bg-rose-900/20 text-rose-600 border border-rose-200 dark:border-rose-800 rounded-lg hover:bg-rose-100 dark:hover:bg-rose-900/40 transition-colors"
            >
              <span className="material-symbols-outlined text-[16px]">sync_problem</span>
              数据冲突 {conflicts.total}
            </button>
          )}
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
            onClick={handleExcelImport}
            disabled={importing}
            className="bg-emerald-600 hover:bg-emerald-700 text-white px-4 py-2 rounded-lg font-medium shadow-md shadow-emerald-600/20 transition-all flex items-center gap-2 disabled:opacity-50 disabled:cursor-not-allowed text-sm"
          >
            <span className={`material-symbols-outlined text-[18px] ${importing ? 'animate-spin' : ''}`}>upload_file</span>
            {importing ? '导入中…' : '导入Excel'}
          </button>
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

        {/* ─── Filters & Search ─── */}
        <div className="flex flex-col md:flex-row md:items-center justify-between mb-6 gap-4">
          <div className="flex items-center gap-2 flex-wrap">
            <span className="text-sm font-medium text-slate-500 mr-1">策略筛选:</span>
            {/* 一级筛选 */}
            <select
              value={filterS1}
              onChange={e => { setFilterS1(e.target.value); setPage(1) }}
              className="text-xs border border-slate-200 dark:border-slate-700 rounded-lg px-2 py-1.5 bg-white dark:bg-slate-800 focus:outline-none focus:ring-2 focus:ring-primary/20 max-w-[130px]"
            >
              <option value="">全部一级</option>
              {STRATEGY1_OPTIONS.map(o => <option key={o} value={o}>{o}</option>)}
            </select>
            {/* 二级筛选 */}
            <select
              value={filterS2}
              onChange={e => { setFilterS2(e.target.value); setPage(1) }}
              className="text-xs border border-slate-200 dark:border-slate-700 rounded-lg px-2 py-1.5 bg-white dark:bg-slate-800 focus:outline-none focus:ring-2 focus:ring-primary/20 max-w-[130px]"
            >
              <option value="">全部二级</option>
              {STRATEGY2_OPTIONS.map(o => <option key={o} value={o}>{o}</option>)}
            </select>
            {/* 三级筛选 */}
            <select
              value={filterS3}
              onChange={e => { setFilterS3(e.target.value); setPage(1) }}
              className="text-xs border border-slate-200 dark:border-slate-700 rounded-lg px-2 py-1.5 bg-white dark:bg-slate-800 focus:outline-none focus:ring-2 focus:ring-primary/20 max-w-[110px]"
            >
              <option value="">全部三级</option>
              {STRATEGY3_OPTIONS.map(o => <option key={o} value={o}>{o}</option>)}
            </select>
            {(filterS1 || filterS2 || filterS3) && (
              <button
                onClick={() => { setFilterS1(''); setFilterS2(''); setFilterS3(''); setPage(1) }}
                className="text-xs text-slate-400 hover:text-rose-500 transition-colors"
                title="清除筛选"
              >
                清除
              </button>
            )}
          </div>
          <div className="flex items-center gap-2">
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
                              <div className="flex items-center gap-1 mt-0.5 flex-wrap" onClick={e => e.stopPropagation()}>
                                {fund.strategy1 && <StrategyBadge label={fund.strategy1} color="blue" />}
                                {fund.strategy2 && <StrategyBadge label={fund.strategy2} color="violet" />}
                                {fund.strategy3 && fund.strategy3.split(',').filter(Boolean).map(s => (
                                  <StrategyBadge key={s} label={s.trim()} color="slate" />
                                ))}
                                <button
                                  className="w-4 h-4 rounded border border-dashed border-slate-300 dark:border-slate-600 text-slate-400 hover:border-primary hover:text-primary text-[10px] leading-none flex items-center justify-center"
                                  onClick={e => { e.stopPropagation(); setStrategyEditor(prev => prev?.fundId === fund.fund_id ? null : { fundId: fund.fund_id }) }}
                                  title="编辑策略标签"
                                >
                                  <span className="material-symbols-outlined text-[12px]">edit</span>
                                </button>
                                {strategyEditor?.fundId === fund.fund_id && (
                                  <div className="absolute mt-1 z-50">
                                    <StrategyEditor
                                      fund={fund}
                                      onClose={() => setStrategyEditor(null)}
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
                <span className="material-symbols-outlined">label</span>
              </div>
              <div>
                <p className="text-sm text-slate-500">标签分类</p>
                <p className="text-2xl font-bold">{allTags.length}</p>
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
