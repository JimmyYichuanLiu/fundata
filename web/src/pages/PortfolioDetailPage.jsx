import { useAuth } from '../context/AuthContext.jsx'
import PageState from '../components/PageState.jsx'
import { useEffect, useState, useMemo, useRef, useCallback } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import {
  Chart as ChartJS,
  CategoryScale, LinearScale, PointElement, LineElement,
  Title, Tooltip, Legend, Filler,
} from 'chart.js'
import { Line } from 'react-chartjs-2'
import { fetchPortfolioNav, fetchPortfolioMetrics, fetchPortfolio, calculatePortfolio, fetchIndexDaily } from '../api.js'
import { computeMetrics, computeBenchmarkMetrics } from '../utils/metrics.js'
import RangeScrubber from '../components/RangeScrubber.jsx'

ChartJS.register(CategoryScale, LinearScale, PointElement, LineElement, Title, Tooltip, Legend, Filler)

const BENCHMARK_OPTIONS = [
  { label: '无', code: null },
  { label: '中证1000', code: '000852.SH' },
  { label: '中证500', code: '000905.SH' },
  { label: '沪深300', code: '000300.SH' },
  { label: '上证50', code: '000016.SH' },
  { label: '上证指数', code: '000001.SH' },
]

const RANGE_OPTIONS = [
  { label: '近1月', days: 30 },
  { label: '近3月', days: 90 },
  { label: '近6月', days: 180 },
  { label: '近1年', days: 365 },
  { label: '全部', days: 0 },
]

const METRIC_DEFS = [
  { key: 'periodReturn',     label: '区间收益率', format: 'pct' },
  { key: 'annualizedReturn', label: '年化收益率', format: 'pct' },
  { key: 'annualizedVol',    label: '年化波动率', format: 'pct' },
  { key: 'maxDrawdown',      label: '最大回撤',   format: 'pct' },
  { key: 'sharpe',           label: '夏普比率',   format: 'ratio' },
  { key: 'sortino',          label: '索提诺',     format: 'ratio' },
  { key: 'calmar',           label: '卡玛比率',   format: 'ratio' },
  { key: 'monthlyWinRate',   label: '月胜率',     format: 'pct' },
]

function fmt(val, format) {
  if (val == null) return '—'
  if (format === 'pct') return `${val >= 0 ? '+' : ''}${val.toFixed(2)}%`
  if (format === 'ratio') return val.toFixed(3)
  return String(val)
}

function valColor(val, format) {
  if (val == null) return 'text-gray-400'
  if (format === 'pct' || format === 'ratio') {
    if (val > 0) return 'text-red-500'
    if (val < 0) return 'text-emerald-600'
  }
  return 'text-gray-700'
}

function filterByDays(items, days) {
  if (!days || !items.length) return items
  const last = items[items.length - 1].nav_date
  const cutoff = new Date(last)
  cutoff.setDate(cutoff.getDate() - days)
  const cutStr = cutoff.toISOString().slice(0, 10)
  return items.filter(i => i.nav_date >= cutStr)
}

function normalizeToBase(items, key = 'portfolio_nav') {
  if (!items.length) return []
  const base = items[0][key]
  return items.map(i => (base > 0 ? i[key] / base : 1))
}

export default function PortfolioDetailPage() {
  const { canManage } = useAuth()
  const [error, setError] = useState('')
  const { id } = useParams()
  const navigate = useNavigate()
  const chartRef = useRef(null)

  const [portfolio, setPortfolio] = useState(null)
  const [navItems, setNavItems] = useState([])
  const [staleReason, setStaleReason] = useState('')
  const [metrics, setMetrics] = useState(null)
  const [loading, setLoading] = useState(true)
  const [recalculating, setRecalculating] = useState(false)

  const [activeDays, setActiveDays] = useState(0)
  const [isCustomRange, setIsCustomRange] = useState(false)
  const [customFrom, setCustomFrom] = useState('')
  const [customTo, setCustomTo] = useState('')

  const [benchmarkCode, setBenchmarkCode] = useState(null)
  const [benchItems, setBenchItems] = useState([])

  const load = useCallback(() => {
    setLoading(true)
    setError('')
    Promise.all([fetchPortfolio(id), fetchPortfolioNav(id), fetchPortfolioMetrics(id)])
      .then(([p, n, m]) => {
        setPortfolio(p)
        setNavItems(n.items || [])
        setStaleReason(n.stale ? n.reason || '底层净值已变化，需要管理员重新计算组合。' : '')
        setMetrics(m)
      })
      .catch(err => setError(err.message))
      .finally(() => setLoading(false))
  }, [id])

  useEffect(() => { load() }, [load])

  // Load benchmark index data when selected
  useEffect(() => {
    if (!benchmarkCode || !navItems.length) { setBenchItems([]); return }
    const from = navItems[0]?.nav_date
    const to = navItems[navItems.length - 1]?.nav_date
    fetchIndexDaily(benchmarkCode, { date_from: from, date_to: to, limit: 2000 })
      .then(data => {
        const rows = (data.items || []).map(r => {
          const d = r.trade_date ? String(r.trade_date) : ''
          const navDate = d.length === 8 ? `${d.slice(0, 4)}-${d.slice(4, 6)}-${d.slice(6, 8)}` : (r.nav_date || '')
          return {
            nav_date: navDate,
            unit_nav: r.close,
          }
        }).filter(r => r.nav_date)
        setBenchItems(rows)
      })
      .catch(() => setBenchItems([]))
  }, [benchmarkCode, navItems])

  const filteredItems = useMemo(() => {
    if (isCustomRange && customFrom && customTo) {
      return navItems.filter(i => i.nav_date >= customFrom && i.nav_date <= customTo)
    }
    return filterByDays(navItems, activeDays)
  }, [navItems, activeDays, isCustomRange, customFrom, customTo])

  // Align benchmark to filtered nav dates
  const alignedBench = useMemo(() => {
    if (!benchItems.length || !filteredItems.length) return []
    const benchMap = Object.fromEntries(benchItems.map(b => [b.nav_date, b.unit_nav]))
    return filteredItems.map(i => ({ nav_date: i.nav_date, unit_nav: benchMap[i.nav_date] ?? null }))
  }, [benchItems, filteredItems])

  const fundNorm = useMemo(() => normalizeToBase(filteredItems), [filteredItems])
  const benchNorm = useMemo(() => {
    if (!alignedBench.length) return []
    const first = alignedBench.find(i => i.unit_nav != null)?.unit_nav
    if (!first) return []
    return alignedBench.map(i => (i.unit_nav != null ? i.unit_nav / first : null))
  }, [alignedBench])

  const fundMetrics = useMemo(() => {
    const series = filteredItems.map(i => ({ nav_date: i.nav_date, unit_nav: i.portfolio_nav }))
    return computeMetrics(series, 'unit')
  }, [filteredItems])

  const benchMetrics = useMemo(() => {
    if (!alignedBench.length || !benchmarkCode) return null
    const valid = alignedBench.filter(i => i.unit_nav != null)
    return computeMetrics(valid, 'unit')
  }, [alignedBench, benchmarkCode])

  const relMetrics = useMemo(() => {
    if (!alignedBench.length || !benchmarkCode) return null
    const fundSeries = filteredItems.map(i => ({ nav_date: i.nav_date, unit_nav: i.portfolio_nav }))
    const valid = alignedBench.filter(i => i.unit_nav != null)
    return computeBenchmarkMetrics(fundSeries, valid, 'unit')
  }, [filteredItems, alignedBench, benchmarkCode])

  const allDates = useMemo(() => navItems.map(i => i.nav_date), [navItems])
  const scrubStart = useMemo(() => {
    if (!filteredItems.length || !navItems.length) return 0
    const first = filteredItems[0].nav_date
    return Math.max(0, navItems.findIndex(i => i.nav_date >= first))
  }, [filteredItems, navItems])
  const scrubEnd = useMemo(() => {
    if (!filteredItems.length || !navItems.length) return navItems.length - 1
    const last = filteredItems[filteredItems.length - 1].nav_date
    let idx = navItems.length - 1
    for (let i = navItems.length - 1; i >= 0; i--) {
      if (navItems[i].nav_date <= last) { idx = i; break }
    }
    return idx
  }, [filteredItems, navItems])

  const handleScrub = useCallback((s, e) => {
    if (!navItems.length) return
    setIsCustomRange(true)
    setActiveDays(0)
    setCustomFrom(navItems[s]?.nav_date || '')
    setCustomTo(navItems[e]?.nav_date || '')
  }, [navItems])

  const chartData = useMemo(() => {
    const labels = filteredItems.map(i => i.nav_date)
    const datasets = [{
      label: portfolio?.portfolio_name || '组合净值',
      data: fundNorm,
      borderColor: '#3b82f6',
      backgroundColor: 'rgba(59,130,246,0.08)',
      fill: true,
      tension: 0.3,
      pointRadius: 0,
      pointHoverRadius: 4,
      borderWidth: 2,
    }]
    if (benchNorm.length && benchmarkCode) {
      const bLabel = BENCHMARK_OPTIONS.find(o => o.code === benchmarkCode)?.label || benchmarkCode
      datasets.push({
        label: bLabel,
        data: benchNorm,
        borderColor: '#9ca3af',
        backgroundColor: 'transparent',
        fill: false,
        tension: 0.3,
        pointRadius: 0,
        pointHoverRadius: 4,
        borderWidth: 1.5,
        borderDash: [5, 5],
        spanGaps: true,
      })
    }
    return { labels, datasets }
  }, [filteredItems, fundNorm, benchNorm, benchmarkCode, portfolio])

  const chartOptions = useMemo(() => ({
    responsive: true,
    maintainAspectRatio: false,
    animation: { duration: 0 },
    interaction: { mode: 'index', intersect: false },
    plugins: {
      legend: { display: !!benchmarkCode },
      tooltip: {
        callbacks: {
          label: (item) => {
            const v = Number(item.raw)
            const sign = v >= 1 ? '+' : ''
            return `${item.dataset.label}: ${sign}${((v - 1) * 100).toFixed(2)}%`
          },
        },
      },
    },
    scales: {
      x: {
        grid: { display: false },
        ticks: { maxTicksLimit: 8, maxRotation: 0, font: { size: 11 }, color: '#9ca3af' },
      },
      y: {
        position: 'right',
        grid: { color: '#f3f4f6' },
        ticks: {
          callback: v => `${((Number(v) - 1) * 100).toFixed(1)}%`,
          font: { size: 11 },
          color: '#9ca3af',
        },
      },
    },
  }), [benchmarkCode])

  const handleRecalculate = async () => {
    setRecalculating(true)
    try {
      await calculatePortfolio(id)
      load()
    } catch (err) {
      setError(err.message)
    } finally {
      setRecalculating(false)
    }
  }

  if (loading) {
    return (
      <div className="p-4 md:p-8 space-y-4">
        <div className="shimmer h-8 w-48 rounded" />
        <div className="shimmer h-72 rounded-xl" />
        <div className="shimmer h-40 rounded-xl" />
      </div>
    )
  }

  if (error) return <PageState error={error} onRetry={load} />

  if (!portfolio) {
    return <div className="p-8 text-gray-400">组合不存在</div>
  }

  const buildMethodLabel = portfolio.build_method === 'BATCH_INCLUDE' ? '分批纳入法' : '统一起始日法'

  return (
    <div className="p-4 md:p-8 space-y-4">
      {/* Header */}
      <div className="flex items-start justify-between gap-4">
        <div>
          <button onClick={() => navigate(-1)} className="text-xs text-gray-400 hover:text-gray-600 mb-1">← 返回</button>
          <h1 className="text-xl font-bold text-gray-900">{portfolio.portfolio_name}</h1>
          <p className="text-xs text-gray-400 mt-1">
            {buildMethodLabel} · {portfolio.constituents?.length ?? 0} 只基金 · {navItems.length} 个净值点
          </p>
        </div>
        <button
          hidden={!canManage}
          onClick={handleRecalculate}
          disabled={recalculating}
          className="px-3 py-1.5 text-xs rounded-lg border border-blue-200 text-blue-600 hover:bg-blue-50 disabled:opacity-50 whitespace-nowrap"
        >
          {recalculating ? '重算中…' : '重新计算'}
        </button>
      </div>

      {staleReason && <div className="notice" role="status">{staleReason}</div>}
      {/* Summary metric cards */}
      {fundMetrics && (
        <div className="grid grid-cols-2 md:grid-cols-5 gap-3">
          {[
            { label: '累计收益', val: fundMetrics.periodReturn, format: 'pct' },
            { label: '年化收益', val: fundMetrics.annualizedReturn, format: 'pct' },
            { label: '最大回撤', val: fundMetrics.maxDrawdown, format: 'pct' },
            { label: '年化波动', val: fundMetrics.annualizedVol, format: 'pct' },
            { label: '夏普比率', val: fundMetrics.sharpe, format: 'ratio' },
          ].map(({ label, val, format }) => (
            <div key={label} className="bg-white rounded-xl shadow p-3">
              <p className="text-xs text-gray-400 mb-1">{label}</p>
              <p className={`text-base font-semibold font-mono ${valColor(val, format)}`}>
                {fmt(val, format)}
              </p>
            </div>
          ))}
        </div>
      )}

      {/* Chart */}
      <div className="bg-white rounded-xl shadow p-4">
        {/* Range controls */}
        <div className="flex flex-wrap items-center gap-2 mb-4">
          <div className="flex gap-1 flex-wrap">
            {RANGE_OPTIONS.map(opt => (
              <button
                key={opt.days}
                onClick={() => { setActiveDays(opt.days); setIsCustomRange(false) }}
                className={`px-3 py-1 rounded-full text-xs font-medium transition-colors ${
                  !isCustomRange && activeDays === opt.days
                    ? 'bg-blue-600 text-white'
                    : 'bg-gray-100 text-gray-600 hover:bg-gray-200'
                }`}
              >
                {opt.label}
              </button>
            ))}
          </div>
          <div className="ml-auto flex items-center gap-2">
            <span className="text-xs text-gray-400">基准</span>
            <select
              value={benchmarkCode || ''}
              onChange={e => setBenchmarkCode(e.target.value || null)}
              className="text-xs border border-gray-200 rounded px-2 py-1 focus:outline-none focus:ring-1 focus:ring-blue-400"
            >
              {BENCHMARK_OPTIONS.map(o => (
                <option key={o.code ?? ''} value={o.code ?? ''}>{o.label}</option>
              ))}
            </select>
          </div>
        </div>

        {isCustomRange && (
          <div className="flex items-center gap-3 mb-4 text-xs text-gray-600">
            <input type="date" value={customFrom} onChange={e => setCustomFrom(e.target.value)}
              className="border border-gray-300 rounded px-2 py-1 text-xs" />
            <span>—</span>
            <input type="date" value={customTo} onChange={e => setCustomTo(e.target.value)}
              className="border border-gray-300 rounded px-2 py-1 text-xs" />
          </div>
        )}

        <div className="h-72">
          {filteredItems.length === 0
            ? <div className="h-full flex items-center justify-center text-gray-400 text-sm">暂无有效组合净值。需要管理员检查成分区间并重新计算。</div>
            : <Line ref={chartRef} data={chartData} options={chartOptions} />
          }
        </div>

        {allDates.length > 1 && (
          <RangeScrubber dates={allDates} startIdx={scrubStart} endIdx={scrubEnd} onChange={handleScrub} />
        )}
      </div>

      {/* Metrics table */}
      <div className="bg-white rounded-xl shadow p-4">
        <h2 className="text-sm font-semibold text-gray-700 mb-3">绩效指标</h2>
        <div className="overflow-x-auto">
          <table className="w-full text-xs">
            <thead>
              <tr className="border-b border-gray-200 text-gray-400">
                <th className="text-left py-2 pr-4 font-medium">指标</th>
                <th className="text-right py-2 px-3 font-medium">组合</th>
                {benchmarkCode && <th className="text-right py-2 px-3 font-medium">基准</th>}
              </tr>
            </thead>
            <tbody>
              {METRIC_DEFS.map(({ key, label, format }) => {
                const fv = fundMetrics?.[key]
                const bv = benchMetrics?.[key]
                return (
                  <tr key={key} className="border-b border-gray-50 hover:bg-gray-50">
                    <td className="py-2 pr-4 text-gray-600 whitespace-nowrap">{label}</td>
                    <td className={`py-2 px-3 text-right font-mono ${valColor(fv, format)}`}>{fmt(fv, format)}</td>
                    {benchmarkCode && <td className={`py-2 px-3 text-right font-mono ${valColor(bv, format)}`}>{fmt(bv, format)}</td>}
                  </tr>
                )
              })}
              {relMetrics && benchmarkCode && (
                <>
                  <tr className="border-b border-gray-100">
                    <td className="py-2 pr-4 text-gray-600">相关系数</td>
                    <td className={`py-2 px-3 text-right font-mono ${valColor(relMetrics.correlation, 'ratio')}`}>{fmt(relMetrics.correlation, 'ratio')}</td>
                    <td className="py-2 px-3 text-right text-gray-300">—</td>
                  </tr>
                  <tr className="border-b border-gray-100">
                    <td className="py-2 pr-4 text-gray-600">Beta</td>
                    <td className={`py-2 px-3 text-right font-mono ${valColor(relMetrics.beta, 'ratio')}`}>{fmt(relMetrics.beta, 'ratio')}</td>
                    <td className="py-2 px-3 text-right text-gray-300">—</td>
                  </tr>
                  <tr className="border-b border-gray-100">
                    <td className="py-2 pr-4 text-gray-600">Alpha</td>
                    <td className={`py-2 px-3 text-right font-mono ${valColor(relMetrics.alpha, 'pct')}`}>{fmt(relMetrics.alpha, 'pct')}</td>
                    <td className="py-2 px-3 text-right text-gray-300">—</td>
                  </tr>
                </>
              )}
            </tbody>
          </table>
        </div>
      </div>

      {/* Constituents table */}
      {portfolio.constituents?.length > 0 && (
        <div className="bg-white rounded-xl shadow p-4">
          <h2 className="text-sm font-semibold text-gray-700 mb-3">成分明细</h2>
          <div className="overflow-x-auto">
            <table className="w-full text-xs">
              <thead>
                <tr className="border-b border-gray-200 text-gray-400">
                  <th className="text-left py-2 font-medium">纳入顺序</th>
                  <th className="text-left py-2 font-medium">基金代码</th>
                  <th className="text-right py-2 font-medium">
                    {portfolio.build_method === 'BATCH_INCLUDE' ? '终态金额' : '目标权重'}
                  </th>
                  <th className="text-right py-2 font-medium">生效日</th>
                </tr>
              </thead>
              <tbody>
                {portfolio.constituents.map(c => (
                  <tr key={c.fund_id} className="border-b border-gray-50 hover:bg-gray-50">
                    <td className="py-2 text-gray-400">{c.include_order}</td>
                    <td className="py-2 text-gray-700 font-mono">{c.fund_code}</td>
                    <td className="py-2 text-right font-mono text-gray-700">
                      {portfolio.build_method === 'BATCH_INCLUDE'
                        ? (c.target_amount != null ? c.target_amount.toLocaleString() : '—')
                        : (c.target_weight != null ? `${(c.target_weight * 100).toFixed(1)}%` : '—')
                      }
                    </td>
                    <td className="py-2 text-right text-gray-500">{c.effective_date}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  )
}
