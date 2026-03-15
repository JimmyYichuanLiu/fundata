import { useState, useEffect, useRef, useMemo, useCallback } from 'react'
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Tooltip,
  Legend,
} from 'chart.js'
import { Line } from 'react-chartjs-2'
import { fetchFunds, fetchCompare, subtractDays } from '../api.js'
import { computeMetrics } from '../utils/metrics.js'

ChartJS.register(CategoryScale, LinearScale, PointElement, LineElement, Tooltip, Legend)

const RANGE_OPTIONS = [
  { label: '近3月', days: 90 },
  { label: '近6月', days: 180 },
  { label: '近1年', days: 365 },
  { label: '近3年', days: 1095 },
  { label: '全部', days: 0 },
]

const COLORS = [
  '#3b82f6', '#ef4444', '#10b981', '#f59e0b', '#8b5cf6',
  '#06b6d4', '#f97316', '#84cc16', '#ec4899', '#6366f1',
]

const TABLE_METRICS = [
  { key: 'periodReturn',     label: '区间收益',   format: 'pct' },
  { key: 'annualizedReturn', label: '年化收益',   format: 'pct' },
  { key: 'annualizedVol',    label: '年化波动',   format: 'pct' },
  { key: 'maxDrawdown',      label: '最大回撤',   format: 'pct' },
  { key: 'sharpe',           label: '夏普',       format: 'ratio' },
  { key: 'monthlyWinRate',   label: '月胜率',     format: 'pct' },
]

function formatMetric(val, format) {
  if (val == null) return '—'
  if (format === 'pct') return `${val.toFixed(2)}%`
  if (format === 'ratio') return val.toFixed(3)
  if (format === 'days') return `${Math.round(val)}天`
  return String(val)
}

function metricCellClass(val, format, key) {
  if (val == null) return 'text-gray-300'
  if (format === 'days') return 'text-gray-600'
  // maxDrawdown is always negative; closer to 0 = better
  if (key === 'maxDrawdown') {
    return val >= -5 ? 'text-emerald-600' : val >= -15 ? 'text-yellow-600' : 'text-red-500'
  }
  if (val > 0) return 'text-emerald-600'
  if (val < 0) return 'text-red-500'
  return 'text-gray-600'
}

export default function FundComparison() {
  const [allFunds, setAllFunds] = useState([])
  const [selectedFunds, setSelectedFunds] = useState([])
  const [search, setSearch] = useState('')
  const [showDropdown, setShowDropdown] = useState(false)
  const [activeDays, setActiveDays] = useState(365)
  const [isCustomRange, setIsCustomRange] = useState(false)
  const [customFrom, setCustomFrom] = useState('')
  const [customTo, setCustomTo] = useState('')
  const [normalize, setNormalize] = useState(true)
  const [compareData, setCompareData] = useState(null)
  const [loading, setLoading] = useState(false)

  const searchRef = useRef(null)
  const dropdownRef = useRef(null)

  // Load all funds once
  useEffect(() => {
    fetchFunds().then(setAllFunds).catch(console.warn)
  }, [])

  // Close dropdown on outside click
  useEffect(() => {
    function handleClickOutside(e) {
      if (
        dropdownRef.current && !dropdownRef.current.contains(e.target) &&
        searchRef.current && !searchRef.current.contains(e.target)
      ) {
        setShowDropdown(false)
      }
    }
    document.addEventListener('mousedown', handleClickOutside)
    return () => document.removeEventListener('mousedown', handleClickOutside)
  }, [])

  // Date params derived from range selection
  const dateParams = useMemo(() => {
    if (isCustomRange) {
      return { date_from: customFrom || undefined, date_to: customTo || undefined }
    }
    if (activeDays === 0) return {}
    const today = new Date().toISOString().slice(0, 10)
    const from = subtractDays(today, activeDays)
    return from ? { date_from: from } : {}
  }, [isCustomRange, customFrom, customTo, activeDays])

  // Fetch compare data when selection or date range changes
  useEffect(() => {
    if (selectedFunds.length === 0) {
      setCompareData(null)
      return
    }
    const controller = new AbortController()
    setLoading(true)
    fetchCompare(
      selectedFunds.map(f => f.fund_id),
      dateParams,
      controller.signal,
    )
      .then(data => {
        setCompareData(data)
        setLoading(false)
      })
      .catch(err => {
        if (err.name !== 'AbortError') setLoading(false)
      })
    return () => controller.abort()
  }, [selectedFunds, dateParams])

  // Search candidates (max 20, excluding already selected)
  const selectedIds = useMemo(() => new Set(selectedFunds.map(f => f.fund_id)), [selectedFunds])

  const candidates = useMemo(() => {
    if (!search.trim()) return []
    const q = search.toLowerCase()
    return allFunds
      .filter(f =>
        ((f.product_name || '').toLowerCase().includes(q) ||
         (f.product_code || '').toLowerCase().includes(q))
      )
      .slice(0, 20)
  }, [allFunds, search])

  const addFund = useCallback((fund) => {
    if (selectedFunds.length >= 10) return
    if (selectedIds.has(fund.fund_id)) return
    setSelectedFunds(prev => [...prev, fund])
    setSearch('')
    setShowDropdown(false)
  }, [selectedFunds.length, selectedIds])

  const removeFund = useCallback((fundId) => {
    setSelectedFunds(prev => prev.filter(f => f.fund_id !== fundId))
  }, [])

  // Build chart dataset
  const chartData = useMemo(() => {
    if (!compareData || selectedFunds.length === 0) return null

    // Collect all unique dates across funds
    const dateSet = new Set()
    selectedFunds.forEach(fund => {
      const series = compareData.funds?.[fund.fund_id]?.series || []
      series.forEach(s => dateSet.add(s.date))
    })
    const allDates = Array.from(dateSet).sort()

    const datasets = selectedFunds.map((fund, idx) => {
      const series = compareData.funds?.[fund.fund_id]?.series || []
      const seriesMap = new Map(series.map(s => [s.date, s.nav]))
      const baseVal = normalize && series.length > 0 ? series[0].nav : null

      const vals = allDates.map(date => {
        const v = seriesMap.get(date)
        if (v == null) return null
        if (normalize && baseVal != null && baseVal > 0) return v / baseVal * 100
        return v
      })

      return {
        label: fund.product_name || fund.product_code,
        data: vals,
        borderColor: COLORS[idx % COLORS.length],
        backgroundColor: 'transparent',
        fill: false,
        tension: 0.3,
        pointRadius: 0,
        pointHoverRadius: 4,
        borderWidth: 2,
        spanGaps: true,
      }
    })

    return { labels: allDates, datasets }
  }, [compareData, selectedFunds, normalize])

  // Chart options
  const chartOptions = useMemo(() => ({
    responsive: true,
    maintainAspectRatio: false,
    animation: { duration: 0 },
    interaction: { mode: 'index', intersect: false },
    plugins: {
      legend: { display: true, position: 'top', labels: { boxWidth: 12, font: { size: 11 } } },
      tooltip: {
        callbacks: {
          title: items => items[0]?.label || '',
          label: item => {
            const val = Number(item.raw)
            return `${item.dataset.label}: ${normalize ? val.toFixed(2) : val.toFixed(4)}`
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
          callback: v => normalize ? Number(v).toFixed(1) : Number(v).toFixed(4),
          font: { size: 11 },
          color: '#9ca3af',
        },
      },
    },
  }), [normalize])

  // Metrics per fund for comparison table
  const metricsTable = useMemo(() => {
    if (!compareData) return []
    return selectedFunds.map((fund, idx) => {
      const series = compareData.funds?.[fund.fund_id]?.series || []
      const items = series.map(s => ({ nav_date: s.date, unit_nav: s.nav }))
      return {
        fund,
        color: COLORS[idx % COLORS.length],
        metrics: computeMetrics(items, 'unit'),
      }
    })
  }, [compareData, selectedFunds])

  return (
    <div className="min-h-screen bg-gray-50">
      {/* Header */}
      <header className="bg-white border-b border-gray-200 sticky top-0 z-10">
        <div className="max-w-6xl mx-auto px-4 py-4 flex items-center gap-4">
          <h1 className="text-xl font-bold text-gray-900 flex items-center gap-2">
            <span className="inline-flex items-center justify-center w-8 h-8 bg-purple-600 text-white text-sm font-bold rounded">比</span>
            A股基金对比
          </h1>
          <a href="/" className="text-sm text-gray-500 hover:text-gray-700">← 返回全部基金</a>
        </div>
      </header>

      <main className="max-w-6xl mx-auto px-4 py-6 space-y-6">

        {/* Fund selection */}
        <div className="bg-white rounded-xl shadow p-4">
          <h2 className="text-sm font-semibold text-gray-700 mb-3">
            选择基金
            <span className="ml-2 text-xs font-normal text-gray-400">最多 10 只</span>
          </h2>

          {/* Search input */}
          <div className="relative mb-3" ref={searchRef}>
            <input
              type="text"
              value={search}
              onChange={e => { setSearch(e.target.value); setShowDropdown(true) }}
              onFocus={() => search && setShowDropdown(true)}
              placeholder="搜索基金名称或代码…"
              disabled={selectedFunds.length >= 10}
              className="w-full max-w-sm px-4 py-2 border border-gray-300 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-purple-500 disabled:bg-gray-50 disabled:text-gray-400"
            />
            {selectedFunds.length >= 10 && (
              <span className="ml-3 text-xs text-orange-500">已达上限（10只）</span>
            )}

            {/* Dropdown */}
            {showDropdown && candidates.length > 0 && (
              <div
                ref={dropdownRef}
                className="absolute top-full left-0 mt-1 w-full max-w-sm bg-white border border-gray-200 rounded-lg shadow-lg z-20 max-h-60 overflow-auto"
              >
                {candidates.map(fund => {
                  const isSelected = selectedIds.has(fund.fund_id)
                  return (
                    <button
                      key={fund.fund_id}
                      onClick={() => !isSelected && addFund(fund)}
                      disabled={isSelected}
                      className={`w-full text-left px-4 py-2.5 text-sm border-b border-gray-50 last:border-0 transition-colors ${
                        isSelected
                          ? 'text-gray-300 bg-gray-50 cursor-default'
                          : 'hover:bg-purple-50 text-gray-700'
                      }`}
                    >
                      <span className="font-medium">{fund.product_name || '—'}</span>
                      <code className="ml-2 text-xs text-gray-400 font-mono">{fund.product_code}</code>
                      {isSelected && <span className="ml-2 text-xs text-purple-400">已选</span>}
                    </button>
                  )
                })}
              </div>
            )}
          </div>

          {/* Selected fund chips */}
          {selectedFunds.length > 0 && (
            <div className="flex flex-wrap gap-2">
              {selectedFunds.map((fund, idx) => (
                <span
                  key={fund.fund_id}
                  className="inline-flex items-center gap-1.5 px-3 py-1 rounded-full text-xs font-medium text-white"
                  style={{ backgroundColor: COLORS[idx % COLORS.length] }}
                >
                  {fund.product_name || fund.product_code}
                  <button
                    onClick={() => removeFund(fund.fund_id)}
                    className="opacity-80 hover:opacity-100 leading-none"
                    aria-label="移除"
                  >
                    ×
                  </button>
                </span>
              ))}
            </div>
          )}
        </div>

        {/* Controls */}
        <div className="bg-white rounded-xl shadow p-4">
          <div className="flex flex-wrap items-center gap-3">
            {/* Time range presets */}
            <div className="flex gap-1 flex-wrap">
              {RANGE_OPTIONS.map(opt => (
                <button
                  key={opt.days}
                  onClick={() => { setActiveDays(opt.days); setIsCustomRange(false); setCustomFrom(''); setCustomTo('') }}
                  className={`px-3 py-1 rounded-full text-xs font-medium transition-colors ${
                    !isCustomRange && activeDays === opt.days
                      ? 'bg-purple-600 text-white'
                      : 'bg-gray-100 text-gray-600 hover:bg-gray-200'
                  }`}
                >
                  {opt.label}
                </button>
              ))}
              <button
                onClick={() => setIsCustomRange(true)}
                className={`px-3 py-1 rounded-full text-xs font-medium transition-colors ${
                  isCustomRange ? 'bg-purple-600 text-white' : 'bg-gray-100 text-gray-600 hover:bg-gray-200'
                }`}
              >
                自定义
              </button>
            </div>

            {/* Normalize toggle */}
            <div className="ml-auto flex items-center gap-2">
              <button
                onClick={() => setNormalize(v => !v)}
                className={`px-3 py-1 rounded-full text-xs font-medium transition-colors ${
                  normalize ? 'bg-purple-600 text-white' : 'bg-gray-100 text-gray-600 hover:bg-gray-200'
                }`}
              >
                {normalize ? '归一到100' : '绝对净值'}
              </button>
            </div>
          </div>

          {/* Custom date inputs */}
          {isCustomRange && (
            <div className="flex items-center gap-3 mt-3 text-xs text-gray-600">
              <span>起始日期:</span>
              <input
                type="date"
                value={customFrom}
                onChange={e => setCustomFrom(e.target.value)}
                className="border border-gray-300 rounded px-2 py-1 text-xs focus:outline-none focus:ring-1 focus:ring-purple-500"
              />
              <span>截止日期:</span>
              <input
                type="date"
                value={customTo}
                onChange={e => setCustomTo(e.target.value)}
                className="border border-gray-300 rounded px-2 py-1 text-xs focus:outline-none focus:ring-1 focus:ring-purple-500"
              />
            </div>
          )}
        </div>

        {/* Chart */}
        {selectedFunds.length > 0 && (
          <div className="bg-white rounded-xl shadow p-4">
            <h2 className="text-sm font-semibold text-gray-700 mb-3">
              净值走势对比
              {normalize && <span className="ml-2 text-xs font-normal text-gray-400">（各自归一到100）</span>}
            </h2>
            {loading ? (
              <div className="shimmer rounded-lg h-80" />
            ) : chartData && chartData.labels.length > 0 ? (
              <div className="h-80">
                <Line data={chartData} options={chartOptions} />
              </div>
            ) : (
              <div className="h-80 flex items-center justify-center text-gray-400 text-sm">
                暂无数据
              </div>
            )}
          </div>
        )}

        {/* Metrics comparison table */}
        {metricsTable.length > 0 && !loading && (
          <div className="bg-white rounded-xl shadow overflow-hidden">
            <div className="px-4 py-3 border-b border-gray-100">
              <h2 className="text-sm font-semibold text-gray-700">绩效指标对比</h2>
            </div>
            <div className="overflow-x-auto">
              <table className="w-full text-xs">
                <thead>
                  <tr className="bg-gray-50 border-b border-gray-200">
                    <th className="px-4 py-3 text-left text-gray-500 font-medium w-24">指标</th>
                    {metricsTable.map(({ fund, color }) => (
                      <th key={fund.fund_id} className="px-4 py-3 text-right font-medium" style={{ color }}>
                        <span className="block max-w-[120px] truncate" title={fund.product_name}>
                          {fund.product_name || fund.product_code}
                        </span>
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-100">
                  {TABLE_METRICS.map(m => (
                    <tr key={m.key} className="hover:bg-gray-50">
                      <td className="px-4 py-3 text-gray-500">{m.label}</td>
                      {metricsTable.map(({ fund, metrics }) => {
                        const val = metrics?.[m.key] ?? null
                        return (
                          <td
                            key={fund.fund_id}
                            className={`px-4 py-3 text-right font-mono font-medium ${metricCellClass(val, m.format, m.key)}`}
                          >
                            {formatMetric(val, m.format)}
                          </td>
                        )
                      })}
                    </tr>
                  ))}
                  <tr className="hover:bg-gray-50">
                    <td className="px-4 py-3 text-gray-500">区间天数</td>
                    {metricsTable.map(({ fund, metrics }) => (
                      <td key={fund.fund_id} className="px-4 py-3 text-right text-gray-500">
                        {metrics ? `${metrics.days}天` : '—'}
                      </td>
                    ))}
                  </tr>
                </tbody>
              </table>
            </div>
            <p className="px-4 py-2 text-xs text-gray-400 border-t border-gray-50">
              无风险利率 2.5% · 年化基于 365 天
            </p>
          </div>
        )}

        {/* Empty state */}
        {selectedFunds.length === 0 && (
          <div className="bg-white rounded-xl shadow p-12 text-center">
            <p className="text-gray-400 text-sm mb-1">请在上方搜索并添加基金</p>
            <p className="text-gray-300 text-xs">最多同时对比 10 只基金</p>
          </div>
        )}
      </main>
    </div>
  )
}
