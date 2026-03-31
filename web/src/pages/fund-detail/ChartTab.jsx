import { useState, useRef, useMemo, useCallback } from 'react'
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend,
  Filler,
} from 'chart.js'
import Annotation from 'chartjs-plugin-annotation'
import { Line } from 'react-chartjs-2'
import { createNav, deleteNav } from '../../api.js'
import RangeScrubber from '../../components/RangeScrubber.jsx'
import {
  computeMetrics,
  computeBenchmarkMetrics,
  computeTopDrawdowns,
  computeExcessMetrics,
} from '../../utils/metrics.js'

ChartJS.register(
  CategoryScale, LinearScale, PointElement, LineElement,
  Title, Tooltip, Legend, Filler, Annotation,
)

const RANGE_OPTIONS = [
  { label: '近1周', days: 7 },
  { label: '近1月', days: 30 },
  { label: '近3月', days: 90 },
  { label: '近6月', days: 180 },
  { label: '近1年', days: 365 },
  { label: '全部', days: 0 },
]

/**
 * 从 navItems 中提取出现的年份列表，以及"今年以来"的起始日期（前一年12月31日）。
 * 返回: [{ label, from, to }, ...]
 */
function buildYearOptions(navItems) {
  if (!navItems || navItems.length === 0) return []
  const dates = navItems.map(i => i.nav_date).filter(Boolean)
  const firstDate = dates[0]
  const lastDate = dates[dates.length - 1]
  const firstYear = parseInt(firstDate.slice(0, 4))
  const lastYear = parseInt(lastDate.slice(0, 4))
  const today = lastDate

  const opts = []

  // 今年以来
  const ytdFrom = `${lastYear - 1}-12-31`
  if (ytdFrom >= firstDate || firstYear === lastYear) {
    opts.push({ label: '今年以来', from: ytdFrom < firstDate ? firstDate : ytdFrom, to: today, isYtd: true })
  }

  // 每个自然年（从前一年12-31到本年12-31）
  for (let y = lastYear; y >= firstYear; y--) {
    const from = `${y - 1}-12-31`
    const to = y === lastYear ? today : `${y}-12-31`
    // 只在区间内有数据时才显示
    const hasData = dates.some(d => d >= from && d <= to)
    if (hasData) {
      opts.push({ label: String(y), from, to })
    }
  }

  return opts
}

export const BENCHMARK_OPTIONS = [
  { label: '无', code: null },
  { label: '中证1000', code: '000852.SH' },
  { label: '中证500', code: '000905.SH' },
  { label: '沪深300', code: '000300.SH' },
  { label: '上证50', code: '000016.SH' },
  { label: '上证指数', code: '000001.SH' },
  { label: '深证成指', code: '399001.SZ' },
  { label: '创业板指', code: '399006.SZ' },
  { label: '科创50', code: '000688.SH' },
]

const METRIC_DEFS = [
  { key: 'periodReturn',      label: '区间收益率',   format: 'pct',   group: 'return' },
  { key: 'annualizedReturn',  label: '年化收益率',   format: 'pct',   group: 'return' },
  { key: 'annualizedVol',     label: '年化波动率',   format: 'pct',   group: 'risk' },
  { key: 'downsideRisk',      label: '下行风险',     format: 'pct',   group: 'risk' },
  { key: 'maxDrawdown',       label: '最大回撤',     format: 'pct',   group: 'risk' },
  { key: 'maxDDRecoveryDays', label: '回撤回补期',   format: 'days',  group: 'risk' },
  { key: 'sharpe',            label: '夏普比率',     format: 'ratio', group: 'ratio' },
  { key: 'sortino',           label: '索提诺比率',   format: 'ratio', group: 'ratio' },
  { key: 'calmar',            label: '卡玛比率',     format: 'ratio', group: 'ratio' },
  { key: 'monthlyWinRate',    label: '月胜率',       format: 'pct',   group: 'other' },
  { key: 'longestNoNewHigh',  label: '最长不创新高', format: 'days',  group: 'other' },
  { key: 'skewness',          label: '偏度',         format: 'ratio', group: 'risk' },
  { key: 'kurtosis',          label: '峰度',         format: 'ratio', group: 'risk' },
  { key: 'var95',             label: 'VaR (95%)',    format: 'pct',   group: 'risk' },
]

const BENCH_METRIC_DEFS = [
  { key: 'correlation',      label: '相关系数', format: 'ratio' },
  { key: 'beta',             label: 'Beta',    format: 'ratio' },
  { key: 'alpha',            label: 'Alpha',   format: 'pct' },
  { key: 'trackingError',    label: '跟踪误差', format: 'pct' },
  { key: 'informationRatio', label: '信息比率', format: 'ratio' },
]

function formatMetric(val, format) {
  if (val == null) return '—'
  if (format === 'pct') return `${val.toFixed(2)}%`
  if (format === 'ratio') return val.toFixed(3)
  if (format === 'days') return `${Math.round(val)}天`
  return String(val)
}

function metricColor(val, format) {
  if (val == null) return 'text-gray-400'
  if (format === 'days') return 'text-gray-800'
  if (val > 0) return 'text-red-500'
  if (val < 0) return 'text-emerald-600'
  return 'text-gray-700'
}

function createGradient(ctx, chartArea) {
  const gradient = ctx.createLinearGradient(0, chartArea.top, 0, chartArea.bottom)
  gradient.addColorStop(0, 'rgba(59,130,246,0.3)')
  gradient.addColorStop(1, 'rgba(59,130,246,0.01)')
  return gradient
}

export default function ChartTab({
  fund,
  navItems,
  filteredItems,
  fundIssues,
  benchmarkCode,
  setBenchmarkCode,
  benchmarkItems,
  normalizedData,
  navType,
  setNavType,
  hasAccumulated,
  hasAdjusted,
  loading,
  onRetry,
  activeDays,
  setActiveDays,
  isCustomRange,
  setIsCustomRange,
  customFrom,
  setCustomFrom,
  customTo,
  setCustomTo,
}) {
  const chartRef = useRef(null)
  const [gradient, setGradient] = useState(null)
  const [excessMode, setExcessMode] = useState('off') // 'off' | 'arithmetic' | 'geometric'

  const [showNavForm, setShowNavForm] = useState(false)
  const [navForm, setNavForm] = useState({ nav_date: '', unit_nav: '', accumulated_nav: '' })
  const [submitting, setSubmitting] = useState(false)
  const [navFormError, setNavFormError] = useState('')

  const yearOptions = useMemo(() => buildYearOptions(navItems), [navItems])

  const isBenchmarkMode = !!(normalizedData && benchmarkCode)

  // ── Scrubber ──
  const allNavDates = useMemo(() => navItems.map(i => i.nav_date), [navItems])
  const scrubberStart = useMemo(() => {
    if (!filteredItems.length || !navItems.length) return 0
    const first = filteredItems[0].nav_date
    const idx = navItems.findIndex(i => i.nav_date >= first)
    return Math.max(0, idx)
  }, [filteredItems, navItems])
  const scrubberEnd = useMemo(() => {
    if (!filteredItems.length || !navItems.length) return navItems.length - 1
    const last = filteredItems[filteredItems.length - 1].nav_date
    let idx = navItems.length - 1
    for (let i = navItems.length - 1; i >= 0; i--) {
      if (navItems[i].nav_date <= last) { idx = i; break }
    }
    return idx
  }, [filteredItems, navItems])

  const handleScrubberChange = useCallback((startIdx, endIdx) => {
    if (!navItems.length) return
    setIsCustomRange(true)
    setActiveDays(0)
    setCustomFrom(navItems[startIdx]?.nav_date || '')
    setCustomTo(navItems[endIdx]?.nav_date || '')
  }, [navItems, setIsCustomRange, setActiveDays, setCustomFrom, setCustomTo])

  // ── Metrics computations ──
  const metricsNavType = navType === 'return' ? 'unit' : navType

  const fundMetrics = useMemo(
    () => computeMetrics(filteredItems, metricsNavType),
    [filteredItems, metricsNavType],
  )

  const benchAlignedItems = useMemo(() => {
    if (!normalizedData || !benchmarkCode) return null
    return normalizedData.benchNorm
      .map((val, i) => ({ nav_date: normalizedData.labels[i], unit_nav: val }))
      .filter(i => i.unit_nav != null)
  }, [normalizedData, benchmarkCode])

  const benchMetrics = useMemo(() => {
    if (!benchAlignedItems) return null
    return computeMetrics(benchAlignedItems, 'unit')
  }, [benchAlignedItems])

  const benchRelativeMetrics = useMemo(() => {
    if (!benchAlignedItems || !benchmarkCode) return null
    return computeBenchmarkMetrics(filteredItems, benchAlignedItems, metricsNavType)
  }, [filteredItems, benchAlignedItems, metricsNavType, benchmarkCode])

  const excessMetrics = useMemo(() => {
    if (!benchAlignedItems || !benchmarkCode || excessMode === 'off') return null
    return computeExcessMetrics(filteredItems, benchAlignedItems, metricsNavType, excessMode)
  }, [filteredItems, benchAlignedItems, metricsNavType, benchmarkCode, excessMode])

  const topDrawdowns = useMemo(
    () => computeTopDrawdowns(filteredItems, metricsNavType, 5),
    [filteredItems, metricsNavType],
  )

  // ── Excess curve for chart ──
  const excessSeriesForChart = useMemo(() => {
    if (!normalizedData || !benchmarkCode || excessMode === 'off') return null
    const { fundNorm, benchNorm } = normalizedData
    if (!fundNorm || !benchNorm || fundNorm.length < 2) return null
    const base = fundNorm[0] ?? 100
    const cum = [base]
    for (let i = 1; i < fundNorm.length; i++) {
      const pF = fundNorm[i - 1]
      const cF = fundNorm[i]
      const pB = benchNorm[i - 1]
      const cB = benchNorm[i]
      if (pF == null || pB == null || pF <= 0 || pB <= 0 || cF == null || cB == null) {
        cum.push(cum[cum.length - 1])
        continue
      }
      const fR = (cF - pF) / pF
      const bR = (cB - pB) / pB
      const e = excessMode === 'geometric' ? (1 + fR) / (1 + bR) - 1 : fR - bR
      cum.push(cum[cum.length - 1] * (1 + e))
    }
    return cum
  }, [normalizedData, benchmarkCode, excessMode])

  // ── Chart data ──
  const chartData = useMemo(() => {
    const labels = filteredItems.map(i => i.nav_date)
    let values
    if (navType === 'return') {
      const base = filteredItems.length > 0 ? (filteredItems[0].unit_nav || 1) : 1
      values = filteredItems.map(i => i.unit_nav / base)
    } else if (navType === 'adjusted') {
      values = filteredItems.map(i => i.adjusted_nav ?? i.unit_nav)
    } else {
      values = filteredItems.map(i =>
        navType === 'unit' ? i.unit_nav : (i.accumulated_nav ?? i.unit_nav)
      )
    }
    return { labels, values }
  }, [filteredItems, navType])

  const data = useMemo(() => {
    if (normalizedData && benchmarkCode) {
      const bLabel = BENCHMARK_OPTIONS.find(o => o.code === benchmarkCode)?.label || benchmarkCode
      const datasets = [
        {
          label: fund?.product_name || '基金',
          data: normalizedData.fundNorm,
          borderColor: '#3b82f6',
          backgroundColor: 'transparent',
          fill: false,
          tension: 0.3,
          pointRadius: 0,
          pointHoverRadius: 4,
          borderWidth: 2,
        },
        {
          label: bLabel,
          data: normalizedData.benchNorm,
          borderColor: '#9ca3af',
          backgroundColor: 'transparent',
          fill: false,
          tension: 0.3,
          pointRadius: 0,
          pointHoverRadius: 4,
          borderWidth: 1.5,
          borderDash: [5, 5],
          spanGaps: true,
        },
      ]
      if (excessSeriesForChart) {
        datasets.push({
          label: excessMode === 'arithmetic' ? '算术超额' : '几何超额',
          data: excessSeriesForChart,
          borderColor: '#f97316',
          backgroundColor: 'transparent',
          fill: false,
          tension: 0.3,
          pointRadius: 0,
          pointHoverRadius: 4,
          borderWidth: 1.5,
          borderDash: [3, 3],
          spanGaps: true,
        })
      }
      return { labels: normalizedData.labels, datasets }
    }

    const pointColors = filteredItems.map(i =>
      i.source_id === null ? 'rgba(234,88,12,0.8)' : 'transparent'
    )
    const pointRadii = filteredItems.map(i => i.source_id === null ? 4 : 0)
    return {
      labels: chartData.labels,
      datasets: [
        {
          label: navType === 'unit' ? '单位净值' : navType === 'adjusted' ? '复权净值' : '累计净值',
          data: chartData.values,
          borderColor: '#3b82f6',
          backgroundColor: gradient || 'rgba(59,130,246,0.15)',
          fill: true,
          tension: 0.3,
          pointRadius: pointRadii,
          pointBackgroundColor: pointColors,
          pointHoverRadius: 4,
          pointHoverBackgroundColor: '#3b82f6',
          borderWidth: 2,
        },
      ],
    }
  }, [chartData, gradient, navType, filteredItems, normalizedData, benchmarkCode, fund, excessSeriesForChart, excessMode])

  // ── Drawdown chart ──
  const drawdownSeries = useMemo(() => {
    if (filteredItems.length < 2) return null
    const getVal = item => {
      if (navType === 'adjusted') return item.adjusted_nav ?? item.unit_nav
      return navType === 'unit' ? item.unit_nav : (item.accumulated_nav ?? item.unit_nav)
    }
    let peak = getVal(filteredItems[0])
    const dd = filteredItems.map(item => {
      const v = getVal(item)
      if (v > peak) peak = v
      return peak > 0 ? ((v - peak) / peak) * 100 : 0
    })
    if (Math.min(...dd) >= -0.01) return null
    return dd
  }, [filteredItems, navType])

  const drawdownChartData = useMemo(() => {
    if (!drawdownSeries) return null
    return {
      labels: filteredItems.map(i => i.nav_date),
      datasets: [{
        label: '动态回撤',
        data: drawdownSeries,
        borderColor: 'rgba(239,68,68,0.7)',
        backgroundColor: 'rgba(239,68,68,0.1)',
        fill: true,
        tension: 0.3,
        pointRadius: 0,
        pointHoverRadius: 3,
        borderWidth: 1.5,
      }],
    }
  }, [drawdownSeries, filteredItems])

  const drawdownOptions = useMemo(() => ({
    responsive: true,
    maintainAspectRatio: false,
    animation: { duration: 0 },
    interaction: { mode: 'index', intersect: false },
    plugins: {
      legend: { display: false },
      tooltip: {
        callbacks: {
          label: (item) => `回撤: ${Number(item.raw).toFixed(2)}%`,
        },
      },
    },
    scales: {
      x: {
        grid: { display: false },
        ticks: { maxTicksLimit: 8, maxRotation: 0, font: { size: 10 }, color: '#9ca3af' },
      },
      y: {
        position: 'right',
        grid: { color: '#f3f4f6' },
        ticks: {
          callback: v => `${Number(v).toFixed(1)}%`,
          font: { size: 10 },
          color: '#9ca3af',
        },
      },
    },
  }), [])

  // ── Main chart options ──
  const options = useMemo(() => {
    const annotationEntries = {}

    fundIssues.anomalous.forEach((a, i) => {
      annotationEntries[`anomalous_${i}`] = {
        type: 'line',
        scaleID: 'x',
        value: a.nav_date,
        borderColor: 'rgba(239,68,68,0.7)',
        borderWidth: 1,
        borderDash: [4, 4],
        label: {
          content: `异常 ${a.unit_nav.toFixed(2)}`,
          display: true,
          position: 'start',
          backgroundColor: 'rgba(239,68,68,0.8)',
          color: '#fff',
          font: { size: 10 },
          padding: 2,
        },
      }
    })

    fundIssues.gaps.forEach((g, i) => {
      annotationEntries[`gap_${i}`] = {
        type: 'box',
        xScaleID: 'x',
        xMin: g.from_date,
        xMax: g.to_date,
        backgroundColor: 'rgba(156,163,175,0.15)',
        borderWidth: 0,
        label: {
          content: `断层 ${g.gap_days}天`,
          display: true,
          position: { x: 'center', y: 'start' },
          backgroundColor: 'rgba(107,114,128,0.7)',
          color: '#fff',
          font: { size: 10 },
          padding: 2,
        },
      }
    })

    const excessBase = normalizedData?.fundNorm?.[0] ?? 100

    return {
      responsive: true,
      maintainAspectRatio: false,
      animation: { duration: 0 },
      interaction: { mode: 'index', intersect: false },
      plugins: {
        legend: { display: isBenchmarkMode },
        tooltip: {
          callbacks: {
            title: (items) => items[0]?.label || '',
            label: (item) => {
              if (isBenchmarkMode) {
                const v = Number(item.raw)
                if (item.datasetIndex === 2) {
                  const pct = ((v - excessBase) / excessBase * 100).toFixed(2)
                  return `${item.dataset.label}: ${pct >= 0 ? '+' : ''}${pct}%`
                }
                if (navType === 'return') {
                  const sign = v >= 1 ? '+' : ''
                  return `${item.dataset.label}: ${sign}${((v - 1) * 100).toFixed(2)}%`
                }
                return `${item.dataset.label}: ${v.toFixed(2)}`
              }
              if (navType === 'return') {
                const v = Number(item.raw)
                const sign = v >= 1 ? '+' : ''
                return `收益率: ${sign}${((v - 1) * 100).toFixed(2)}%`
              }
              return `净值: ${Number(item.raw).toFixed(4)}`
            },
          },
        },
        annotation: { annotations: annotationEntries },
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
            callback: v => {
              if (navType === 'return') {
                const pct = (Number(v) - 1) * 100
                return (pct >= 0 ? '+' : '') + pct.toFixed(1) + '%'
              }
              return isBenchmarkMode ? Number(v).toFixed(2) : Number(v).toFixed(4)
            },
            font: { size: 11 },
            color: '#9ca3af',
          },
        },
      },
      onResize: (chart) => {
        if (chart.chartArea) setGradient(createGradient(chart.ctx, chart.chartArea))
      },
    }
  }, [fundIssues, isBenchmarkMode, navType, normalizedData])

  const handleChartRef = useCallback((ref) => {
    chartRef.current = ref
    if (ref?.chartArea) setGradient(createGradient(ref.ctx, ref.chartArea))
  }, [])

  const handleNavSubmit = useCallback(async (e) => {
    e.preventDefault()
    if (!fund) return
    setSubmitting(true)
    setNavFormError('')
    try {
      await createNav({
        product_code: fund.product_code,
        nav_date: navForm.nav_date,
        unit_nav: parseFloat(navForm.unit_nav),
        accumulated_nav: navForm.accumulated_nav ? parseFloat(navForm.accumulated_nav) : null,
      })
      setShowNavForm(false)
      setNavForm({ nav_date: '', unit_nav: '', accumulated_nav: '' })
      onRetry()
    } catch (err) {
      setNavFormError(err.message)
    } finally {
      setSubmitting(false)
    }
  }, [fund, navForm, onRetry])

  const handleDeleteNav = useCallback(async (navId) => {
    if (!window.confirm('确认删除该手动录入记录？')) return
    try {
      await deleteNav(navId)
      onRetry()
    } catch (err) {
      console.error('delete nav failed', err)
    }
  }, [onRetry])

  const manualItems = navItems.filter(i => i.source_id === null)
  const hasBench = !!(benchMetrics && benchmarkCode)

  return (
    <div className="space-y-4">
      {/* Chart panel */}
      <div className="bg-white rounded-xl shadow p-4">
        {/* Benchmark selector + excess toggle row */}
        <div className="flex flex-wrap items-center gap-2 mb-3">
          <label className="text-xs text-gray-500 shrink-0">基准指数:</label>
          <select
            value={benchmarkCode || ''}
            onChange={e => setBenchmarkCode(e.target.value || null)}
            className="text-xs border border-gray-200 rounded px-2 py-1 text-gray-700 focus:outline-none focus:ring-1 focus:ring-blue-500"
          >
            {BENCHMARK_OPTIONS.map(opt => (
              <option key={opt.label} value={opt.code || ''}>
                {opt.label}
              </option>
            ))}
          </select>
          {isBenchmarkMode && (
            <>
              <span className="text-xs text-gray-400">
                {navType === 'return' ? '（收益率，以1为基准）' : '（净值基数=100，已归一化）'}
              </span>
              <div className="flex items-center gap-1 ml-1">
                <span className="text-xs text-gray-500">超额曲线:</span>
                {[
                  { v: 'off',        label: '关' },
                  { v: 'arithmetic', label: '算术' },
                  { v: 'geometric',  label: '几何' },
                ].map(opt => (
                  <button
                    key={opt.v}
                    onClick={() => setExcessMode(opt.v)}
                    className={`px-2 py-0.5 rounded-full text-xs font-medium transition-colors ${
                      excessMode === opt.v
                        ? 'bg-orange-500 text-white'
                        : 'bg-gray-100 text-gray-600 hover:bg-gray-200'
                    }`}
                  >
                    {opt.label}
                  </button>
                ))}
              </div>
            </>
          )}
        </div>

        {/* Controls row */}
        <div className="flex flex-wrap items-center gap-3 mb-4">
          <div className="flex gap-1 flex-wrap">
            {RANGE_OPTIONS.map(opt => (
              <button
                key={opt.days}
                onClick={() => { setActiveDays(opt.days); setIsCustomRange(false); setCustomFrom(''); setCustomTo('') }}
                className={`px-3 py-1 rounded-full text-xs font-medium transition-colors ${
                  !isCustomRange && activeDays === opt.days
                    ? 'bg-blue-600 text-white'
                    : 'bg-gray-100 text-gray-600 hover:bg-gray-200'
                }`}
              >
                {opt.label}
              </button>
            ))}
            {yearOptions.map(opt => (
              <button
                key={opt.label}
                onClick={() => {
                  setIsCustomRange(true)
                  setActiveDays(0)
                  setCustomFrom(opt.from)
                  setCustomTo(opt.to)
                }}
                className={`px-3 py-1 rounded-full text-xs font-medium transition-colors ${
                  isCustomRange && customFrom === opt.from && customTo === opt.to
                    ? 'bg-blue-600 text-white'
                    : opt.isYtd
                      ? 'bg-blue-50 text-blue-600 hover:bg-blue-100'
                      : 'bg-gray-100 text-gray-600 hover:bg-gray-200'
                }`}
              >
                {opt.label}
              </button>
            ))}
            <button
              onClick={() => setIsCustomRange(true)}
              className={`px-3 py-1 rounded-full text-xs font-medium transition-colors ${
                isCustomRange && !yearOptions.some(o => o.from === customFrom && o.to === customTo)
                  ? 'bg-blue-600 text-white'
                  : 'bg-gray-100 text-gray-600 hover:bg-gray-200'
              }`}
            >
              自定义
            </button>
          </div>

          <div className="ml-auto flex items-center gap-2">
            <div className="flex gap-1">
              <button
                onClick={() => setNavType('unit')}
                className={`px-3 py-1 rounded-full text-xs font-medium transition-colors ${
                  navType === 'unit' ? 'bg-blue-600 text-white' : 'bg-gray-100 text-gray-600 hover:bg-gray-200'
                }`}
              >
                单位净值
              </button>
              {hasAccumulated && (
                <button
                  onClick={() => setNavType('accumulated')}
                  className={`px-3 py-1 rounded-full text-xs font-medium transition-colors ${
                    navType === 'accumulated' ? 'bg-blue-600 text-white' : 'bg-gray-100 text-gray-600 hover:bg-gray-200'
                  }`}
                >
                  累计净值
                </button>
              )}
              {hasAdjusted && (
                <button
                  onClick={() => setNavType('adjusted')}
                  className={`px-3 py-1 rounded-full text-xs font-medium transition-colors ${
                    navType === 'adjusted' ? 'bg-blue-600 text-white' : 'bg-gray-100 text-gray-600 hover:bg-gray-200'
                  }`}
                >
                  复权净值
                </button>
              )}
              <button
                onClick={() => setNavType('return')}
                className={`px-3 py-1 rounded-full text-xs font-medium transition-colors ${
                  navType === 'return' ? 'bg-blue-600 text-white' : 'bg-gray-100 text-gray-600 hover:bg-gray-200'
                }`}
              >
                收益率
              </button>
            </div>
            {fund && (
              <button
                onClick={() => setShowNavForm(true)}
                className="px-3 py-1 rounded-full text-xs font-medium bg-orange-50 text-orange-700 border border-orange-200 hover:bg-orange-100 transition-colors"
              >
                + 手动录入
              </button>
            )}
          </div>
        </div>

        {/* Custom date range inputs */}
        {isCustomRange && (
          <div className="flex items-center gap-3 mb-4 text-xs text-gray-600">
            <span>起始日期:</span>
            <input
              type="date"
              value={customFrom}
              onChange={e => setCustomFrom(e.target.value)}
              className="border border-gray-300 rounded px-2 py-1 text-xs focus:outline-none focus:ring-1 focus:ring-blue-500"
            />
            <span>截止日期:</span>
            <input
              type="date"
              value={customTo}
              onChange={e => setCustomTo(e.target.value)}
              className="border border-gray-300 rounded px-2 py-1 text-xs focus:outline-none focus:ring-1 focus:ring-blue-500"
            />
          </div>
        )}

        {/* Issue summary */}
        {!loading && (fundIssues.anomalous.length > 0 || fundIssues.gaps.length > 0) && (
          <div className="mb-3 flex flex-wrap gap-2 text-xs">
            {fundIssues.anomalous.length > 0 && (
              <span className="inline-flex items-center gap-1 px-2 py-1 rounded-full bg-red-50 text-red-600 border border-red-200">
                <span className="w-2 h-2 rounded-full bg-red-400 inline-block" />
                {fundIssues.anomalous.length} 个异常净值（红色虚线标注）
              </span>
            )}
            {fundIssues.gaps.length > 0 && (
              <span className="inline-flex items-center gap-1 px-2 py-1 rounded-full bg-gray-50 text-gray-600 border border-gray-200">
                <span className="w-2 h-2 rounded-full bg-gray-400 inline-block" />
                {fundIssues.gaps.length} 处日期断层（灰色区间标注）
              </span>
            )}
          </div>
        )}

        {/* Main chart */}
        {loading ? (
          <div className="shimmer rounded-lg h-72" />
        ) : filteredItems.length === 0 ? (
          <div className="h-72 flex items-center justify-center text-gray-400 text-sm">暂无数据</div>
        ) : (
          <div className="h-72">
            <Line ref={handleChartRef} data={data} options={options} />
          </div>
        )}

        {!loading && filteredItems.length > 0 && (
          <p className="text-xs text-gray-400 mt-2 text-right">
            显示 {filteredItems.length} 条记录
            {isBenchmarkMode && normalizedData && (
              <span className="ml-2">· 基准: {BENCHMARK_OPTIONS.find(o => o.code === benchmarkCode)?.label}</span>
            )}
          </p>
        )}

        {!loading && allNavDates.length > 1 && (
          <RangeScrubber
            dates={allNavDates}
            startIdx={scrubberStart}
            endIdx={scrubberEnd}
            onChange={handleScrubberChange}
          />
        )}
      </div>

      {/* Drawdown chart */}
      {!loading && drawdownChartData && (
        <div className="bg-white rounded-xl shadow p-4">
          <h3 className="text-sm font-semibold text-gray-700 mb-3">动态回撤</h3>
          <div className="h-36 md:h-40">
            <Line data={drawdownChartData} options={drawdownOptions} />
          </div>
        </div>
      )}

      {/* Metrics comparison table */}
      {!loading && fundMetrics && (
        <div className="bg-white rounded-xl shadow p-4">
          <h3 className="text-sm font-semibold text-gray-700 mb-4">业绩指标</h3>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-gray-200 text-gray-500 text-xs">
                  <th className="text-left py-2 pr-4 font-medium">指标</th>
                  <th className="text-right py-2 px-4 font-medium">基金</th>
                  {hasBench && <th className="text-right py-2 px-4 font-medium">基准</th>}
                  {hasBench && <th className="text-right py-2 pl-4 font-medium">超额</th>}
                </tr>
              </thead>
              <tbody>
                {METRIC_DEFS.map(m => {
                  const fundVal = fundMetrics[m.key]
                  const benchVal = hasBench ? benchMetrics?.[m.key] : null
                  const excess = (fundVal != null && benchVal != null && m.format === 'pct')
                    ? fundVal - benchVal
                    : null
                  return (
                    <tr key={m.key} className="border-b border-gray-50 hover:bg-gray-50">
                      <td className="py-2 pr-4 text-gray-600 text-xs">{m.label}</td>
                      <td className={`py-2 px-2 md:px-4 text-right font-mono text-xs ${metricColor(fundVal, m.format)}`}>
                        {formatMetric(fundVal, m.format)}
                      </td>
                      {hasBench && (
                        <td className={`py-2 px-2 md:px-4 text-right font-mono text-xs ${metricColor(benchVal, m.format)}`}>
                          {formatMetric(benchVal, m.format)}
                        </td>
                      )}
                      {hasBench && (
                        <td className={`py-2 pl-2 md:pl-4 text-right font-mono text-xs ${metricColor(excess, 'pct')}`}>
                          {excess != null ? formatMetric(excess, 'pct') : '—'}
                        </td>
                      )}
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </div>
          <p className="text-xs text-gray-400 mt-3">
            区间 {fundMetrics.days} 天 · 无风险利率 2.5%
          </p>
        </div>
      )}

      {/* Benchmark-relative metrics */}
      {!loading && benchRelativeMetrics && (
        <div className="bg-white rounded-xl shadow p-4">
          <h3 className="text-sm font-semibold text-gray-700 mb-4">基准对比指标</h3>
          <div className="grid grid-cols-2 md:grid-cols-5 gap-3">
            {BENCH_METRIC_DEFS.map(m => {
              const val = benchRelativeMetrics[m.key]
              return (
                <div key={m.key} className="bg-blue-50 rounded-lg p-3">
                  <p className="text-xs text-gray-500 mb-1">{m.label}</p>
                  <p className={`text-sm font-semibold font-mono ${metricColor(val, m.format)}`}>
                    {formatMetric(val, m.format)}
                  </p>
                </div>
              )
            })}
          </div>
        </div>
      )}

      {/* Excess return metrics */}
      {!loading && benchmarkCode && excessMetrics && excessMode !== 'off' && (
        <div className="bg-white rounded-xl shadow p-4">
          <div className="flex items-center justify-between mb-4">
            <h3 className="text-sm font-semibold text-gray-700">超额分析</h3>
            <span className="text-xs text-gray-400">
              {excessMode === 'arithmetic'
                ? '算术超额：每期收益率之差累乘'
                : '几何超额：(1+基金收益)/(1+基准收益)−1 累乘'}
            </span>
          </div>
          <div className="grid grid-cols-2 md:grid-cols-5 gap-3">
            {[
              { key: 'periodExcess',     label: '超额收益率',   format: 'pct' },
              { key: 'annualizedExcess', label: '年化超额收益', format: 'pct' },
              { key: 'excessVol',        label: '超额波动率',   format: 'pct' },
              { key: 'excessMaxDD',      label: '超额最大回撤', format: 'pct' },
              { key: 'excessSharpe',     label: '超额夏普',     format: 'ratio' },
            ].map(m => {
              const val = excessMetrics[m.key]
              return (
                <div key={m.key} className="bg-purple-50 rounded-lg p-3">
                  <p className="text-xs text-gray-500 mb-1">{m.label}</p>
                  <p className={`text-sm font-semibold font-mono ${metricColor(val, m.format)}`}>
                    {formatMetric(val, m.format)}
                  </p>
                </div>
              )
            })}
          </div>
        </div>
      )}

      {/* Top 5 drawdowns */}
      {!loading && topDrawdowns.length > 0 && (
        <div className="bg-white rounded-xl shadow p-4">
          <h3 className="text-sm font-semibold text-gray-700 mb-3">Top {topDrawdowns.length} 回撤</h3>
          <div className="overflow-x-auto">
            <table className="w-full text-xs">
              <thead>
                <tr className="border-b border-gray-200 text-gray-500">
                  <th className="text-left py-2 font-medium">#</th>
                  <th className="text-left py-2 font-medium">峰值日期</th>
                  <th className="text-left py-2 font-medium">谷底日期</th>
                  <th className="text-left py-2 font-medium">恢复日期</th>
                  <th className="text-right py-2 font-medium">回撤幅度</th>
                  <th className="text-right py-2 font-medium">恢复天数</th>
                </tr>
              </thead>
              <tbody>
                {topDrawdowns.map((dd, i) => (
                  <tr key={i} className="border-b border-gray-50 hover:bg-gray-50">
                    <td className="py-2 text-gray-400">{i + 1}</td>
                    <td className="py-2 text-gray-600">{dd.peakDate}</td>
                    <td className="py-2 text-gray-600">{dd.troughDate}</td>
                    <td className="py-2 text-gray-600">{dd.recoveryDate || '未恢复'}</td>
                    <td className="py-2 text-right font-mono text-red-500">{dd.drawdown.toFixed(2)}%</td>
                    <td className="py-2 text-right font-mono text-gray-700">
                      {dd.recoveryDays != null ? `${dd.recoveryDays}天` : '—'}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* Manual records list */}
      {!loading && manualItems.length > 0 && (
        <div className="bg-white rounded-xl shadow p-4">
          <h3 className="text-sm font-semibold text-gray-700 mb-3">
            手动录入记录
            <span className="ml-2 text-xs font-normal text-orange-600">
              ● 橙色点标注于图表
            </span>
          </h3>
          <table className="w-full text-xs">
            <thead>
              <tr className="text-left text-gray-500 border-b border-gray-100">
                <th className="py-2">净值日期</th>
                <th className="py-2 text-right">单位净值</th>
                <th className="py-2 text-right">累计净值</th>
                <th className="py-2 text-right">操作</th>
              </tr>
            </thead>
            <tbody>
              {manualItems.map(item => (
                <tr key={item.id} className="border-b border-gray-50 hover:bg-gray-50">
                  <td className="py-2 text-gray-600">{item.nav_date}</td>
                  <td className="py-2 text-right font-mono text-gray-800">
                    {item.unit_nav.toFixed(4)}
                  </td>
                  <td className="py-2 text-right font-mono text-gray-500">
                    {item.accumulated_nav != null ? item.accumulated_nav.toFixed(4) : '—'}
                  </td>
                  <td className="py-2 text-right">
                    <button
                      onClick={() => handleDeleteNav(item.id)}
                      className="text-red-400 hover:text-red-600"
                    >
                      删除
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {/* Manual entry modal */}
      {showNavForm && (
        <div
          className="fixed inset-0 bg-black/40 z-50 flex items-center justify-center p-4"
          onClick={() => { setShowNavForm(false); setNavForm({ nav_date: '', unit_nav: '', accumulated_nav: '' }); setNavFormError('') }}
        >
          <div
            className="bg-white rounded-xl shadow-lg w-full max-w-sm p-6"
            onClick={e => e.stopPropagation()}
          >
            <h3 className="text-base font-semibold text-gray-800 mb-4">手动录入净值</h3>
            <form onSubmit={handleNavSubmit} className="space-y-4">
              <div>
                <label className="block text-xs text-gray-600 mb-1">净值日期</label>
                <input
                  type="date"
                  value={navForm.nav_date}
                  onChange={e => setNavForm(f => ({ ...f, nav_date: e.target.value }))}
                  required
                  className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
                />
              </div>
              <div>
                <label className="block text-xs text-gray-600 mb-1">单位净值</label>
                <input
                  type="number"
                  step="0.0001"
                  min="0.0001"
                  value={navForm.unit_nav}
                  onChange={e => setNavForm(f => ({ ...f, unit_nav: e.target.value }))}
                  required
                  className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
                />
              </div>
              <div>
                <label className="block text-xs text-gray-600 mb-1">累计净值（可选）</label>
                <input
                  type="number"
                  step="0.0001"
                  min="0.0001"
                  value={navForm.accumulated_nav}
                  onChange={e => setNavForm(f => ({ ...f, accumulated_nav: e.target.value }))}
                  className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
                />
              </div>
              {navFormError && (
                <p className="text-xs text-red-500">{navFormError}</p>
              )}
              <div className="flex gap-2 justify-end pt-2">
                <button
                  type="button"
                  onClick={() => { setShowNavForm(false); setNavForm({ nav_date: '', unit_nav: '', accumulated_nav: '' }); setNavFormError('') }}
                  className="px-4 py-2 text-sm text-gray-600 rounded-lg hover:bg-gray-100"
                >
                  取消
                </button>
                <button
                  type="submit"
                  disabled={submitting}
                  className="px-4 py-2 text-sm bg-orange-500 text-white rounded-lg hover:bg-orange-600 disabled:opacity-50"
                >
                  {submitting ? '提交中…' : '确认录入'}
                </button>
              </div>
            </form>
          </div>
        </div>
      )}
    </div>
  )
}
