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
import { subtractDays, createNav, deleteNav } from '../../api.js'

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
  loading,
  onRetry,
  // range controls
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

  // Manual NAV entry state
  const [showNavForm, setShowNavForm] = useState(false)
  const [navForm, setNavForm] = useState({ nav_date: '', unit_nav: '', accumulated_nav: '' })
  const [submitting, setSubmitting] = useState(false)
  const [navFormError, setNavFormError] = useState('')

  const isBenchmarkMode = !!(normalizedData && benchmarkCode)

  // ── Chart data ──
  const chartData = useMemo(() => {
    const labels = filteredItems.map(i => i.nav_date)
    const values = filteredItems.map(i =>
      navType === 'unit' ? i.unit_nav : (i.accumulated_nav ?? i.unit_nav)
    )
    return { labels, values }
  }, [filteredItems, navType])

  const data = useMemo(() => {
    if (normalizedData && benchmarkCode) {
      const bLabel = BENCHMARK_OPTIONS.find(o => o.code === benchmarkCode)?.label || benchmarkCode
      return {
        labels: normalizedData.labels,
        datasets: [
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
        ],
      }
    }

    const pointColors = filteredItems.map(i =>
      i.source_id === null ? 'rgba(234,88,12,0.8)' : 'transparent'
    )
    const pointRadii = filteredItems.map(i => i.source_id === null ? 4 : 0)
    return {
      labels: chartData.labels,
      datasets: [
        {
          label: navType === 'unit' ? '单位净值' : '累计净值',
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
  }, [chartData, gradient, navType, filteredItems, normalizedData, benchmarkCode, fund])

  // ── Drawdown overlay ──
  const drawdownSeries = useMemo(() => {
    if (filteredItems.length < 2) return null
    const getVal = item => navType === 'unit' ? item.unit_nav : (item.accumulated_nav ?? item.unit_nav)
    let peak = getVal(filteredItems[0])
    const dd = filteredItems.map(item => {
      const v = getVal(item)
      if (v > peak) peak = v
      return peak > 0 ? ((v - peak) / peak) * 100 : 0
    })
    // Only show if there's actual drawdown
    if (Math.min(...dd) >= -0.01) return null
    return dd
  }, [filteredItems, navType])

  // Drawdown chart data
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
              if (isBenchmarkMode) return `${item.dataset.label}: ${Number(item.raw).toFixed(2)}`
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
            callback: v => isBenchmarkMode ? Number(v).toFixed(2) : Number(v).toFixed(4),
            font: { size: 11 },
            color: '#9ca3af',
          },
        },
      },
      onResize: (chart) => {
        if (chart.chartArea) setGradient(createGradient(chart.ctx, chart.chartArea))
      },
    }
  }, [fundIssues, isBenchmarkMode])

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

  return (
    <div className="space-y-4">
      {/* Chart panel */}
      <div className="bg-white rounded-xl shadow p-4">
        {/* Benchmark selector row */}
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
            <span className="text-xs text-gray-400 ml-1">（净值基数=100，已归一化）</span>
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
            <button
              onClick={() => setIsCustomRange(true)}
              className={`px-3 py-1 rounded-full text-xs font-medium transition-colors ${
                isCustomRange
                  ? 'bg-blue-600 text-white'
                  : 'bg-gray-100 text-gray-600 hover:bg-gray-200'
              }`}
            >
              自定义
            </button>
          </div>

          <div className="ml-auto flex items-center gap-2">
            {hasAccumulated && (
              <div className="flex gap-1">
                <button
                  onClick={() => setNavType('unit')}
                  className={`px-3 py-1 rounded-full text-xs font-medium transition-colors ${
                    navType === 'unit' ? 'bg-blue-600 text-white' : 'bg-gray-100 text-gray-600 hover:bg-gray-200'
                  }`}
                >
                  单位净值
                </button>
                <button
                  onClick={() => setNavType('accumulated')}
                  className={`px-3 py-1 rounded-full text-xs font-medium transition-colors ${
                    navType === 'accumulated' ? 'bg-blue-600 text-white' : 'bg-gray-100 text-gray-600 hover:bg-gray-200'
                  }`}
                >
                  累计净值
                </button>
              </div>
            )}
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
      </div>

      {/* Drawdown chart */}
      {!loading && drawdownChartData && (
        <div className="bg-white rounded-xl shadow p-4">
          <h3 className="text-sm font-semibold text-gray-700 mb-3">动态回撤</h3>
          <div className="h-40">
            <Line data={drawdownChartData} options={drawdownOptions} />
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
