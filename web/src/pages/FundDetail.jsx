import { useState, useEffect, useRef, useMemo, useCallback } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
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
import { fetchFund, fetchFundNav, fetchFundIssues, subtractDays, createNav, deleteNav } from '../api.js'

ChartJS.register(
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend,
  Filler,
  Annotation
)

const RANGE_OPTIONS = [
  { label: '近1周', days: 7 },
  { label: '近1月', days: 30 },
  { label: '近3月', days: 90 },
  { label: '近6月', days: 180 },
  { label: '近1年', days: 365 },
  { label: '全部', days: 0 },
]

function StatCard({ label, value, valueClass }) {
  return (
    <div className="bg-white rounded-xl shadow p-4">
      <p className="text-xs text-gray-500 mb-1">{label}</p>
      <p className={`text-lg font-semibold ${valueClass || 'text-gray-900'}`}>{value}</p>
    </div>
  )
}

function createGradient(ctx, chartArea) {
  const gradient = ctx.createLinearGradient(0, chartArea.top, 0, chartArea.bottom)
  gradient.addColorStop(0, 'rgba(59,130,246,0.3)')
  gradient.addColorStop(1, 'rgba(59,130,246,0.01)')
  return gradient
}

export default function FundDetail() {
  const { id } = useParams()
  const navigate = useNavigate()
  const chartRef = useRef(null)

  const [fund, setFund] = useState(null)
  const [navItems, setNavItems] = useState([])
  const [fundIssues, setFundIssues] = useState({ anomalous: [], gaps: [] })
  const [activeDays, setActiveDays] = useState(0)
  const [navType, setNavType] = useState('unit')
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [retryCount, setRetryCount] = useState(0)
  const [showNavForm, setShowNavForm] = useState(false)
  const [navForm, setNavForm] = useState({ nav_date: '', unit_nav: '', accumulated_nav: '' })
  const [submitting, setSubmitting] = useState(false)
  const [navFormError, setNavFormError] = useState('')

  useEffect(() => {
    if (!id) { navigate('/'); return }
    const numId = parseInt(id, 10)
    if (isNaN(numId)) { navigate('/'); return }

    const controller = new AbortController()
    const { signal } = controller

    setLoading(true)
    setError(null)

    Promise.all([
      fetchFund(numId, signal),
      fetchFundNav(numId, { limit: 5000, apply_filter: false }, signal),
    ])
      .then(([f, items]) => {
        setFund(f)
        setNavItems(items)
        setLoading(false)
      })
      .catch(err => {
        if (err.name === 'AbortError') return
        setError(err.message)
        setLoading(false)
      })

    // Issues loaded independently so a failure here never blocks the main chart
    fetchFundIssues(numId, signal)
      .then(issues => setFundIssues(issues || { anomalous: [], gaps: [] }))
      .catch(err => { if (err.name !== 'AbortError') console.warn('fund issues load failed', err) })

    return () => controller.abort()
  }, [id, navigate, retryCount])

  // Filter by active days
  const filteredItems = useMemo(() => {
    if (activeDays === 0 || navItems.length === 0) return navItems
    const latestDate = navItems[navItems.length - 1].nav_date
    const from = subtractDays(latestDate, activeDays)
    if (!from) return navItems
    return navItems.filter(item => item.nav_date >= from)
  }, [navItems, activeDays])

  // Check if accumulated_nav data exists
  const hasAccumulated = useMemo(
    () => navItems.some(item => item.accumulated_nav != null),
    [navItems]
  )

  // Weekly return (client-side)
  const weeklyPct = useMemo(() => {
    if (navItems.length < 2) return null
    const latestDate = navItems[navItems.length - 1].nav_date
    const from = subtractDays(latestDate, 14)
    if (!from) return null
    const recent = navItems.filter(i => i.nav_date >= from)
    if (recent.length < 2) return null
    const first = recent[0].unit_nav
    const last = recent[recent.length - 1].unit_nav
    return first > 0 ? (last - first) / first * 100 : null
  }, [navItems])

  // Chart data
  const chartData = useMemo(() => {
    const labels = filteredItems.map(i => i.nav_date)
    const values = filteredItems.map(i =>
      navType === 'unit' ? i.unit_nav : (i.accumulated_nav ?? i.unit_nav)
    )
    return { labels, values }
  }, [filteredItems, navType])

  // Chart.js dataset — memoized so the chart only re-draws when data truly changes
  const [gradient, setGradient] = useState(null)

  const data = useMemo(() => {
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
  }, [chartData, gradient, navType, filteredItems])

  const options = useMemo(() => {
    // Build annotation objects for anomalous dates and gap ranges
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
      interaction: {
        mode: 'index',
        intersect: false,
      },
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: {
            title: (items) => items[0]?.label || '',
            label: (item) => `净值: ${Number(item.raw).toFixed(4)}`,
          },
        },
        annotation: {
          annotations: annotationEntries,
        },
      },
      scales: {
        x: {
          grid: { display: false },
          ticks: {
            maxTicksLimit: 8,
            maxRotation: 0,
            font: { size: 11 },
            color: '#9ca3af',
          },
        },
        y: {
          position: 'right',
          grid: { color: '#f3f4f6' },
          ticks: {
            callback: (v) => Number(v).toFixed(4),
            font: { size: 11 },
            color: '#9ca3af',
          },
        },
      },
      onResize: (chart) => {
        if (chart.chartArea) {
          setGradient(createGradient(chart.ctx, chart.chartArea))
        }
      },
    }
  }, [fundIssues])  // setGradient is stable; fundIssues drives annotations

  // useCallback prevents a new function ref every render,
  // which would otherwise trigger an infinite setGradient → re-render loop
  const handleChartRef = useCallback((ref) => {
    chartRef.current = ref
    if (ref?.chartArea) {
      setGradient(createGradient(ref.ctx, ref.chartArea))
    }
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
      setRetryCount(c => c + 1)
    } catch (err) {
      setNavFormError(err.message)
    } finally {
      setSubmitting(false)
    }
  }, [fund, navForm])

  const handleDeleteNav = useCallback(async (navId) => {
    if (!window.confirm('确认删除该手动录入记录？')) return
    try {
      await deleteNav(navId)
      setRetryCount(c => c + 1)
    } catch (err) {
      console.error('delete nav failed', err)
    }
  }, [])

  if (error) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <div className="bg-white rounded-xl shadow p-8 max-w-md text-center">
          <div className="text-4xl mb-4">⚠️</div>
          <h2 className="text-xl font-semibold text-gray-800 mb-2">加载失败</h2>
          <p className="text-red-400 text-xs mt-2">{error}</p>
          <button
            onClick={() => navigate('/')}
            className="mt-4 px-4 py-2 bg-blue-600 text-white rounded-lg text-sm hover:bg-blue-700"
          >
            返回列表
          </button>
          <button
            onClick={() => setRetryCount(c => c + 1)}
            className="mt-4 ml-2 px-4 py-2 bg-gray-100 text-gray-700 rounded-lg text-sm hover:bg-gray-200"
          >
            重试
          </button>
        </div>
      </div>
    )
  }

  const weeklyColor = weeklyPct == null
    ? 'text-gray-400'
    : weeklyPct > 0
    ? 'text-red-500'
    : weeklyPct < 0
    ? 'text-emerald-600'
    : 'text-gray-500'

  const weeklyLabel = weeklyPct == null
    ? '—'
    : `${weeklyPct > 0 ? '+' : ''}${weeklyPct.toFixed(2)}%`

  return (
    <div className="min-h-screen bg-gray-50">
      {/* Header */}
      <header className="bg-white border-b border-gray-200 sticky top-0 z-10">
        <div className="max-w-5xl mx-auto px-4 py-4 flex items-center gap-4">
          <button
            onClick={() => navigate('/')}
            className="text-blue-600 hover:text-blue-800 text-sm flex items-center gap-1"
          >
            ← 返回全部基金
          </button>
          {fund && (
            <div className="flex items-baseline gap-2 ml-2">
              <h1 className="text-lg font-bold text-gray-900">{fund.product_name || '—'}</h1>
              <code className="text-xs text-gray-400 font-mono">{fund.product_code}</code>
            </div>
          )}
        </div>
      </header>

      <main className="max-w-5xl mx-auto px-4 py-6 space-y-6">
        {/* Stat cards */}
        {loading ? (
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
            {Array.from({ length: 4 }, (_, i) => (
              <div key={i} className="bg-white rounded-xl shadow p-4">
                <div className="shimmer rounded h-3 w-16 mb-2" />
                <div className="shimmer rounded h-6 w-24" />
              </div>
            ))}
          </div>
        ) : fund && (
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
            <StatCard
              label="最新净值"
              value={fund.latest_nav != null ? fund.latest_nav.toFixed(4) : '—'}
            />
            <StatCard
              label="近一周收益"
              value={weeklyLabel}
              valueClass={weeklyColor}
            />
            <StatCard
              label="记录数"
              value={fund.record_count.toLocaleString()}
            />
            <StatCard
              label="数据区间"
              value={fund.earliest_date && fund.latest_date
                ? `${fund.earliest_date} ~ ${fund.latest_date}`
                : '—'
              }
            />
          </div>
        )}

        {/* Chart panel */}
        <div className="bg-white rounded-xl shadow p-4">
          {/* Controls */}
          <div className="flex flex-wrap items-center gap-3 mb-4">
            {/* Time range */}
            <div className="flex gap-1 flex-wrap">
              {RANGE_OPTIONS.map(opt => (
                <button
                  key={opt.days}
                  onClick={() => setActiveDays(opt.days)}
                  className={`px-3 py-1 rounded-full text-xs font-medium transition-colors ${
                    activeDays === opt.days
                      ? 'bg-blue-600 text-white'
                      : 'bg-gray-100 text-gray-600 hover:bg-gray-200'
                  }`}
                >
                  {opt.label}
                </button>
              ))}
            </div>

            <div className="ml-auto flex items-center gap-2">
              {/* Nav type toggle */}
              {hasAccumulated && (
                <div className="flex gap-1">
                  <button
                    onClick={() => setNavType('unit')}
                    className={`px-3 py-1 rounded-full text-xs font-medium transition-colors ${
                      navType === 'unit'
                        ? 'bg-blue-600 text-white'
                        : 'bg-gray-100 text-gray-600 hover:bg-gray-200'
                    }`}
                  >
                    单位净值
                  </button>
                  <button
                    onClick={() => setNavType('accumulated')}
                    className={`px-3 py-1 rounded-full text-xs font-medium transition-colors ${
                      navType === 'accumulated'
                        ? 'bg-blue-600 text-white'
                        : 'bg-gray-100 text-gray-600 hover:bg-gray-200'
                    }`}
                  >
                    累计净值
                  </button>
                </div>
              )}
              {/* Manual entry button */}
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

          {/* Chart */}
          {loading ? (
            <div className="shimmer rounded-lg h-72" />
          ) : filteredItems.length === 0 ? (
            <div className="h-72 flex items-center justify-center text-gray-400 text-sm">
              暂无数据
            </div>
          ) : (
            <div className="h-72">
              <Line
                ref={handleChartRef}
                data={data}
                options={options}
              />
            </div>
          )}

          {/* Data count note */}
          {!loading && filteredItems.length > 0 && (
            <p className="text-xs text-gray-400 mt-2 text-right">
              显示 {filteredItems.length} 条记录
            </p>
          )}
        </div>

        {/* Manual records list */}
        {!loading && (() => {
          const manualItems = navItems.filter(i => i.source_id === null)
          if (manualItems.length === 0) return null
          return (
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
          )
        })()}

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
      </main>
    </div>
  )
}
