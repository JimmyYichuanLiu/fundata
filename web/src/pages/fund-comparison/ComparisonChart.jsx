import { useState, useMemo } from 'react'
import {
  Chart as ChartJS, CategoryScale, LinearScale, PointElement,
  LineElement, Title, Tooltip, Legend, Filler,
} from 'chart.js'
import { Line } from 'react-chartjs-2'
import { FUND_COLORS, BENCHMARK_OPTIONS } from '../../utils/metricDefs.js'

ChartJS.register(CategoryScale, LinearScale, PointElement, LineElement, Title, Tooltip, Legend, Filler)

const RANGE_OPTIONS = [
  { label: '近1月', days: 30   },
  { label: '近3月', days: 90   },
  { label: '近6月', days: 180  },
  { label: '近1年', days: 365  },
  { label: '近3年', days: 1095 },
  { label: '全部',  days: 0    },
]

export default function ComparisonChart({
  compareList, navDataMap, benchItems, benchmarkCode, loading, commonStart,
}) {
  // null = use commonStart (default), number = days from latest
  const [activeDays, setActiveDays] = useState(null)

  // Compute effective fromDate
  const fromDate = useMemo(() => {
    if (activeDays === null) return commonStart || ''
    if (activeDays === 0)   return ''
    // pick latest "last date" across all funds, subtract days
    const allLast = Object.values(navDataMap)
      .map(items => items.length > 0 ? items[items.length - 1].nav_date : '')
      .filter(Boolean)
    if (allLast.length === 0) return ''
    const last = allLast.reduce((a, b) => (a > b ? a : b))
    const normalized = /^\d{8}$/.test(last)
      ? `${last.slice(0,4)}-${last.slice(4,6)}-${last.slice(6,8)}`
      : last
    const d = new Date(`${normalized}T00:00:00`)
    if (Number.isNaN(d.getTime())) return ''
    d.setDate(d.getDate() - activeDays)
    return d.toISOString().slice(0, 10)
  }, [activeDays, navDataMap, commonStart])

  const { labels, seriesMap, benchSeries } = useMemo(() => {
    if (compareList.length === 0) return { labels: [], seriesMap: {}, benchSeries: [] }

    // Filter each fund's items by fromDate
    const filteredMap = {}
    compareList.forEach(f => {
      const items = navDataMap[f.fund_id] || []
      filteredMap[f.fund_id] = fromDate ? items.filter(i => i.nav_date >= fromDate) : items
    })

    // Union of all dates
    const dateSet = new Set()
    Object.values(filteredMap).forEach(items => items.forEach(i => dateSet.add(i.nav_date)))
    const sortedLabels = [...dateSet].sort()
    if (sortedLabels.length === 0) return { labels: [], seriesMap: {}, benchSeries: [] }

    // Normalize each fund to start = 1
    const sm = {}
    compareList.forEach(f => {
      const items = filteredMap[f.fund_id]
      if (items.length === 0) { sm[f.fund_id] = sortedLabels.map(() => null); return }
      const navByDate = new Map(items.map(i => [i.nav_date, i.unit_nav]))
      const base = items[0].unit_nav || 1
      let lastVal = null
      sm[f.fund_id] = sortedLabels.map(d => {
        if (navByDate.has(d)) lastVal = navByDate.get(d) / base
        return lastVal
      })
    })

    // Benchmark normalized to 1
    let bs = []
    if (benchItems.length > 0) {
      const from = sortedLabels[0]
      const to   = sortedLabels[sortedLabels.length - 1]
      const filtered = benchItems.filter(i =>
        i.trade_date >= from.replace(/-/g, '') && i.trade_date <= to.replace(/-/g, '')
      )
      if (filtered.length > 0) {
        const benchByDate = new Map(filtered.map(i => [
          `${i.trade_date.slice(0,4)}-${i.trade_date.slice(4,6)}-${i.trade_date.slice(6,8)}`,
          i.close,
        ]))
        const base = filtered[0].close
        let lastB = null
        bs = sortedLabels.map(d => {
          if (benchByDate.has(d)) lastB = benchByDate.get(d) / base
          return lastB
        })
      }
    }

    return { labels: sortedLabels, seriesMap: sm, benchSeries: bs }
  }, [compareList, navDataMap, benchItems, fromDate])

  const drawdownSeries = useMemo(() => {
    const result = {}
    compareList.forEach(f => {
      const vals = (seriesMap[f.fund_id] || []).filter(v => v != null)
      if (vals.length < 2) return
      let peak = vals[0]
      result[f.fund_id] = vals.map(v => { if (v > peak) peak = v; return peak > 0 ? (v - peak) / peak * 100 : 0 })
    })
    return result
  }, [compareList, seriesMap])

  const mainChartData = useMemo(() => {
    const datasets = compareList.map((f, idx) => ({
      label: f.product_name,
      data: seriesMap[f.fund_id] || [],
      borderColor: FUND_COLORS[idx % FUND_COLORS.length],
      backgroundColor: 'transparent',
      fill: false,
      tension: 0.3,
      pointRadius: 0,
      pointHoverRadius: 4,
      borderWidth: 2,
      spanGaps: true,
    }))
    if (benchSeries.length > 0 && benchmarkCode) {
      const bLabel = BENCHMARK_OPTIONS.find(o => o.code === benchmarkCode)?.label || benchmarkCode
      datasets.push({
        label: bLabel,
        data: benchSeries,
        borderColor: '#9ca3af',
        backgroundColor: 'transparent',
        fill: false,
        tension: 0.3,
        pointRadius: 0,
        pointHoverRadius: 3,
        borderWidth: 1.5,
        borderDash: [5, 5],
        spanGaps: true,
      })
    }
    return { labels, datasets }
  }, [labels, seriesMap, benchSeries, compareList, benchmarkCode])

  const ddChartData = useMemo(() => {
    const datasets = compareList
      .filter(f => drawdownSeries[f.fund_id])
      .map((f, idx) => ({
        label: f.product_name,
        data: drawdownSeries[f.fund_id],
        borderColor: FUND_COLORS[idx % FUND_COLORS.length],
        backgroundColor: 'transparent',
        fill: false,
        tension: 0.3,
        pointRadius: 0,
        borderWidth: 1.5,
        spanGaps: true,
      }))
    return datasets.length > 0 ? { labels, datasets } : null
  }, [labels, drawdownSeries, compareList])

  const mainOptions = useMemo(() => ({
    responsive: true,
    maintainAspectRatio: false,
    animation: { duration: 0 },
    interaction: { mode: 'index', intersect: false },
    plugins: {
      legend: { position: 'top', labels: { font: { size: 11 }, boxWidth: 12 } },
      tooltip: {
        callbacks: {
          label: item => {
            const v = Number(item.raw)
            const pct = (v - 1) * 100
            return `${item.dataset.label}: ${pct >= 0 ? '+' : ''}${pct.toFixed(2)}%`
          },
        },
      },
    },
    scales: {
      x: { grid: { display: false }, ticks: { maxTicksLimit: 8, maxRotation: 0, font: { size: 10 }, color: '#9ca3af' } },
      y: {
        position: 'right',
        grid: { color: '#f3f4f6' },
        ticks: {
          callback: v => { const p = (Number(v) - 1) * 100; return (p >= 0 ? '+' : '') + p.toFixed(1) + '%' },
          font: { size: 10 }, color: '#9ca3af',
        },
      },
    },
  }), [])

  const ddOptions = useMemo(() => ({
    responsive: true,
    maintainAspectRatio: false,
    animation: { duration: 0 },
    interaction: { mode: 'index', intersect: false },
    plugins: {
      legend: { position: 'top', labels: { font: { size: 11 }, boxWidth: 12 } },
      tooltip: { callbacks: { label: item => `${item.dataset.label}: ${Number(item.raw).toFixed(2)}%` } },
    },
    scales: {
      x: { grid: { display: false }, ticks: { maxTicksLimit: 8, maxRotation: 0, font: { size: 10 }, color: '#9ca3af' } },
      y: {
        position: 'right',
        grid: { color: '#f3f4f6' },
        ticks: { callback: v => `${Number(v).toFixed(1)}%`, font: { size: 10 }, color: '#9ca3af' },
      },
    },
  }), [])

  return (
    <div className="space-y-4">
      <div className="bg-white rounded-xl shadow p-4">
        {/* Range buttons */}
        <div className="flex flex-wrap gap-2 mb-4">
          <button
            onClick={() => setActiveDays(null)}
            className={`px-3 py-1 rounded-full text-xs font-medium transition-colors ${
              activeDays === null ? 'bg-blue-600 text-white' : 'bg-blue-50 text-blue-600 hover:bg-blue-100'
            }`}
          >
            共同区间
          </button>
          {RANGE_OPTIONS.map(opt => (
            <button
              key={opt.days}
              onClick={() => setActiveDays(opt.days)}
              className={`px-3 py-1 rounded-full text-xs font-medium transition-colors ${
                activeDays === opt.days ? 'bg-blue-600 text-white' : 'bg-gray-100 text-gray-600 hover:bg-gray-200'
              }`}
            >
              {opt.label}
            </button>
          ))}
        </div>

        {loading ? (
          <div className="shimmer rounded h-72" />
        ) : labels.length === 0 ? (
          <div className="h-72 flex items-center justify-center text-gray-400 text-sm">暂无数据</div>
        ) : (
          <div className="h-72">
            <Line data={mainChartData} options={mainOptions} />
          </div>
        )}
        {fromDate && (
          <p className="text-xs text-gray-400 mt-2 text-right">
            起点 {fromDate} · 净值已归一化（起点=1）
          </p>
        )}
      </div>

      {/* Drawdown */}
      {!loading && ddChartData && (
        <div className="bg-white rounded-xl shadow p-4">
          <h3 className="text-sm font-semibold text-gray-700 mb-3">动态回撤对比</h3>
          <div className="h-48">
            <Line data={ddChartData} options={ddOptions} />
          </div>
        </div>
      )}
    </div>
  )
}
