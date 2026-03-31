import { useState, useMemo } from 'react'
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  BarElement,
  PointElement,
  LineElement,
  RadialLinearScale,
  ArcElement,
  Tooltip,
  Legend,
  Filler,
} from 'chart.js'
import { Bar, Radar } from 'react-chartjs-2'
import { computePeriodicReturns, computeAnnualMetrics, computeMetrics } from '../../utils/metrics.js'

ChartJS.register(
  CategoryScale, LinearScale, BarElement, PointElement, LineElement,
  RadialLinearScale, ArcElement, Tooltip, Legend, Filler,
)

const FREQ_OPTIONS = [
  { label: '月度', value: 'monthly' },
  { label: '季度', value: 'quarterly' },
  { label: '半年度', value: 'semiannual' },
  { label: '年度', value: 'annual' },
]

function formatPct(val) {
  if (val == null) return '—'
  return `${val > 0 ? '+' : ''}${val.toFixed(2)}%`
}

function pctColor(val) {
  if (val == null) return 'text-gray-400'
  if (val > 0) return 'text-red-500'
  if (val < 0) return 'text-emerald-600'
  return 'text-gray-600'
}

function pctBgColor(val) {
  if (val == null) return ''
  if (val > 3) return 'bg-red-100 text-red-700'
  if (val > 0) return 'bg-red-50 text-red-600'
  if (val < -3) return 'bg-emerald-100 text-emerald-700'
  if (val < 0) return 'bg-emerald-50 text-emerald-600'
  return 'bg-gray-50 text-gray-600'
}

export default function PerformanceTab({
  navItems,
  filteredItems,
  navType,
}) {
  const [frequency, setFrequency] = useState('monthly')

  const periodicReturns = useMemo(
    () => computePeriodicReturns(filteredItems, navType, frequency),
    [filteredItems, navType, frequency],
  )

  const annualMetrics = useMemo(
    () => computeAnnualMetrics(navItems, navType),
    [navItems, navType],
  )

  const allMetrics = useMemo(
    () => computeMetrics(navItems, navType),
    [navItems, navType],
  )

  // Monthly returns grouped by year for the heatmap table
  const monthlyReturns = useMemo(
    () => computePeriodicReturns(navItems, navType, 'monthly'),
    [navItems, navType],
  )

  const yearMonthGrid = useMemo(() => {
    if (monthlyReturns.length === 0) return null
    const map = new Map()
    monthlyReturns.forEach(r => {
      map.set(r.period, r.return)
    })
    const years = [...new Set(monthlyReturns.map(r => r.period.slice(0, 4)))].sort()
    const months = ['01','02','03','04','05','06','07','08','09','10','11','12']
    return { years, months, map }
  }, [monthlyReturns])

  // Bar chart data for periodic returns
  const barData = useMemo(() => {
    if (periodicReturns.length === 0) return null
    return {
      labels: periodicReturns.map(r => r.period),
      datasets: [{
        label: '收益率',
        data: periodicReturns.map(r => r.return),
        backgroundColor: periodicReturns.map(r =>
          r.return >= 0 ? 'rgba(239,68,68,0.6)' : 'rgba(16,185,129,0.6)'
        ),
        borderColor: periodicReturns.map(r =>
          r.return >= 0 ? 'rgba(239,68,68,0.8)' : 'rgba(16,185,129,0.8)'
        ),
        borderWidth: 1,
        borderRadius: 2,
      }],
    }
  }, [periodicReturns])

  const barOptions = useMemo(() => ({
    responsive: true,
    maintainAspectRatio: false,
    animation: { duration: 0 },
    plugins: {
      legend: { display: false },
      tooltip: {
        callbacks: {
          label: (item) => `${Number(item.raw).toFixed(2)}%`,
        },
      },
    },
    scales: {
      x: {
        grid: { display: false },
        ticks: {
          maxRotation: 45,
          font: { size: 10 },
          color: '#9ca3af',
          autoSkip: true,
          maxTicksLimit: 24,
        },
      },
      y: {
        grid: { color: '#f3f4f6' },
        ticks: {
          callback: v => `${Number(v).toFixed(1)}%`,
          font: { size: 10 },
          color: '#9ca3af',
        },
      },
    },
  }), [])

  // Radar chart: annual comparison
  const radarData = useMemo(() => {
    if (annualMetrics.length < 1 || !allMetrics) return null
    const labels = ['年化收益率', '夏普比率', '最大回撤', '年化波动率', '月胜率', '卡玛比率']

    // Normalize each metric to 0-100 scale for radar
    function normalize(val, min, max) {
      if (val == null) return 0
      const clamped = Math.max(min, Math.min(max, val))
      return ((clamped - min) / (max - min)) * 100
    }

    function getRadarValues(m) {
      return [
        normalize(m.annualizedReturn, -50, 100),
        normalize(m.sharpe, -2, 5),
        normalize(-Math.abs(m.maxDrawdown || 0), -50, 0), // Less drawdown is better
        normalize(-(m.annualizedVol || 0), -80, 0), // Less vol is better
        normalize(m.monthlyWinRate, 0, 100),
        normalize(m.calmar, -2, 10),
      ]
    }

    const datasets = []
    // Overall
    datasets.push({
      label: '全区间',
      data: getRadarValues(allMetrics),
      borderColor: '#3b82f6',
      backgroundColor: 'rgba(59,130,246,0.15)',
      borderWidth: 2,
      pointRadius: 3,
      pointBackgroundColor: '#3b82f6',
    })
    // Latest year
    if (annualMetrics.length > 0) {
      const latest = annualMetrics[annualMetrics.length - 1]
      datasets.push({
        label: latest.year,
        data: getRadarValues(latest.metrics),
        borderColor: '#f59e0b',
        backgroundColor: 'rgba(245,158,11,0.1)',
        borderWidth: 2,
        pointRadius: 3,
        pointBackgroundColor: '#f59e0b',
      })
    }

    return {
      labels,
      datasets,
    }
  }, [annualMetrics, allMetrics])

  const radarOptions = useMemo(() => ({
    responsive: true,
    maintainAspectRatio: false,
    animation: { duration: 0 },
    plugins: {
      legend: { position: 'bottom', labels: { font: { size: 11 } } },
    },
    scales: {
      r: {
        beginAtZero: true,
        max: 100,
        ticks: { display: false },
        pointLabels: { font: { size: 11 }, color: '#6b7280' },
        grid: { color: '#e5e7eb' },
      },
    },
  }), [])

  if (filteredItems.length < 2) {
    return <div className="text-center text-gray-400 py-12">数据不足，无法计算</div>
  }

  return (
    <div className="space-y-4">
      {/* Periodic returns bar chart */}
      <div className="bg-white rounded-xl shadow p-4">
        <div className="flex items-center justify-between mb-4">
          <h3 className="text-sm font-semibold text-gray-700">区间收益</h3>
          <div className="flex gap-1">
            {FREQ_OPTIONS.map(opt => (
              <button
                key={opt.value}
                onClick={() => setFrequency(opt.value)}
                className={`px-3 py-1 rounded-full text-xs font-medium transition-colors ${
                  frequency === opt.value
                    ? 'bg-blue-600 text-white'
                    : 'bg-gray-100 text-gray-600 hover:bg-gray-200'
                }`}
              >
                {opt.label}
              </button>
            ))}
          </div>
        </div>
        {barData ? (
          <div className="h-56">
            <Bar data={barData} options={barOptions} />
          </div>
        ) : (
          <div className="h-56 flex items-center justify-center text-gray-400 text-sm">暂无数据</div>
        )}
      </div>

      {/* Monthly return heatmap table */}
      {yearMonthGrid && (
        <div className="bg-white rounded-xl shadow p-4">
          <h3 className="text-sm font-semibold text-gray-700 mb-3">月度收益率 (%)</h3>
          <div className="overflow-x-auto">
            <table className="w-full text-xs">
              <thead>
                <tr className="border-b border-gray-200 text-gray-500">
                  <th className="text-left py-2 font-medium pr-3">年份</th>
                  {yearMonthGrid.months.map(m => (
                    <th key={m} className="text-center py-2 font-medium px-1 w-[60px]">{parseInt(m)}月</th>
                  ))}
                  <th className="text-center py-2 font-medium px-2">全年</th>
                </tr>
              </thead>
              <tbody>
                {yearMonthGrid.years.map(year => {
                  // Compute year total
                  let yearFirst = null, yearLast = null
                  navItems.forEach(item => {
                    if (!item.nav_date || !item.nav_date.startsWith(year)) return
                    const v = navType === 'unit'
                      ? item.unit_nav
                      : navType === 'adjusted'
                        ? (item.adjusted_nav ?? item.unit_nav)
                        : (item.accumulated_nav ?? item.unit_nav)
                    if (v == null) return
                    if (yearFirst === null) yearFirst = v
                    yearLast = v
                  })
                  const yearReturn = (yearFirst && yearFirst > 0) ? (yearLast - yearFirst) / yearFirst * 100 : null

                  return (
                    <tr key={year} className="border-b border-gray-50">
                      <td className="py-1.5 pr-3 text-gray-700 font-medium">{year}</td>
                      {yearMonthGrid.months.map(m => {
                        const key = `${year}-${m}`
                        const val = yearMonthGrid.map.get(key)
                        return (
                          <td key={m} className={`py-1.5 px-1 text-center font-mono ${pctBgColor(val)} rounded`}>
                            {val != null ? val.toFixed(2) : ''}
                          </td>
                        )
                      })}
                      <td className={`py-1.5 px-2 text-center font-mono font-medium ${pctBgColor(yearReturn)} rounded`}>
                        {yearReturn != null ? yearReturn.toFixed(2) : '—'}
                      </td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* Annual metrics table */}
      {annualMetrics.length > 0 && (
        <div className="bg-white rounded-xl shadow p-4">
          <h3 className="text-sm font-semibold text-gray-700 mb-3">年度指标</h3>
          <div className="overflow-x-auto">
            <table className="w-full text-xs">
              <thead>
                <tr className="border-b border-gray-200 text-gray-500">
                  <th className="text-left py-2 font-medium">年份</th>
                  <th className="text-right py-2 font-medium">收益率</th>
                  <th className="text-right py-2 font-medium">年化波动</th>
                  <th className="text-right py-2 font-medium">最大回撤</th>
                  <th className="text-right py-2 font-medium">夏普比率</th>
                  <th className="text-right py-2 font-medium">卡玛比率</th>
                  <th className="text-right py-2 font-medium">月胜率</th>
                </tr>
              </thead>
              <tbody>
                {annualMetrics.map(({ year, metrics: m }) => (
                  <tr key={year} className="border-b border-gray-50 hover:bg-gray-50">
                    <td className="py-2 text-gray-700 font-medium">{year}</td>
                    <td className={`py-2 text-right font-mono ${pctColor(m.periodReturn)}`}>
                      {formatPct(m.periodReturn)}
                    </td>
                    <td className="py-2 text-right font-mono text-gray-700">
                      {m.annualizedVol != null ? `${m.annualizedVol.toFixed(2)}%` : '—'}
                    </td>
                    <td className="py-2 text-right font-mono text-red-500">
                      {m.maxDrawdown != null ? `${m.maxDrawdown.toFixed(2)}%` : '—'}
                    </td>
                    <td className={`py-2 text-right font-mono ${pctColor(m.sharpe)}`}>
                      {m.sharpe != null ? m.sharpe.toFixed(3) : '—'}
                    </td>
                    <td className={`py-2 text-right font-mono ${pctColor(m.calmar)}`}>
                      {m.calmar != null ? m.calmar.toFixed(3) : '—'}
                    </td>
                    <td className="py-2 text-right font-mono text-gray-700">
                      {m.monthlyWinRate != null ? `${m.monthlyWinRate.toFixed(1)}%` : '—'}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* Radar chart */}
      {radarData && (
        <div className="bg-white rounded-xl shadow p-4">
          <h3 className="text-sm font-semibold text-gray-700 mb-3">风险收益画像</h3>
          <div className="h-72 max-w-lg mx-auto">
            <Radar data={radarData} options={radarOptions} />
          </div>
          <p className="text-xs text-gray-400 text-center mt-2">
            数值已归一化至 0-100 范围，数值越高表现越好
          </p>
        </div>
      )}
    </div>
  )
}
