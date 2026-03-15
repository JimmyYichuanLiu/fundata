import { useMemo } from 'react'
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Filler,
  Tooltip,
} from 'chart.js'
import { Line } from 'react-chartjs-2'
import { computeMetrics, computeBenchmarkMetrics, computeTopDrawdowns } from '../../utils/metrics.js'

ChartJS.register(CategoryScale, LinearScale, PointElement, LineElement, Filler, Tooltip)

// ── Metric definition table ──

const METRIC_DEFS = [
  { key: 'periodReturn',      label: '区间收益率',     format: 'pct',   group: 'return' },
  { key: 'annualizedReturn',  label: '年化收益率',     format: 'pct',   group: 'return' },
  { key: 'annualizedVol',     label: '年化波动率',     format: 'pct',   group: 'risk' },
  { key: 'downsideRisk',      label: '下行风险',       format: 'pct',   group: 'risk' },
  { key: 'maxDrawdown',       label: '最大回撤',       format: 'pct',   group: 'risk' },
  { key: 'maxDDRecoveryDays', label: '回撤回补期',     format: 'days',  group: 'risk' },
  { key: 'sharpe',            label: '夏普比率',       format: 'ratio', group: 'ratio' },
  { key: 'sortino',           label: '索提诺比率',     format: 'ratio', group: 'ratio' },
  { key: 'calmar',            label: '卡玛比率',       format: 'ratio', group: 'ratio' },
  { key: 'monthlyWinRate',    label: '月胜率',         format: 'pct',   group: 'other' },
  { key: 'longestNoNewHigh',  label: '最长不创新高',   format: 'days',  group: 'other' },
  { key: 'skewness',          label: '偏度',           format: 'ratio', group: 'risk' },
  { key: 'kurtosis',          label: '峰度',           format: 'ratio', group: 'risk' },
  { key: 'var95',             label: 'VaR (95%)',     format: 'pct',   group: 'risk' },
]

const BENCH_METRIC_DEFS = [
  { key: 'correlation',      label: '相关系数',     format: 'ratio' },
  { key: 'beta',             label: 'Beta',        format: 'ratio' },
  { key: 'alpha',            label: 'Alpha',       format: 'pct' },
  { key: 'trackingError',    label: '跟踪误差',     format: 'pct' },
  { key: 'informationRatio', label: '信息比率',     format: 'ratio' },
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
  if (val > 0) return 'text-emerald-600'
  if (val < 0) return 'text-red-500'
  return 'text-gray-700'
}

export default function MetricsTab({
  filteredItems,
  navType,
  benchmarkCode,
  normalizedData,
}) {
  const fundMetrics = useMemo(() => computeMetrics(filteredItems, navType), [filteredItems, navType])

  // Build benchmark items aligned to fund dates for computeBenchmarkMetrics
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
    return computeBenchmarkMetrics(filteredItems, benchAlignedItems, navType)
  }, [filteredItems, benchAlignedItems, navType, benchmarkCode])

  const topDrawdowns = useMemo(
    () => computeTopDrawdowns(filteredItems, navType, 5),
    [filteredItems, navType],
  )

  // Drawdown chart
  const drawdownSeries = useMemo(() => {
    if (filteredItems.length < 2) return null
    const getVal = item => navType === 'unit' ? item.unit_nav : (item.accumulated_nav ?? item.unit_nav)
    let peak = getVal(filteredItems[0])
    const dd = filteredItems.map(item => {
      const v = getVal(item)
      if (v > peak) peak = v
      return peak > 0 ? ((v - peak) / peak) * 100 : 0
    })
    if (Math.min(...dd) >= -0.01) return null
    return dd
  }, [filteredItems, navType])

  const ddChartData = useMemo(() => {
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

  const ddOptions = useMemo(() => ({
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

  if (!fundMetrics) {
    return <div className="text-center text-gray-400 py-12">数据不足，无法计算指标</div>
  }

  const hasBench = !!(benchMetrics && benchmarkCode)

  return (
    <div className="space-y-4">
      {/* Metrics comparison table */}
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
                    <td className={`py-2 px-4 text-right font-mono text-xs ${metricColor(fundVal, m.format)}`}>
                      {formatMetric(fundVal, m.format)}
                    </td>
                    {hasBench && (
                      <td className={`py-2 px-4 text-right font-mono text-xs ${metricColor(benchVal, m.format)}`}>
                        {formatMetric(benchVal, m.format)}
                      </td>
                    )}
                    {hasBench && (
                      <td className={`py-2 pl-4 text-right font-mono text-xs ${metricColor(excess, 'pct')}`}>
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

      {/* Benchmark-relative metrics */}
      {benchRelativeMetrics && (
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

      {/* Drawdown chart */}
      {ddChartData && (
        <div className="bg-white rounded-xl shadow p-4">
          <h3 className="text-sm font-semibold text-gray-700 mb-3">动态回撤</h3>
          <div className="h-48">
            <Line data={ddChartData} options={ddOptions} />
          </div>
        </div>
      )}

      {/* Top 5 drawdowns */}
      {topDrawdowns.length > 0 && (
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
    </div>
  )
}
