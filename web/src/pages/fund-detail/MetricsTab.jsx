import { useState, useMemo } from 'react'
import { computeMetrics, computeBenchmarkMetrics, computeExcessMetrics } from '../../utils/metrics.js'

const PERIOD_OPTIONS = [
  { label: '近1年', days: 365 },
  { label: '近3年', days: 1095 },
  { label: '成立来', days: 0 },
]

function fmt(val, format) {
  if (val == null) return '—'
  if (format === 'pct') return `${val >= 0 ? '+' : ''}${val.toFixed(2)}%`
  if (format === 'ratio') return val.toFixed(3)
  if (format === 'days') return `${Math.round(val)}天`
  return String(val)
}

function valColor(val, format, invertGood) {
  if (val == null) return 'text-gray-500'
  if (format === 'days') return 'text-gray-700'
  const positive = invertGood ? val < 0 : val > 0
  const negative = invertGood ? val > 0 : val < 0
  if (positive) return 'text-red-500'
  if (negative) return 'text-emerald-600'
  return 'text-gray-700'
}

function MetricRow({ label, fundVal, benchVal, format, invertGood }) {
  return (
    <tr className="border-b border-gray-50 hover:bg-gray-50">
      <td className="py-2 pr-2 text-xs text-gray-600 whitespace-nowrap">{label}</td>
      <td className={`py-2 px-2 text-right font-mono text-xs ${valColor(fundVal, format, invertGood)}`}>
        {fmt(fundVal, format)}
      </td>
      <td className={`py-2 px-2 text-right font-mono text-xs ${valColor(benchVal, format, invertGood)}`}>
        {fmt(benchVal, format)}
      </td>
      <td className="py-2 pl-2 text-right text-xs text-gray-300">—</td>
      <td className="py-2 pl-2 text-right text-xs text-gray-300">—</td>
    </tr>
  )
}

function MetricSection({ title, children }) {
  return (
    <div className="bg-white rounded-xl shadow p-4">
      <h3 className="text-sm font-semibold text-gray-700 mb-3">{title}</h3>
      <div className="overflow-x-auto">
        <table className="w-full text-xs">
          <thead>
            <tr className="border-b border-gray-200 text-gray-400">
              <th className="text-left py-2 pr-2 font-medium">指标</th>
              <th className="text-right py-2 px-2 font-medium">本基金</th>
              <th className="text-right py-2 px-2 font-medium">基准</th>
              <th className="text-right py-2 pl-2 font-medium text-gray-300">同类均</th>
              <th className="text-right py-2 pl-2 font-medium text-gray-300">同类排名</th>
            </tr>
          </thead>
          <tbody>{children}</tbody>
        </table>
      </div>
    </div>
  )
}

export default function MetricsTab({
  navItems,
  filteredItems,
  navType,
  benchmarkCode,
  normalizedData,
  benchmarkItems,
}) {
  const [period, setPeriod] = useState(365)

  // Build period-filtered items from navItems based on selected period
  const periodItems = useMemo(() => {
    if (!navItems || navItems.length === 0) return []
    if (period === 0) return navItems
    const latest = navItems[navItems.length - 1].nav_date
    const d = new Date(latest)
    d.setDate(d.getDate() - period)
    const from = d.toISOString().slice(0, 10)
    return navItems.filter(i => i.nav_date >= from)
  }, [navItems, period])

  const metricsNavType = navType === 'return' ? 'unit' : navType

  const fm = useMemo(() => computeMetrics(periodItems, metricsNavType), [periodItems, metricsNavType])

  // Align benchmark to periodItems date range
  const benchAligned = useMemo(() => {
    if (!normalizedData || !benchmarkCode || periodItems.length === 0) return null
    const { labels, benchNorm } = normalizedData
    if (!labels || !benchNorm) return null
    const fromDate = periodItems[0].nav_date
    const result = []
    for (let i = 0; i < labels.length; i++) {
      if (labels[i] >= fromDate && benchNorm[i] != null) {
        result.push({ nav_date: labels[i], unit_nav: benchNorm[i] })
      }
    }
    return result.length >= 2 ? result : null
  }, [normalizedData, benchmarkCode, periodItems])

  const bm = useMemo(() => {
    if (!benchAligned) return null
    return computeMetrics(benchAligned, 'unit')
  }, [benchAligned])

  const rel = useMemo(() => {
    if (!benchAligned || !benchmarkCode) return null
    return computeBenchmarkMetrics(periodItems, benchAligned, metricsNavType)
  }, [periodItems, benchAligned, metricsNavType, benchmarkCode])

  const exc = useMemo(() => {
    if (!benchAligned || !benchmarkCode) return null
    return computeExcessMetrics(periodItems, benchAligned, metricsNavType, 'geometric')
  }, [periodItems, benchAligned, metricsNavType, benchmarkCode])

  if (!fm) {
    return <div className="text-center text-gray-400 py-12">数据不足，无法计算</div>
  }

  return (
    <div className="space-y-4">
      {/* Period selector */}
      <div className="bg-white rounded-xl shadow p-3 flex items-center gap-2">
        <span className="text-xs text-gray-500">统计区间:</span>
        {PERIOD_OPTIONS.map(opt => (
          <button
            key={opt.days}
            onClick={() => setPeriod(opt.days)}
            className={`px-3 py-1 rounded-full text-xs font-medium transition-colors ${
              period === opt.days
                ? 'bg-blue-600 text-white'
                : 'bg-gray-100 text-gray-600 hover:bg-gray-200'
            }`}
          >
            {opt.label}
          </button>
        ))}
        {benchmarkCode && (
          <span className="ml-auto text-xs text-gray-400">基准列 = 当前选中指数</span>
        )}
      </div>

      {/* 2-column grid on lg */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        {/* 收益能力 */}
        <MetricSection title="收益能力">
          <MetricRow label="区间收益"     fundVal={fm.periodReturn}     benchVal={bm?.periodReturn}     format="pct" />
          <MetricRow label="年化收益"     fundVal={fm.annualizedReturn}  benchVal={bm?.annualizedReturn}  format="pct" />
          <MetricRow label="Alpha"        fundVal={rel?.alpha}           benchVal={null}                  format="pct" />
          <MetricRow label="月胜率"       fundVal={fm.monthlyWinRate}    benchVal={bm?.monthlyWinRate}    format="pct" />
          <MetricRow label="Beta (进攻)"  fundVal={rel?.beta}            benchVal={null}                  format="ratio" />
        </MetricSection>

        {/* 投资性价比 */}
        <MetricSection title="投资性价比">
          <MetricRow label="夏普比率"   fundVal={fm.sharpe}   benchVal={bm?.sharpe}   format="ratio" />
          <MetricRow label="索提诺"     fundVal={fm.sortino}  benchVal={bm?.sortino}  format="ratio" />
          <MetricRow label="卡玛比率"   fundVal={fm.calmar}   benchVal={bm?.calmar}   format="ratio" />
          <MetricRow label="信息比率"   fundVal={rel?.informationRatio} benchVal={null} format="ratio" />
        </MetricSection>

        {/* 抗风险能力 */}
        <MetricSection title="抗风险能力">
          <MetricRow label="最大回撤"     fundVal={fm.maxDrawdown}       benchVal={bm?.maxDrawdown}       format="pct"   invertGood />
          <MetricRow label="回撤回补期"   fundVal={fm.maxDDRecoveryDays} benchVal={bm?.maxDDRecoveryDays} format="days"  />
          <MetricRow label="年化波动率"   fundVal={fm.annualizedVol}     benchVal={bm?.annualizedVol}     format="pct"   invertGood />
          <MetricRow label="下行风险"     fundVal={fm.downsideRisk}      benchVal={bm?.downsideRisk}      format="pct"   invertGood />
          <MetricRow label="Beta (防守)"  fundVal={rel?.beta}            benchVal={null}                  format="ratio" />
          <MetricRow label="跟踪误差"     fundVal={rel?.trackingError}   benchVal={null}                  format="pct"   invertGood />
        </MetricSection>

        {/* 超额指标 */}
        <MetricSection title="超额指标">
          <MetricRow label="超额收益"     fundVal={exc?.periodExcess}     benchVal={null} format="pct" />
          <MetricRow label="超额年化"     fundVal={exc?.annualizedExcess} benchVal={null} format="pct" />
          <MetricRow label="超额波动率"   fundVal={exc?.excessVol}        benchVal={null} format="pct" invertGood />
          <MetricRow label="超额最大回撤" fundVal={exc?.excessMaxDD}      benchVal={null} format="pct" invertGood />
          <MetricRow label="超额夏普"     fundVal={exc?.excessSharpe}     benchVal={null} format="ratio" />
          <MetricRow label="相关系数"     fundVal={rel?.correlation}      benchVal={null} format="ratio" />
        </MetricSection>
      </div>

      {!benchmarkCode && (
        <p className="text-xs text-gray-400 text-center">
          在「业绩走势」tab 选择基准指数后，可显示基准对比和超额指标
        </p>
      )}

      <p className="text-xs text-gray-400 text-center">
        区间 {fm.days} 天 · 无风险利率 2.5% · 同类数据暂未接入
      </p>
    </div>
  )
}
