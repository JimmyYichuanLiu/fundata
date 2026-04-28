import { useState, useMemo } from 'react'
import { computeMetrics, computeExcessMetrics } from '../../utils/metrics.js'
import {
  METRIC_GROUPS, EXCESS_METRIC_GROUPS, fmtMetric, metricColor, FUND_COLORS,
} from '../../utils/metricDefs.js'
import { BENCHMARK_OPTIONS } from '../../utils/metricDefs.js'

const PERIOD_OPTIONS = [
  { label: '近1年',  days: 365  },
  { label: '近3年',  days: 1095 },
  { label: '成立来', days: 0    },
]

function filterByDays(items, days) {
  if (!items || items.length === 0) return []
  if (days === 0) return items
  const last = items[items.length - 1].nav_date
  const normalized = /^\d{8}$/.test(last)
    ? `${last.slice(0,4)}-${last.slice(4,6)}-${last.slice(6,8)}`
    : last
  const d = new Date(`${normalized}T00:00:00`)
  if (Number.isNaN(d.getTime())) return items
  d.setDate(d.getDate() - days)
  const from = d.toISOString().slice(0, 10)
  return items.filter(i => i.nav_date >= from)
}

// Convert benchItems (index daily) to nav-like array
function buildBenchAligned(benchItems, fromDate) {
  if (!benchItems || benchItems.length === 0) return []
  return benchItems
    .map(i => ({
      nav_date: `${i.trade_date.slice(0,4)}-${i.trade_date.slice(4,6)}-${i.trade_date.slice(6,8)}`,
      unit_nav: i.close,
    }))
    .filter(i => i.nav_date >= fromDate)
}

// Convert fund navItems to the same shape for computeExcessMetrics
function fundItemsAsNav(items) {
  return items.map(i => ({ nav_date: i.nav_date, unit_nav: i.unit_nav }))
}

function bestIdx(vals, invertGood) {
  const nums = vals.map(v => (v == null ? null : Number(v)))
  const valid = nums.filter(v => v != null)
  if (valid.length < 2) return -1
  const best = invertGood ? Math.min(...valid) : Math.max(...valid)
  return nums.findIndex(v => v === best)
}

export default function ComparisonMetrics({
  compareList, navDataMap, benchItems, benchmarkCode,
}) {
  const [period, setPeriod]           = useState(365)
  const [excessMode, setExcessMode]   = useState(false)
  // fund_id of fund used as benchmark, null = use index benchmark
  const [baseFundId, setBaseFundId]   = useState(null)

  const benchLabel = BENCHMARK_OPTIONS.find(o => o.code === benchmarkCode)?.label || '基准指数'

  // Per-fund absolute metrics (always computed)
  const absMetrics = useMemo(() => {
    return compareList.map(f => {
      const items = filterByDays(navDataMap[f.fund_id] || [], period)
      const fm    = computeMetrics(items, 'unit')
      return { fund_id: f.fund_id, name: f.product_name, items, fm }
    })
  }, [compareList, navDataMap, period])

  // When excessMode + baseFundId: compute excess of each fund vs the base fund
  // When excessMode + !baseFundId: compute excess vs index benchmark
  const metricsData = useMemo(() => {
    if (!excessMode) return absMetrics.map(d => ({ ...d, exc: null }))

    // Determine base series
    const baseEntry = baseFundId
      ? absMetrics.find(d => d.fund_id === baseFundId)
      : null

    return absMetrics.map(d => {
      if (baseFundId && d.fund_id === baseFundId) {
        // Base fund shows its own absolute metrics
        return { ...d, exc: null, isBase: true }
      }

      let baseItems
      if (baseFundId && baseEntry) {
        baseItems = fundItemsAsNav(baseEntry.items)
      } else if (benchmarkCode) {
        const fromDate = d.items.length > 0 ? d.items[0].nav_date : ''
        baseItems = buildBenchAligned(benchItems, fromDate)
      } else {
        return { ...d, exc: null }
      }

      if (baseItems.length < 2) return { ...d, exc: null }
      const exc = computeExcessMetrics(d.items, baseItems, 'unit', 'geometric')
      return { ...d, exc }
    })
  }, [absMetrics, excessMode, baseFundId, benchmarkCode, benchItems])

  const groups = (excessMode && !baseFundId && !benchmarkCode)
    ? METRIC_GROUPS   // no base at all, fall back
    : excessMode
      ? EXCESS_METRIC_GROUPS
      : METRIC_GROUPS

  const baseLabel = baseFundId
    ? (compareList.find(f => f.fund_id === baseFundId)?.product_name || '基准基金')
    : benchLabel

  return (
    <div className="space-y-4">
      {/* Controls */}
      <div className="bg-white rounded-xl shadow p-4 flex flex-wrap items-center gap-3">
        <span className="text-xs text-gray-500">统计区间:</span>
        {PERIOD_OPTIONS.map(opt => (
          <button
            key={opt.days}
            onClick={() => setPeriod(opt.days)}
            className={`px-3 py-1 rounded-full text-xs font-medium transition-colors ${
              period === opt.days ? 'bg-blue-600 text-white' : 'bg-gray-100 text-gray-600 hover:bg-gray-200'
            }`}
          >
            {opt.label}
          </button>
        ))}
        <label className="ml-auto flex items-center gap-1.5 cursor-pointer select-none text-xs text-gray-600">
          <input
            type="checkbox"
            checked={excessMode}
            onChange={e => { setExcessMode(e.target.checked); if (!e.target.checked) setBaseFundId(null) }}
            className="w-3.5 h-3.5 accent-blue-600"
          />
          超额指标
        </label>
        {excessMode && (
          <span className="text-xs text-blue-600">
            基准: {baseLabel}
          </span>
        )}
      </div>

      {/* Metric groups */}
      {groups.map(group => (
        <MetricGroupTable
          key={group.key}
          group={group}
          metricsData={metricsData}
          excessMode={excessMode}
          baseFundId={baseFundId}
          onSetBase={fid => setBaseFundId(prev => prev === fid ? null : fid)}
          colors={FUND_COLORS}
        />
      ))}

      <p className="text-xs text-gray-400 text-center">
        {absMetrics[0]?.fm?.days != null ? `区间约 ${absMetrics[0].fm.days} 天 · ` : ''}无风险利率 1.75%
        {excessMode ? ` · 超额基准: ${baseLabel}（几何超额）` : ''}
      </p>
    </div>
  )
}

function MetricGroupTable({ group, metricsData, excessMode, baseFundId, onSetBase, colors }) {
  return (
    <div className="bg-white rounded-xl shadow overflow-hidden">
      <div className="px-5 py-3 border-b border-gray-100">
        <h3 className="text-sm font-semibold text-gray-700">{group.label}</h3>
      </div>
      <div className="overflow-x-auto">
        <table className="w-full text-xs">
          <thead>
            <tr className="bg-gray-50 border-b border-gray-100">
              <th className="text-left px-5 py-2.5 font-medium text-gray-500 w-32">指标</th>
              {metricsData.map((d, idx) => (
                <th key={d.fund_id} className="text-right px-4 py-2.5 font-medium min-w-[120px]">
                  <div className="flex flex-col items-end gap-1">
                    <span
                      style={{ color: colors[idx % colors.length] }}
                      className="truncate block max-w-[140px]"
                      title={d.name}
                    >
                      {d.name.length > 10 ? d.name.slice(0, 10) + '…' : d.name}
                    </span>
                    {/* Base fund checkbox */}
                    <label
                      className="flex items-center gap-1 cursor-pointer text-[10px] text-gray-400 hover:text-blue-500 font-normal"
                      title="设为基准基金"
                    >
                      <input
                        type="checkbox"
                        checked={baseFundId === d.fund_id}
                        onChange={() => onSetBase(d.fund_id)}
                        className="w-3 h-3 accent-blue-600"
                      />
                      基准
                    </label>
                  </div>
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {group.metrics.map(m => {
              // For non-excess rows, always use absolute values for best highlighting
              // For excess rows, use excess values; base fund column shows absolute
              const vals = metricsData.map(d => {
                if (excessMode && !d.isBase) {
                  const excMap = {
                    periodExcess:     d.exc?.periodExcess,
                    annualizedExcess: d.exc?.annualizedExcess,
                    excessSharpe:     d.exc?.excessSharpe,
                    alpha:            d.exc?.excessSharpe != null ? d.exc?.periodExcess : null, // fallback
                    excessMaxDD:      d.exc?.excessMaxDD,
                    excessVol:        d.exc?.excessVol,
                  }
                  // alpha special case from abs
                  if (m.key === 'alpha') return d.exc?.periodExcess ?? null
                  return excMap[m.key] ?? null
                }
                return d.fm?.[m.key] ?? null
              })
              const best = bestIdx(vals, m.invertGood)
              return (
                <tr key={m.key} className="border-b border-gray-50 hover:bg-gray-50/60">
                  <td className="px-5 py-2.5 text-gray-600">{m.label}</td>
                  {vals.map((val, idx) => {
                    const isBase = metricsData[idx].isBase
                    return (
                      <td
                        key={idx}
                        className={`px-4 py-2.5 text-right font-mono ${metricColor(val, m.format, m.invertGood)} ${
                          idx === best ? 'bg-blue-50 font-semibold' : ''
                        } ${isBase ? 'bg-amber-50/60' : ''}`}
                      >
                        {fmtMetric(val, m.format)}
                        {isBase && excessMode && (
                          <span className="ml-1 text-[9px] text-amber-500 font-normal">基准</span>
                        )}
                      </td>
                    )
                  })}
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>
    </div>
  )
}
