import { useMemo } from 'react'
import { FUND_COLORS, BENCHMARK_OPTIONS } from '../../utils/metricDefs.js'

function computeCorrelationMatrix(navDataMap, compareList, benchItems, benchmarkCode) {
  // Build daily return series for each fund
  const series = compareList.map(f => {
    const items = navDataMap[f.fund_id] || []
    const rets = []
    for (let i = 1; i < items.length; i++) {
      const prev = items[i - 1].unit_nav
      const curr = items[i].unit_nav
      if (prev > 0) rets.push({ date: items[i].nav_date, r: (curr - prev) / prev })
    }
    return { id: f.fund_id, name: f.product_name, rets }
  })

  // Add benchmark if available
  if (benchmarkCode && benchItems.length > 1) {
    const bLabel = BENCHMARK_OPTIONS.find(o => o.code === benchmarkCode)?.label || '基准'
    const sorted = [...benchItems].sort((a, b) => a.trade_date.localeCompare(b.trade_date))
    const rets = []
    for (let i = 1; i < sorted.length; i++) {
      const prev = sorted[i - 1].close
      const curr = sorted[i].close
      if (prev > 0) {
        const d = sorted[i].trade_date
        rets.push({ date: `${d.slice(0,4)}-${d.slice(4,6)}-${d.slice(6,8)}`, r: (curr - prev) / prev })
      }
    }
    series.push({ id: '__bench__', name: bLabel, rets })
  }

  const n = series.length
  if (n < 2) return null

  // Pairwise correlation
  const matrix = []
  for (let i = 0; i < n; i++) {
    matrix[i] = []
    for (let j = 0; j < n; j++) {
      if (i === j) { matrix[i][j] = 1; continue }
      if (j < i)   { matrix[i][j] = matrix[j][i]; continue }
      // find common dates
      const mapI = new Map(series[i].rets.map(r => [r.date, r.r]))
      const common = series[j].rets.filter(r => mapI.has(r.date))
      if (common.length < 5) { matrix[i][j] = null; continue }
      const xs = common.map(r => mapI.get(r.date))
      const ys = common.map(r => r.r)
      const n2 = xs.length
      const mx = xs.reduce((s, v) => s + v, 0) / n2
      const my = ys.reduce((s, v) => s + v, 0) / n2
      let cov = 0, vx = 0, vy = 0
      for (let k = 0; k < n2; k++) {
        const dx = xs[k] - mx, dy = ys[k] - my
        cov += dx * dy; vx += dx * dx; vy += dy * dy
      }
      matrix[i][j] = (vx > 0 && vy > 0) ? +(cov / Math.sqrt(vx * vy)).toFixed(3) : null
    }
  }
  return { series, matrix }
}

function corrColor(val) {
  if (val == null) return 'bg-gray-50 text-gray-400'
  const abs = Math.abs(val)
  if (abs >= 0.9) return val > 0 ? 'bg-red-200 text-red-900' : 'bg-emerald-200 text-emerald-900'
  if (abs >= 0.7) return val > 0 ? 'bg-red-100 text-red-700' : 'bg-emerald-100 text-emerald-700'
  if (abs >= 0.4) return val > 0 ? 'bg-orange-50 text-orange-700' : 'bg-teal-50 text-teal-700'
  return 'bg-gray-50 text-gray-600'
}

export default function ComparisonCorrelation({
  compareList, navDataMap, benchItems, benchmarkCode,
}) {
  const result = useMemo(
    () => computeCorrelationMatrix(navDataMap, compareList, benchItems, benchmarkCode),
    [navDataMap, compareList, benchItems, benchmarkCode],
  )

  if (!result) {
    return (
      <div className="bg-white rounded-xl shadow p-6 text-center text-gray-400 text-sm">
        数据不足，无法计算相关性
      </div>
    )
  }

  const { series, matrix } = result

  return (
    <div className="bg-white rounded-xl shadow overflow-hidden">
      <div className="px-5 py-3 border-b border-gray-100">
        <h3 className="text-sm font-semibold text-gray-700">相关性矩阵</h3>
        <p className="text-xs text-gray-400 mt-0.5">基于日收益率计算（皮尔逊相关系数）</p>
      </div>
      <div className="overflow-x-auto p-4">
        <table className="text-xs border-separate border-spacing-1">
          <thead>
            <tr>
              <th className="w-32" />
              {series.map((s, j) => (
                <th key={j} className="text-center font-medium text-gray-500 px-2 pb-1 max-w-[90px]">
                  <span
                    className="block truncate"
                    style={{ color: j < FUND_COLORS.length && s.id !== '__bench__' ? FUND_COLORS[j] : '#9ca3af' }}
                    title={s.name}
                  >
                    {s.name.length > 8 ? s.name.slice(0, 8) + '…' : s.name}
                  </span>
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {series.map((row, i) => (
              <tr key={i}>
                <td className="text-right pr-2 text-gray-600 font-medium max-w-[120px]">
                  <span className="truncate block" title={row.name}>
                    {row.name.length > 9 ? row.name.slice(0, 9) + '…' : row.name}
                  </span>
                </td>
                {series.map((_, j) => {
                  const val = matrix[i][j]
                  return (
                    <td
                      key={j}
                      className={`text-center py-2 px-3 rounded font-mono ${corrColor(val)}`}
                    >
                      {val != null ? val.toFixed(2) : '—'}
                    </td>
                  )
                })}
              </tr>
            ))}
          </tbody>
        </table>
        <div className="flex items-center gap-4 mt-3 text-[10px] text-gray-400">
          <span>颜色说明:</span>
          <span className="px-2 py-0.5 bg-red-200 text-red-900 rounded">≥0.9 高正相关</span>
          <span className="px-2 py-0.5 bg-red-100 text-red-700 rounded">≥0.7</span>
          <span className="px-2 py-0.5 bg-emerald-100 text-emerald-700 rounded">负相关</span>
        </div>
      </div>
    </div>
  )
}
