import { FUND_COLORS } from '../../utils/metricDefs.js'

export default function ComparisonHero({ compareList, navDataMap, loading, onRemove }) {
  return (
    <div className="bg-white rounded-xl shadow p-5">
      {loading ? (
        <div className="flex gap-3 overflow-x-auto">
          {compareList.map(f => (
            <div key={f.fund_id} className="min-w-[160px] shimmer rounded-xl h-20" />
          ))}
        </div>
      ) : (
        <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-4 gap-3">
          {compareList.map((f, idx) => {
            const items  = navDataMap[f.fund_id] || []
            const latest = items[items.length - 1]
            const color  = FUND_COLORS[idx % FUND_COLORS.length]
            return (
              <div
                key={f.fund_id}
                className="relative border border-gray-100 rounded-xl p-4 hover:shadow-sm transition-shadow"
                style={{ borderLeftColor: color, borderLeftWidth: 3 }}
              >
                <button
                  onClick={() => onRemove(f.fund_id)}
                  className="absolute top-2 right-2 text-gray-300 hover:text-red-400 text-base leading-none"
                >×</button>
                <p className="text-xs text-gray-500 truncate pr-5 leading-snug">{f.product_name}</p>
                <p className="text-xl font-bold text-gray-900 mt-2 font-mono">
                  {latest?.unit_nav != null ? latest.unit_nav.toFixed(4) : '—'}
                </p>
                <p className="text-[10px] text-gray-400 mt-0.5">{latest?.nav_date || ''}</p>
              </div>
            )
          })}
        </div>
      )}
    </div>
  )
}
