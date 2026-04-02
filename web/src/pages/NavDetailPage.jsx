import { useState, useEffect } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import { fetchFund, fetchFundNav } from '../api.js'

export default function NavDetailPage() {
  const { id } = useParams()
  const navigate = useNavigate()

  const [fund, setFund] = useState(null)
  const [navItems, setNavItems] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [search, setSearch] = useState('')

  useEffect(() => {
    if (!id) { navigate('/'); return }
    const numId = parseInt(id, 10)
    if (isNaN(numId)) { navigate('/'); return }

    const controller = new AbortController()
    setLoading(true)
    setError(null)

    Promise.all([
      fetchFund(numId, controller.signal),
      fetchFundNav(numId, { limit: 5000, apply_filter: false }, controller.signal),
    ])
      .then(([f, items]) => {
        setFund(f)
        setNavItems([...items].reverse()) // newest first
        setLoading(false)
      })
      .catch(err => {
        if (err.name === 'AbortError') return
        setError(err.message)
        setLoading(false)
      })

    return () => controller.abort()
  }, [id, navigate])

  const filtered = search
    ? navItems.filter(i => i.nav_date.includes(search))
    : navItems

  return (
    <div className="min-h-screen bg-gray-50">
      {/* Header */}
      <header className="bg-white border-b border-gray-200 sticky top-14 lg:top-0 z-10">
        <div className="max-w-4xl mx-auto px-4 py-3 flex items-center gap-4">
          <button
            onClick={() => navigate(`/fund/${id}`)}
            className="text-blue-600 hover:text-blue-800 text-sm flex items-center gap-1 shrink-0"
          >
            ← 返回详情
          </button>
          {fund && (
            <div className="flex items-baseline gap-2 min-w-0">
              <h1 className="text-base font-bold text-gray-900 truncate">{fund.product_name || '—'}</h1>
              <code className="text-xs text-gray-400 font-mono shrink-0">{fund.product_code}</code>
            </div>
          )}
          <span className="ml-auto text-sm text-gray-400 shrink-0">净值明细</span>
        </div>
      </header>

      <main className="max-w-4xl mx-auto px-4 py-6">
        {/* Search + count bar */}
        <div className="flex items-center justify-between mb-4 gap-3">
          <input
            type="text"
            value={search}
            onChange={e => setSearch(e.target.value)}
            placeholder="按日期筛选，如 2024-06"
            className="px-3 py-1.5 text-sm border border-gray-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500/20 focus:border-blue-400 w-56"
          />
          <span className="text-sm text-gray-400">
            共 {filtered.length} 条记录
          </span>
        </div>

        {/* Table */}
        <div className="bg-white rounded-xl shadow overflow-hidden">
          {loading ? (
            <div className="p-8 text-center text-gray-400 text-sm">加载中…</div>
          ) : error ? (
            <div className="p-8 text-center text-red-400 text-sm">{error}</div>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead className="bg-gray-50 border-b border-gray-100">
                  <tr className="text-left text-xs font-semibold text-gray-500 uppercase tracking-wider">
                    <th className="px-4 py-3">净值日期</th>
                    <th className="px-4 py-3 text-right">单位净值</th>
                    <th className="px-4 py-3 text-right">累计净值</th>
                    <th className="px-4 py-3 text-right">复权累计净值</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-50">
                  {filtered.map(item => (
                    <tr
                      key={item.id}
                      className={`hover:bg-gray-50 transition-colors ${item.source_id === null ? 'bg-orange-50/40' : ''}`}
                    >
                      <td className="px-4 py-2.5 font-mono text-gray-700 text-xs">
                        {item.nav_date}
                        {item.source_id === null && (
                          <span className="ml-1.5 text-[10px] text-orange-500 font-medium">手动</span>
                        )}
                      </td>
                      <td className="px-4 py-2.5 text-right font-mono text-gray-900 font-medium">
                        {item.unit_nav != null ? item.unit_nav.toFixed(4) : '—'}
                      </td>
                      <td className="px-4 py-2.5 text-right font-mono text-gray-600">
                        {item.accumulated_nav != null ? item.accumulated_nav.toFixed(4) : '—'}
                      </td>
                      <td className="px-4 py-2.5 text-right font-mono text-gray-500">
                        {item.adjusted_nav != null ? item.adjusted_nav.toFixed(4) : '—'}
                      </td>
                    </tr>
                  ))}
                  {filtered.length === 0 && (
                    <tr>
                      <td colSpan={4} className="px-4 py-10 text-center text-gray-400">暂无数据</td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>
          )}
        </div>
      </main>
    </div>
  )
}
