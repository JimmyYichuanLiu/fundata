import { useState, useEffect } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import { fetchFund, fetchFundNav, updateNav, deleteNav } from '../api.js'

import { useAuth } from '../context/AuthContext.jsx'

export default function NavDetailPage() {
  const { canManage } = useAuth()
  const [editing, setEditing] = useState(null)
  const [saving, setSaving] = useState(false)
  const [saveError, setSaveError] = useState('')
  const [revision, setRevision] = useState(0)
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
  }, [id, navigate, revision])

  const filtered = search
    ? navItems.filter(i => i.nav_date.includes(search))
    : navItems

  const save = async () => {
    const unit = Number(editing.unit_nav), accumulated = editing.accumulated_nav === '' ? null : Number(editing.accumulated_nav)
    if (!editing.nav_date || !Number.isFinite(unit) || unit <= 0 || (accumulated != null && (!Number.isFinite(accumulated) || accumulated <= 0))) { setSaveError('请填写有效日期与大于零的净值。'); return }
    setSaving(true); setSaveError('')
    try { await updateNav(editing.id, { nav_date: editing.nav_date, unit_nav: unit, accumulated_nav: accumulated }); setEditing(null); setRevision(v => v + 1) } catch (err) { setSaveError(err.message) } finally { setSaving(false) }
  }
  const remove = async () => {
    if (!window.confirm('确认删除这条净值？删除后将重新计算该基金复权净值。')) return
    setSaving(true); setSaveError('')
    try { await deleteNav(editing.id); setEditing(null); setRevision(v => v + 1) } catch (err) { setSaveError(err.message) } finally { setSaving(false) }
  }
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
              <table className="w-full text-sm min-w-[650px]">
                <thead className="bg-gray-50 border-b border-gray-100">
                  <tr className="text-left text-xs font-semibold text-gray-500 uppercase tracking-wider">
                    <th className="px-4 py-3">净值日期</th>
                    <th className="px-4 py-3 text-right">单位净值</th>
                    <th className="px-4 py-3 text-right">累计净值</th>
                    <th className="px-4 py-3 text-right">复权累计净值</th><th className="px-4 py-3">来源</th>{canManage && <th className="px-4 py-3">管理</th>}
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-50">
                  {filtered.map(item => (
                    <tr
                      key={item.id}
                      className={`hover:bg-gray-50 transition-colors ${item.data_source === 'manual' ? 'bg-orange-50/40' : ''}`}
                    >
                      <td className="px-4 py-2.5 font-mono text-gray-700 text-xs">
                        {item.nav_date}
                        {item.data_source === 'manual' && (
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
                        {item.adj_nav != null ? item.adj_nav.toFixed(4) : '—'}
                      </td><td className="px-4 py-3 whitespace-nowrap">{({ email: '邮件采集', zx_excel: 'ZX 数据', manual: '手动录入' })[item.data_source] || '—'}</td>{canManage && <td className="px-4 py-3"><button className="button-secondary" onClick={() => { setEditing({ ...item, accumulated_nav: item.accumulated_nav ?? '' }); setSaveError('') }}>编辑</button></td>}
                    </tr>
                  ))}
                  {filtered.length === 0 && (
                    <tr>
                      <td colSpan={canManage ? 6 : 5} className="px-4 py-10 text-center text-gray-400">暂无数据</td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>
          )}
        </div>
      </main>
      {canManage && editing && <div className="fixed inset-0 z-50 bg-slate-950/50 flex items-center justify-center p-4" role="dialog" aria-modal="true" aria-labelledby="nav-edit-title"><form className="panel p-6 w-full max-w-md space-y-4" onSubmit={e => { e.preventDefault(); save() }}><h2 id="nav-edit-title" className="text-lg font-bold">编辑净值记录</h2><p className="text-xs text-slate-500">{fund?.product_name} · {fund?.product_code}</p><label className="block"><span className="field-label">净值日期</span><input required type="date" className="control w-full" value={editing.nav_date} onChange={e => setEditing(p => ({ ...p, nav_date: e.target.value }))} /></label><label className="block"><span className="field-label">单位净值</span><input required type="number" step="any" min="0.000001" className="control w-full" value={editing.unit_nav} onChange={e => setEditing(p => ({ ...p, unit_nav: e.target.value }))} /></label><label className="block"><span className="field-label">累计净值（留空保留原值）</span><input type="number" step="any" min="0.000001" className="control w-full" value={editing.accumulated_nav} onChange={e => setEditing(p => ({ ...p, accumulated_nav: e.target.value }))} /></label>{saveError && <div className="notice notice-error" role="alert">{saveError}</div>}<div className="flex gap-2"><button type="button" disabled={saving} className="button-secondary text-red-600" onClick={remove}>删除记录</button><button type="button" disabled={saving} className="button-secondary ml-auto" onClick={() => setEditing(null)}>取消</button><button disabled={saving} className="button-primary">{saving ? '保存中…' : '保存净值'}</button></div></form></div>}
    </div>
  )
}
