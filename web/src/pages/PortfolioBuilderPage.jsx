import { useEffect, useMemo, useState } from 'react'
import { useLocation, useNavigate, Link } from 'react-router-dom'
import { createPortfolio, calculatePortfolio, fetchFunds, fetchFundNav } from '../api.js'
import { buildEqualWeights, updateWeights, sumWeights, buildPortfolioCalculatePayload, validatePortfolio } from '../utils/portfolio.js'
import { parseFundIds } from '../utils/selection.js'
import { useAuth } from '../context/AuthContext.jsx'
import FundPicker from '../components/FundPicker.jsx'
import PageState from '../components/PageState.jsx'

export default function PortfolioBuilderPage() {
  const navigate = useNavigate(), { search } = useLocation(), { canManage, loading: authLoading } = useAuth()
  const [funds, setFunds] = useState([]), [weights, setWeights] = useState(() => buildEqualWeights(parseFundIds(search)))
  const [name, setName] = useState('我的基金组合'), [method, setMethod] = useState('UNIFIED_START')
  const [effectiveDate, setEffectiveDate] = useState(''), [dates, setDates] = useState({})
  const [loading, setLoading] = useState(true), [saving, setSaving] = useState(false), [error, setError] = useState('')
  const [createdId, setCreatedId] = useState(null)
  useEffect(() => { const c = new AbortController(); fetchFunds(c.signal).then(setFunds).catch(err => { if (err.name !== 'AbortError') setError(err.message) }).finally(() => setLoading(false)); return () => c.abort() }, [])
  const selectionKey = weights.map(w => w.fund_id).join(',')
  useEffect(() => {
    const controller = new AbortController()
    setDates({}); setEffectiveDate('')
    if (!weights.length) return () => controller.abort()
    Promise.all(weights.map(w => fetchFundNav(w.fund_id, { limit: 5000, apply_filter: true }, controller.signal).then(items => [w.fund_id, { first: items[0]?.nav_date || '', last: items.at(-1)?.nav_date || '' }]))).then(entries => {
      const data = Object.fromEntries(entries); setDates(data)
      const firstDates = entries.map(([, d]) => d.first).filter(Boolean).sort()
      setEffectiveDate(firstDates.length ? (method === 'UNIFIED_START' ? firstDates.at(-1) : firstDates[0]) : '')
    }).catch(err => { if (err.name !== 'AbortError') setError(err.message) })
    return () => controller.abort()
  }, [selectionKey, method])
  const toggle = fund => { setCreatedId(null); setWeights(current => current.some(w => w.fund_id === fund.fund_id) ? buildEqualWeights(current.filter(w => w.fund_id !== fund.fund_id).map(w => w.fund_id)) : buildEqualWeights([...current.map(w => w.fund_id), fund.fund_id])) }
  const totalWeight = useMemo(() => sumWeights(weights), [weights])
  const issue = validatePortfolio({ weights, portfolioName: name, effectiveDate, dates, method })
  const generate = async () => {
    if (!canManage || issue) return
    setSaving(true); setError('')
    try {
      let id = createdId
      if (!id) { const created = await createPortfolio(buildPortfolioCalculatePayload({ method, portfolioName: name.trim(), weights, effectiveDate, dates })); id = created.id; setCreatedId(id) }
      await calculatePortfolio(id)
      navigate('/portfolios/' + id)
    } catch (err) { setError(err.message) } finally { setSaving(false) }
  }
  if (loading || authLoading) return <PageState loading />
  if (!canManage) return <PageState title="此操作需要管理员身份">当前看板提供只读浏览。<Link className="text-primary" to="/portfolios"> 返回组合列表</Link></PageState>
  return <div className="page-wrap"><div className="page-heading"><div><div className="eyebrow">PORTFOLIO / 组合研究</div><h1>构建基金组合</h1><p>定义投资权重与纳入方式，观察组合的长期表现。</p></div><Link className="button-secondary" to="/portfolios">返回组合列表</Link></div>
    <FundPicker funds={funds} selectedIds={weights.map(w => w.fund_id)} onToggle={toggle} />
    <section className="panel mt-5"><div className="panel-heading"><h2>组合配置</h2><span className="badge">{weights.length} 只成分基金</span></div><div className="form-grid">
      <label><span className="field-label">组合名称</span><input className="control w-full" value={name} maxLength={100} onChange={e => { setName(e.target.value); setCreatedId(null) }} /></label>
      <label><span className="field-label">起始日期</span><input className="control w-full" type="date" value={effectiveDate} onChange={e => { setEffectiveDate(e.target.value); setCreatedId(null) }} /></label>
      <div className="col-span-full"><span className="field-label">构建方式</span><div className="flex flex-wrap gap-2">{[['UNIFIED_START', '统一起始日法'], ['BATCH_INCLUDE', '分批纳入法']].map(([value, label]) => <button key={value} className={method === value ? 'button-primary' : 'button-secondary'} onClick={() => { setMethod(value); setCreatedId(null) }}>{label}</button>)}</div><p className="text-xs text-slate-500 mt-3">{method === 'UNIFIED_START' ? '从所有基金均有有效净值的日期开始，以目标权重配置。' : '每只基金在起始日与其首个净值日的较晚日期纳入，以配置比例作为终态金额比例。'}</p></div></div>
      <div className="table-scroll"><table className="research-table"><thead><tr><th className="fund-identity">成分基金</th><th>可用净值区间</th><th>配置比例</th></tr></thead><tbody>{weights.map(w => { const fund = funds.find(f => f.fund_id === w.fund_id); return <tr key={w.fund_id}><td className="fund-identity"><Link to={'/fund/' + w.fund_id}>{fund?.product_name || '基金不可用'}</Link><small>{fund?.product_code || w.fund_id}</small></td><td>{dates[w.fund_id]?.first || '—'} 至 {dates[w.fund_id]?.last || '—'}</td><td><input aria-label={`${fund?.product_name || w.fund_id} 权重百分比`} className="control w-24 text-right" type="number" min="0" max="100" step="0.01" value={Number.isFinite(w.weight) ? Number((w.weight * 100).toFixed(6)) : ''} onChange={e => { setCreatedId(null); setWeights(prev => updateWeights(prev, w.fund_id, e.target.value === '' ? NaN : Number(e.target.value) / 100)) }} /> %</td></tr> })}</tbody></table></div>
      <div className="pagination"><button className="button-secondary" onClick={() => { setWeights(buildEqualWeights(weights.map(w => w.fund_id))); setCreatedId(null) }}>重置等权</button><span>权重合计 <strong className={issue ? 'positive' : 'negative'}>{Number.isFinite(totalWeight) ? (totalWeight * 100).toFixed(2) + '%' : '待填写'}</strong></span></div></section>
    {issue && <div className="notice">{issue}</div>}{error && <div className="notice notice-error" role="alert">{error}{createdId && <span> · 组合已保存，修复数据后可重试计算。</span>}</div>}
    <div className="mt-5 flex justify-end"><button className="button-primary" disabled={saving || !!issue} onClick={generate}>{saving ? '正在生成组合…' : createdId ? '重试组合计算' : '保存并生成组合 →'}</button></div>
  </div>
}
