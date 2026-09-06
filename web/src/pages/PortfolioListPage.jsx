import { useEffect, useState } from 'react'
import { Link } from 'react-router-dom'
import { fetchPortfolios } from '../api.js'
import { useAuth } from '../context/AuthContext.jsx'
import PageState from '../components/PageState.jsx'
export default function PortfolioListPage() {
  const { canManage } = useAuth()
  const [items, setItems] = useState([]), [loading, setLoading] = useState(true), [error, setError] = useState(''), [version, setVersion] = useState(0)
  useEffect(() => { const c = new AbortController(); setLoading(true); setError(''); fetchPortfolios(c.signal).then(data => setItems(data.items || [])).catch(err => { if (err.name !== 'AbortError') setError(err.message) }).finally(() => { if (!c.signal.aborted) setLoading(false) }); return () => c.abort() }, [version])
  return <div className="page-wrap"><div className="page-heading"><div><div className="eyebrow">PORTFOLIO / 组合研究</div><h1>组合管理</h1><p>将基金研究转化为组合视角，持续观察配置效果。</p></div>{canManage && <Link className="button-primary" to="/portfolios/new">+ 创建组合</Link>}</div><section className="panel"><div className="panel-heading"><h2>研究组合</h2><span className="badge">{items.length} 个组合</span></div>{loading ? <PageState loading /> : error ? <PageState error={error} onRetry={() => setVersion(v => v + 1)} /> : !items.length ? <PageState title="还没有研究组合">{canManage ? '点击创建组合，选择基金并配置权重。' : '管理员保存组合后，可在此查看净值与表现。'}</PageState> : <div className="table-scroll"><table className="research-table"><thead><tr><th className="fund-identity">组合名称</th><th>构建方式</th><th>创建时间</th><th>分析</th></tr></thead><tbody>{items.map(item => <tr key={item.id}><td className="fund-identity"><Link to={'/portfolios/' + item.id}>{item.portfolio_name}</Link></td><td>{item.build_method === 'BATCH_INCLUDE' ? '分批纳入法' : '统一起始日法'}</td><td>{item.created_at?.slice(0, 10) || '—'}</td><td><Link className="text-primary" to={'/portfolios/' + item.id}>查看组合 ↗</Link></td></tr>)}</tbody></table></div>}</section></div>
}
