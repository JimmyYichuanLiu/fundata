import { NavLink, useLocation } from 'react-router-dom'
import { useState, useEffect } from 'react'
import { Chart } from 'chart.js'
import Icon from './Icon.jsx'
import { useAuth } from '../context/AuthContext.jsx'

const NAV_ITEMS = [
  { to: '/', icon: 'analytics', label: '基金概览', caption: 'Fund universe' },
  { to: '/compare', icon: 'compare_arrows', label: '基金对比', caption: 'Compare & discover' },
  { to: '/portfolios', icon: 'account_balance_wallet', label: '组合管理', caption: 'Portfolio research' },
  { to: '/market', icon: 'trending_up', label: '市场观察', caption: 'Market overview' },
  { to: '/basis', icon: 'show_chart', label: '基差分析', caption: 'Basis monitor' },
]
export default function Layout({ children }) {
  const { admin_enabled, canManage, readonly, sessionError, retrySession } = useAuth()
  const [dark, setDark] = useState(() => { try { return localStorage.getItem('theme') === 'dark' } catch { return false } })
  const [open, setOpen] = useState(false)
  const location = useLocation()
  useEffect(() => setOpen(false), [location.pathname])
  useEffect(() => {
    document.documentElement.classList.toggle('dark', dark)
    try { localStorage.setItem('theme', dark ? 'dark' : 'light') } catch {}
    Chart.defaults.color = dark ? '#aebdcd' : '#65748b'
    Chart.defaults.borderColor = dark ? '#2b3b50' : '#e8edf3'
    Chart.defaults.font.family = 'Segoe UI, Microsoft YaHei, system-ui, sans-serif'
    Object.values(Chart.instances).forEach(chart => chart.update('none'))
  }, [dark])
  const items = admin_enabled && !readonly ? [...NAV_ITEMS, { to: '/admin', icon: 'settings', label: canManage ? '数据管理' : '管理员登录', caption: 'Data operations' }] : NAV_ITEMS
  return <div className="ft-shell">
    {open && <button className="sidebar-overlay" aria-label="关闭导航" onClick={() => setOpen(false)} />}
    <aside className={`ft-sidebar ${open ? 'is-open' : ''}`}>
      <NavLink className="brand" to="/"><span className="brand-mark"><Icon name="show_chart" /></span><span>FundTrack<small>基金净值研究终端</small></span></NavLink>
      <p className="nav-eyebrow">研究工作台</p>
      <nav aria-label="主导航">{items.map(item => <NavLink key={item.to} end={item.to === '/'} to={item.to} className={({ isActive }) => `sidebar-link ${isActive ? 'active' : ''}`}><Icon name={item.icon} /><span>{item.label}<small>{item.caption}</small></span></NavLink>)}</nav>
      <div className="sidebar-bottom"><div className="workspace-label"><span className="status-dot" />{canManage ? '管理员工作空间' : '公开研究 · 只读'}</div><button onClick={() => setDark(value => !value)} className="theme-switch"><Icon name={dark ? 'light_mode' : 'dark_mode'} />{dark ? '切换浅色模式' : '切换深色模式'}</button><small>净值源于已入库数据<br />历史表现不代表未来收益</small></div>
    </aside>
    <main className="ft-main"><div className="mobile-topbar"><button onClick={() => setOpen(true)} aria-label="打开导航"><Icon name="menu" /></button><NavLink to="/">FundTrack</NavLink><button onClick={() => setDark(value => !value)} aria-label="切换主题"><Icon name={dark ? 'light_mode' : 'dark_mode'} /></button></div>{sessionError && <div className="notice mx-4" role="alert">{sessionError} <button className="text-primary underline" onClick={retrySession}>重试身份检查</button></div>}{children}<footer className="site-footer"><span>FundTrack · 基金净值研究</span><span>数据溯源 / 净值追踪 / 组合洞察</span></footer></main>
  </div>
}
