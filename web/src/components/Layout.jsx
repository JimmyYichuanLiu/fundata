import { NavLink, useLocation } from 'react-router-dom'
import { useState, useEffect } from 'react'

const NAV_ITEMS = [
  { to: '/', icon: 'analytics', label: '基金' },
  { to: '/market', icon: 'trending_up', label: '市场' },
  { to: '/compare', icon: 'compare_arrows', label: '基金对比' },
  { to: '/basis', icon: 'show_chart', label: '基差分析' },
  { to: '/crude', icon: 'local_gas_station', label: '原油' },
]

function SidebarLink({ to, icon, label }) {
  return (
    <NavLink
      to={to}
      end={to === '/'}
      className={({ isActive }) =>
        `flex items-center gap-3 px-3 py-2 rounded-lg font-medium transition-colors ${
          isActive
            ? 'bg-primary/10 text-primary'
            : 'text-slate-600 dark:text-slate-400 hover:bg-slate-100 dark:hover:bg-slate-800'
        }`
      }
    >
      <span className="material-symbols-outlined">{icon}</span>
      <span>{label}</span>
    </NavLink>
  )
}

export default function Layout({ children }) {
  const [dark, setDark] = useState(() => {
    if (typeof window === 'undefined') return true
    const saved = localStorage.getItem('theme')
    return saved ? saved === 'dark' : true
  })
  const [sidebarOpen, setSidebarOpen] = useState(false)
  const location = useLocation()

  useEffect(() => {
    setSidebarOpen(false)
  }, [location.pathname])

  useEffect(() => {
    document.documentElement.classList.toggle('dark', dark)
    localStorage.setItem('theme', dark ? 'dark' : 'light')
  }, [dark])

  return (
    <div className="bg-background-light dark:bg-background-dark text-slate-900 dark:text-slate-100 min-h-screen flex">
      {/* Mobile overlay */}
      {sidebarOpen && (
        <div
          className="fixed inset-0 bg-black/40 z-40 lg:hidden"
          onClick={() => setSidebarOpen(false)}
        />
      )}

      {/* Sidebar */}
      <aside
        className={`w-64 border-r border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-900 flex-col fixed lg:sticky top-0 h-screen z-50 transition-transform lg:translate-x-0 ${
          sidebarOpen ? 'translate-x-0 flex' : '-translate-x-full lg:flex hidden lg:!flex'
        }`}
      >
        <div className="p-6 flex items-center gap-3">
          <div className="w-8 h-8 bg-primary rounded-lg flex items-center justify-center text-white shadow-lg shadow-primary/20">
            <span className="material-symbols-outlined text-sm">dashboard</span>
          </div>
          <span className="font-bold text-xl tracking-tight">FundTrack</span>
        </div>

        <nav className="flex-1 px-4 space-y-1 mt-4">
          {NAV_ITEMS.map(item => (
            <SidebarLink key={item.to} {...item} />
          ))}
        </nav>

        <div className="p-4 border-t border-slate-200 dark:border-slate-800">
          <button
            onClick={() => setDark(d => !d)}
            className="flex items-center gap-3 px-3 py-2 w-full text-slate-600 dark:text-slate-400 hover:bg-slate-100 dark:hover:bg-slate-800 rounded-lg transition-colors"
          >
            <span className="material-symbols-outlined">
              {dark ? 'light_mode' : 'dark_mode'}
            </span>
            <span>{dark ? '浅色模式' : '深色模式'}</span>
          </button>
        </div>
      </aside>

      {/* Main content */}
      <main className="flex-1 flex flex-col min-w-0">
        {/* Mobile topbar */}
        <div className="lg:hidden sticky top-0 z-40 h-14 bg-white dark:bg-slate-900 border-b border-slate-200 dark:border-slate-700 flex items-center px-4 gap-3 shadow-sm">
          <button onClick={() => setSidebarOpen(true)} className="w-10 h-10 flex items-center justify-center rounded-lg border border-slate-200 dark:border-slate-700">
            <span className="material-symbols-outlined">menu</span>
          </button>
          <span className="font-bold text-base tracking-tight">FundTrack</span>
        </div>
        {children}
      </main>
    </div>
  )
}
