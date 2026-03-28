import { useState, useEffect, useCallback } from 'react'
import { Link } from 'react-router-dom'
import { fetchCrudeDaily, daysAgoYYYYMMDD } from '../api/crudeApi.js'

const SYMBOL_COLORS = {
  WTI:      '#2563eb',
  BRENT:    '#16a34a',
  SC:       '#ea580c',
  MURBAN:   '#7c3aed',
  DME_OMAN: '#0891b2',
}

function yyyymmddToDisplay(s) {
  if (!s || s.length !== 8) return s || ''
  return `${s.slice(0, 4)}-${s.slice(4, 6)}-${s.slice(6, 8)}`
}

function fmt(v, decimals = 2) {
  if (v == null) return <span className="text-slate-300">—</span>
  return v.toFixed(decimals)
}

export default function CrudeDataTable() {
  const [items, setItems]   = useState([])
  const [loading, setLoading] = useState(false)
  const [error, setError]   = useState(null)

  const load = useCallback(async (signal) => {
    setLoading(true)
    setError(null)
    try {
      const res = await fetchCrudeDaily({ limit: 2000 }, signal)
      setItems((res.items || []).slice().reverse())  // 降序展示
    } catch (e) {
      if (e.name !== 'AbortError') setError(e.message)
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    const ac = new AbortController()
    load(ac.signal)
    return () => ac.abort()
  }, [load])

  const rows = items.slice(0, 30)

  return (
    <div className="px-4 py-4 md:p-6 max-w-5xl mx-auto space-y-4">
      {/* 页头 */}
      <div className="flex items-center gap-3">
        <Link
          to="/crude"
          className="flex items-center gap-1 text-sm text-slate-500 hover:text-primary transition-colors"
        >
          <span className="material-symbols-outlined text-base">arrow_back</span>
          返回原油
        </Link>
        <h1 className="text-xl font-bold">近30交易日原油数据</h1>
      </div>

      {loading && (
        <div className="flex items-center gap-2 text-slate-400 text-sm">
          <span className="material-symbols-outlined animate-spin text-base">progress_activity</span>
          加载中…
        </div>
      )}
      {error && (
        <div className="bg-red-50 border border-red-200 rounded-lg px-4 py-2 text-sm text-red-700">{error}</div>
      )}

      {!loading && rows.length > 0 && (
        <div className="bg-white dark:bg-slate-900 rounded-xl border border-slate-200 dark:border-slate-700 shadow-sm overflow-hidden">
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="bg-slate-50 dark:bg-slate-800 text-left">
                  <th className="px-4 py-3 font-medium text-slate-600 dark:text-slate-400">日期</th>
                  <th className="px-4 py-3 font-medium text-right" style={{ color: SYMBOL_COLORS.WTI }}>WTI<br/><span className="text-[10px] font-normal text-slate-400">USD/桶</span></th>
                  <th className="px-4 py-3 font-medium text-right" style={{ color: SYMBOL_COLORS.BRENT }}>Brent<br/><span className="text-[10px] font-normal text-slate-400">USD/桶</span></th>
                  <th className="px-4 py-3 font-medium text-right" style={{ color: SYMBOL_COLORS.SC }}>上海SC<br/><span className="text-[10px] font-normal text-slate-400">CNY/桶</span></th>
                  <th className="px-4 py-3 font-medium text-right" style={{ color: SYMBOL_COLORS.SC }}>SC（USD）<br/><span className="text-[10px] font-normal text-slate-400">USD/桶</span></th>
                  <th className="px-4 py-3 font-medium text-right text-slate-500">USD/CNY<br/><span className="text-[10px] font-normal text-slate-400">汇率</span></th>
                  <th className="px-4 py-3 font-medium text-right" style={{ color: SYMBOL_COLORS.MURBAN }}>Murban<br/><span className="text-[10px] font-normal text-slate-400">USD/桶</span></th>
                  <th className="px-4 py-3 font-medium text-right" style={{ color: SYMBOL_COLORS.DME_OMAN }}>DME Oman<br/><span className="text-[10px] font-normal text-slate-400">USD/桶</span></th>
                </tr>
              </thead>
              <tbody>
                {rows.map((row, idx) => (
                  <tr
                    key={row.trade_date}
                    className={`border-t border-slate-100 dark:border-slate-800 hover:bg-slate-50/60 dark:hover:bg-slate-800/30 transition-colors ${
                      idx % 2 === 0 ? '' : 'bg-slate-50/30 dark:bg-slate-800/10'
                    }`}
                  >
                    <td className="px-4 py-2.5 text-slate-500 font-mono text-xs">{yyyymmddToDisplay(row.trade_date)}</td>
                    <td className="px-4 py-2.5 text-right font-mono">{fmt(row.WTI)}</td>
                    <td className="px-4 py-2.5 text-right font-mono">{fmt(row.BRENT)}</td>
                    <td className="px-4 py-2.5 text-right font-mono">{fmt(row.SC)}</td>
                    <td className="px-4 py-2.5 text-right font-mono">
                      {row.SC_USD != null ? fmt(row.SC_USD) : <span className="text-slate-300">—</span>}
                    </td>
                    <td className="px-4 py-2.5 text-right font-mono text-slate-500">
                      {row.SC_RATE != null ? fmt(row.SC_RATE, 4) : <span className="text-slate-300">—</span>}
                    </td>
                    <td className="px-4 py-2.5 text-right font-mono">
                      {row.MURBAN != null
                        ? <span>{fmt(row.MURBAN)}{row.MURBAN_is_reference && <span className="text-amber-400 ml-0.5 text-[10px]">⚠</span>}</span>
                        : <span className="text-slate-300">—</span>}
                    </td>
                    <td className="px-4 py-2.5 text-right font-mono">
                      {row.DME_OMAN != null
                        ? <span>{fmt(row.DME_OMAN)}{row.DME_OMAN_is_reference && <span className="text-amber-400 ml-0.5 text-[10px]">⚠</span>}</span>
                        : <span className="text-slate-300">—</span>}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      <p className="text-xs text-slate-400">
        ⚠ 标记表示参考价（来源：oilprice.com，非官方）。SC（USD）列使用 yfinance USDCNY=X 汇率换算。
      </p>
    </div>
  )
}
