import { useState, useEffect, useMemo } from 'react'
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend,
} from 'chart.js'
import { Line } from 'react-chartjs-2'
import { fetchQuarterlyBasis, fetchBasisToday } from '../api.js'
import RangeScrubber from '../components/RangeScrubber.jsx'

ChartJS.register(CategoryScale, LinearScale, PointElement, LineElement, Title, Tooltip, Legend)

// ── Constants ────────────────────────────────────────────────────────────────

const SYMBOLS = [
  { symbol: 'IF', name: '沪深300', index: '000300.SH' },
  { symbol: 'IC', name: '中证500', index: '000905.SH' },
  { symbol: 'IH', name: '上证50',  index: '000016.SH' },
  { symbol: 'IM', name: '中证1000', index: '000852.SH' },
]

const RANGE_OPTIONS = [
  { label: '近1月', days: 30 },
  { label: '近3月', days: 90 },
  { label: '近6月', days: 180 },
  { label: '近1年', days: 365 },
  { label: '全部', days: 0 },
]

function daysAgoYYYYMMDD(days) {
  const d = new Date()
  d.setDate(d.getDate() - days)
  return d.toISOString().slice(0, 10).replace(/-/g, '')
}

function dateToYYYYMMDD(dateStr) {
  return dateStr.replace(/-/g, '')
}

function yyyymmddToDate(s) {
  if (!s || s.length !== 8) return ''
  return `${s.slice(0, 4)}-${s.slice(4, 6)}-${s.slice(6, 8)}`
}

// ── Helpers ───────────────────────────────────────────────────────────────────

function dbDateToDisplay(d) {
  if (!d || d.length !== 8) return d
  return `${d.slice(0, 4)}-${d.slice(4, 6)}-${d.slice(6, 8)}`
}

function calcColor(val) {
  if (val == null) return 'text-gray-400'
  return val > 0 ? 'text-red-500' : 'text-emerald-600'
}

function formatPct(val, decimals = 2) {
  if (val == null) return '—'
  const sign = val > 0 ? '+' : ''
  return `${sign}${val.toFixed(decimals)}%`
}

const CONTRACT_COLORS = {
  '当季': 'border-blue-400',
  '下季': 'border-orange-400',
  '隔季': 'border-purple-400',
}

// ── Main component ────────────────────────────────────────────────────────────

export default function BasisAnalysis() {
  const [error, setError] = useState('')
  const [revision, setRevision] = useState(0)
  const [activeSymbol, setActiveSymbol] = useState('IF')
  const [basisItems, setBasisItems] = useState([])
  const [loading, setLoading] = useState(false)
  const [todayData, setTodayData] = useState(null)
  const [todayLoading, setTodayLoading] = useState(false)
  const [rangeIdx, setRangeIdx] = useState(3) // default 近1年
  const [customFrom, setCustomFrom] = useState('')
  const [customTo, setCustomTo] = useState('')

  // Scrubber state: indices into allDates for the visible window
  const [scrubStart, setScrubStart] = useState(0)
  const [scrubEnd, setScrubEnd] = useState(0)

  const symbolMeta = SYMBOLS.find(s => s.symbol === activeSymbol)

  // Compute date_from / date_to based on range selection
  const dateParams = useMemo(() => {
    if (customFrom || customTo) {
      return {
        date_from: customFrom ? dateToYYYYMMDD(customFrom) : undefined,
        date_to: customTo ? dateToYYYYMMDD(customTo) : undefined,
      }
    }
    const opt = RANGE_OPTIONS[rangeIdx]
    if (!opt || opt.days === 0) return {} // 全部
    return { date_from: daysAgoYYYYMMDD(opt.days) }
  }, [rangeIdx, customFrom, customTo])

  // Fetch today's contract snapshot
  useEffect(() => {
    const controller = new AbortController()
    setTodayLoading(true)
    setError('')
    setTodayData(null)
    fetchBasisToday(activeSymbol, controller.signal)
      .then(data => {
        setTodayData(data)
        setTodayLoading(false)
      })
      .catch(err => {
        if (err.name !== 'AbortError') { setTodayLoading(false); setError('基差快照读取失败：' + err.message) }
      })
    return () => controller.abort()
  }, [activeSymbol, revision])

  // Fetch quarterly basis history when symbol or date range changes
  useEffect(() => {
    const controller = new AbortController()
    setLoading(true)
    setBasisItems([])
    fetchQuarterlyBasis(activeSymbol, dateParams, controller.signal)
      .then(data => {
        setBasisItems(data.items || [])
        setLoading(false)
      })
      .catch(err => {
        if (err.name !== 'AbortError') { setLoading(false); setError('基差历史读取失败：' + err.message) }
      })
    return () => controller.abort()
  }, [activeSymbol, dateParams, revision])

  // Split into 当季 / 下季 series
  const { currentSeries, nextSeries } = useMemo(() => {
    const current = basisItems.filter(i => i.contract_type === '当季')
    const next    = basisItems.filter(i => i.contract_type === '下季')
    return { currentSeries: current, nextSeries: next }
  }, [basisItems])

  // Build union of dates for chart x-axis
  const allDates = useMemo(() => {
    const set = new Set(basisItems.map(i => i.trade_date))
    return Array.from(set).sort()
  }, [basisItems])

  // Reset scrubber to full range when data reloads
  useEffect(() => {
    setScrubStart(0)
    setScrubEnd(Math.max(0, allDates.length - 1))
  }, [allDates])

  // Scrubber-visible slice of allDates
  const visibleDates = useMemo(() => allDates.slice(scrubStart, scrubEnd + 1), [allDates, scrubStart, scrubEnd])

  // Chart datasets: annualized_basis_pct for 当季 and 下季
  const chartData = useMemo(() => {
    const makeMap = series => new Map(series.map(i => [i.trade_date, i.annualized_basis_pct]))
    const curMap = makeMap(currentSeries)
    const nxtMap = makeMap(nextSeries)
    return {
      labels: visibleDates.map(dbDateToDisplay),
      datasets: [
        {
          label: '当季年化基差%',
          data: visibleDates.map(d => curMap.get(d) ?? null),
          borderColor: '#3b82f6',
          backgroundColor: 'transparent',
          fill: false,
          tension: 0.3,
          pointRadius: 0,
          pointHoverRadius: 4,
          borderWidth: 1.5,
          spanGaps: true,
        },
        {
          label: '下季年化基差%',
          data: visibleDates.map(d => nxtMap.get(d) ?? null),
          borderColor: '#f97316',
          backgroundColor: 'transparent',
          fill: false,
          tension: 0.3,
          pointRadius: 0,
          pointHoverRadius: 4,
          borderWidth: 1.5,
          spanGaps: true,
        },
        {
          label: '零线',
          data: visibleDates.map(() => 0),
          borderColor: '#d1d5db',
          backgroundColor: 'transparent',
          fill: false,
          tension: 0,
          pointRadius: 0,
          borderWidth: 1,
          borderDash: [4, 4],
        },
      ],
    }
  }, [visibleDates, currentSeries, nextSeries])

  const chartOptions = useMemo(() => ({
    responsive: true,
    maintainAspectRatio: false,
    animation: { duration: 0 },
    interaction: { mode: 'index', intersect: false },
    plugins: {
      legend: {
        display: true,
        position: 'top',
        labels: {
          boxWidth: 12,
          font: { size: 11 },
          filter: item => item.text !== '零线',
        },
      },
      tooltip: {
        callbacks: {
          title: items => items[0]?.label || '',
          label: item => {
            if (item.datasetIndex === 2) return null
            const val = item.raw
            if (val == null) return null
            const sign = val > 0 ? '+' : ''
            const label = item.datasetIndex === 0 ? '当季' : '下季'
            return `${label}: ${sign}${val.toFixed(4)}%`
          },
          filter: item => item.datasetIndex !== 2,
        },
      },
    },
    scales: {
      x: {
        grid: { display: false },
        ticks: { maxTicksLimit: 10, maxRotation: 0, font: { size: 11 }, color: '#9ca3af' },
      },
      y: {
        position: 'right',
        grid: { color: '#f3f4f6' },
        ticks: {
          callback: v => `${Number(v).toFixed(1)}%`,
          font: { size: 11 },
          color: '#9ca3af',
        },
      },
    },
  }), [])

  // ── Render ─────────────────────────────────────────────────────────────────
  return (
    <div className="min-h-screen bg-gray-50 basis-page">
      {/* Header */}
      <header className="bg-white border-b border-gray-200 sticky top-14 lg:top-0 z-10">
        <div className="max-w-6xl mx-auto px-4 py-4 flex flex-wrap items-center gap-4">
          <h1 className="text-xl font-bold text-gray-900 flex items-center gap-2">
            <span className="inline-flex items-center justify-center w-8 h-8 bg-teal-600 text-white text-sm font-bold rounded">差</span>
            股指基差分析
          </h1>
          <p className="text-xs text-gray-400 ml-2">
            基差 = 现货 − 期货 &nbsp;·&nbsp; 年化基差% = 基差 / 期货价 / 剩余天数 × 365 × 100
          </p>
        </div>
      </header>

      <main className="max-w-6xl mx-auto px-4 py-6 space-y-6"><div className="page-heading"><div><div className="eyebrow">BASIS / 期现研究</div><h2 className="text-2xl font-bold">观察期现结构与期限差异</h2><p>对照现货指数、交割期限与历史区间，理解年化基差的变化。</p></div></div>

        {/* Symbol tabs */}
        <div className="flex gap-2 flex-wrap">
          {SYMBOLS.map(s => (
            <button
              key={s.symbol}
              onClick={() => setActiveSymbol(s.symbol)}
              className={`px-3 py-1.5 md:px-4 md:py-2 rounded-lg text-sm font-medium transition-colors ${
                activeSymbol === s.symbol
                  ? 'bg-teal-600 text-white shadow'
                  : 'bg-white text-gray-600 border border-gray-200 hover:border-teal-400 hover:text-teal-600'
              }`}
            >
              {s.symbol} · {s.name}
            </button>
          ))}
        </div>

        {error && <div className="notice notice-error" role="alert">{error} <button className="button-secondary" onClick={() => setRevision(v => v + 1)}>重试</button></div>}
        {/* Date range controls */}
        <div className="flex flex-wrap items-center gap-2">
          {RANGE_OPTIONS.map((opt, idx) => (
            <button
              key={opt.label}
              onClick={() => { setRangeIdx(idx); setCustomFrom(''); setCustomTo('') }}
              className={`px-3 py-1.5 rounded text-xs font-medium transition-colors ${
                rangeIdx === idx && !customFrom && !customTo
                  ? 'bg-teal-600 text-white'
                  : 'bg-white text-gray-500 border border-gray-200 hover:border-teal-400'
              }`}
            >
              {opt.label}
            </button>
          ))}
          <span className="text-xs text-gray-400 ml-2">自定义:</span>
          <input
            type="date"
            value={customFrom}
            onChange={e => { setCustomFrom(e.target.value); setRangeIdx(-1) }}
            className="px-2 py-1 border border-gray-200 rounded text-xs text-gray-600 focus:border-teal-400 focus:outline-none"
          />
          <span className="text-xs text-gray-400">~</span>
          <input
            type="date"
            value={customTo}
            onChange={e => { setCustomTo(e.target.value); setRangeIdx(-1) }}
            className="px-2 py-1 border border-gray-200 rounded text-xs text-gray-600 focus:border-teal-400 focus:outline-none"
          />
          {(customFrom || customTo) && (
            <button
              onClick={() => { setCustomFrom(''); setCustomTo(''); setRangeIdx(3) }}
              className="text-xs text-gray-400 hover:text-gray-600"
            >
              清除
            </button>
          )}
        </div>

        {/* Today's contracts table */}
        <div className="bg-white rounded-xl shadow overflow-hidden">
          <div className="px-4 py-3 border-b border-gray-100 flex items-center justify-between">
            <h2 className="text-sm font-semibold text-gray-700">今日合约快照</h2>
            {todayData?.trade_date && (
              <span className="text-xs text-gray-400">
                数据日期：{dbDateToDisplay(todayData.trade_date)}
              </span>
            )}
          </div>

          {todayLoading ? (
            <div className="p-4">
              <div className="shimmer rounded h-20" />
            </div>
          ) : todayData?.items?.length > 0 ? (
            <div className="overflow-x-auto">
              <table className="w-full text-xs">
                <thead>
                  <tr className="bg-gray-50 border-b border-gray-200 text-left text-gray-500 font-medium">
                    <th className="px-2 py-2 md:px-4">合約</th>
                    <th className="px-2 py-2 md:px-4">类型</th>
                    <th className="px-2 py-2 md:px-4">到期日</th>
                    <th className="px-2 py-2 md:px-4 text-right">剩余天数</th>
                    <th className="px-2 py-2 md:px-4 text-right">现货</th>
                    <th className="px-2 py-2 md:px-4 text-right">期货</th>
                    <th className="px-2 py-2 md:px-4 text-right">基差</th>
                    <th className="px-2 py-2 md:px-4 text-right">年化基差%</th>
                    <th className="px-2 py-2 md:px-4 text-center">方向</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-100">
                  {todayData.items.map(item => (
                    <tr key={item.ts_code} className="hover:bg-gray-50">
                      <td className="px-2 py-2 md:px-4 md:py-2.5">
                        <code className={`font-mono font-semibold text-gray-700 border-l-2 pl-2 ${CONTRACT_COLORS[item.contract_type] || 'border-gray-300'}`}>
                          {item.ts_code}
                        </code>
                      </td>
                      <td className="px-2 py-2 md:px-4 md:py-2.5">
                        <span className={`text-xs font-medium ${
                          item.contract_type === '当季' ? 'text-blue-600' :
                          item.contract_type === '下季' ? 'text-orange-500' :
                          'text-purple-500'
                        }`}>
                          {item.contract_type}
                        </span>
                      </td>
                      <td className="px-2 py-2 md:px-4 md:py-2.5 text-gray-500">
                        {dbDateToDisplay(item.expiry_date)}
                      </td>
                      <td className="px-2 py-2 md:px-4 md:py-2.5 text-right text-gray-500">
                        {item.remaining_days}
                      </td>
                      <td className="px-2 py-2 md:px-4 md:py-2.5 text-right font-mono text-gray-700">
                        {item.index_close?.toFixed(2) ?? '—'}
                      </td>
                      <td className="px-2 py-2 md:px-4 md:py-2.5 text-right font-mono text-gray-700">
                        {item.futures_close?.toFixed(2) ?? '—'}
                      </td>
                      <td className={`px-2 py-2 md:px-4 md:py-2.5 text-right font-mono font-medium ${calcColor(item.basis)}`}>
                        {item.basis != null ? (item.basis > 0 ? '+' : '') + item.basis.toFixed(2) : '—'}
                      </td>
                      <td className={`px-2 py-2 md:px-4 md:py-2.5 text-right font-mono font-bold ${calcColor(item.annualized_basis_pct)}`}>
                        {formatPct(item.annualized_basis_pct, 2)}
                      </td>
                      <td className="px-2 py-2 md:px-4 md:py-2.5 text-center">
                        {item.annualized_basis_pct != null ? (
                          <span className={`inline-block px-1.5 py-0.5 rounded text-xs font-medium ${
                            item.annualized_basis_pct > 0
                              ? 'bg-red-50 text-red-600'
                              : 'bg-emerald-50 text-emerald-700'
                          }`}>
                            {item.annualized_basis_pct > 0 ? '贴水' : '升水'}
                          </span>
                        ) : '—'}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : (
            <div className="px-4 py-8 text-center text-gray-400 text-sm">
              暂无数据（请先同步行情）
            </div>
          )}
        </div>

        {/* Annualized basis chart */}
        <div className="bg-white rounded-xl shadow p-4">
          <div className="flex items-center justify-between mb-3">
            <div>
              <h2 className="text-sm font-semibold text-gray-700">
                年化基差走势
              </h2>
              <p className="text-xs text-gray-400 mt-0.5">
                {symbolMeta?.symbol} · {symbolMeta?.name} vs {symbolMeta?.index}
                &nbsp;·&nbsp; 正值 = 贴水（现货 &gt; 期货），负值 = 升水（期货 &gt; 现货）
              </p>
            </div>
            {allDates.length > 0 && (
              <span className="text-xs text-gray-400">
                {dbDateToDisplay(allDates[0])} – {dbDateToDisplay(allDates[allDates.length - 1])}
              </span>
            )}
          </div>

          {loading ? (
            <div className="shimmer rounded-lg h-72" />
          ) : allDates.length === 0 ? (
            <div className="h-72 flex items-center justify-center text-gray-400 text-sm">
              暂无数据（请先同步行情，数据从 2025-01-01 起）
            </div>
          ) : (
            <div className="h-72">
              <Line data={chartData} options={chartOptions} />
            </div>
          )}

          {/* Range scrubber */}
          {!loading && allDates.length > 1 && (
            <RangeScrubber
              dates={allDates}
              startIdx={scrubStart}
              endIdx={scrubEnd}
              onChange={(s, e) => { setScrubStart(s); setScrubEnd(e) }}
            />
          )}
        </div>

        {/* Data table: latest 30 trading days */}
        {!loading && basisItems.length > 0 && (() => {
          const lastDates = allDates.slice(-30)
          const tableRows = []
          for (const d of [...lastDates].reverse()) {
            const cur = basisItems.find(x => x.trade_date === d && x.contract_type === '当季')
            const nxt = basisItems.find(x => x.trade_date === d && x.contract_type === '下季')
            if (cur || nxt) tableRows.push({ date: d, current: cur, next: nxt })
          }
          return (
            <div className="bg-white rounded-xl shadow overflow-hidden">
              <div className="px-4 py-3 border-b border-gray-100">
                <h2 className="text-sm font-semibold text-gray-700">近期数据（最近 30 个交易日）</h2>
              </div>
              <div className="overflow-x-auto">
                <table className="w-full text-xs">
                  <thead>
                    <tr className="bg-gray-50 border-b border-gray-200 text-left text-gray-500 font-medium">
                      <th className="px-2 py-1.5 md:px-4 md:py-2">日期</th>
                      <th className="px-2 py-1.5 md:px-4 md:py-2">合约</th>
                      <th className="px-2 py-1.5 md:px-4 md:py-2 text-right">现货</th>
                      <th className="px-2 py-1.5 md:px-4 md:py-2 text-right">期货</th>
                      <th className="px-2 py-1.5 md:px-4 md:py-2 text-right">基差</th>
                      <th className="px-2 py-1.5 md:px-4 md:py-2 text-right">年化基差%</th>
                      <th className="px-2 py-1.5 md:px-4 md:py-2 text-right">剩余天数</th>
                      <th className="px-2 py-1.5 md:px-4 md:py-2">合约</th>
                      <th className="px-2 py-1.5 md:px-4 md:py-2 text-right">现货</th>
                      <th className="px-2 py-1.5 md:px-4 md:py-2 text-right">期货</th>
                      <th className="px-2 py-1.5 md:px-4 md:py-2 text-right">基差</th>
                      <th className="px-2 py-1.5 md:px-4 md:py-2 text-right">年化基差%</th>
                      <th className="px-2 py-1.5 md:px-4 md:py-2 text-right">剩余天数</th>
                    </tr>
                    <tr className="bg-gray-50 border-b border-gray-100 text-gray-400 text-xs">
                      <th className="px-4 py-1" />
                      <th className="px-4 py-1 text-blue-500">当季</th>
                      <th colSpan={5} />
                      <th className="px-4 py-1 text-orange-500">下季</th>
                      <th colSpan={5} />
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-gray-100">
                    {tableRows.map(({ date, current: c, next: n }) => (
                      <tr key={date} className="hover:bg-gray-50">
                        <td className="px-2 py-1.5 md:px-4 md:py-2 text-gray-500 whitespace-nowrap">
                          {dbDateToDisplay(date)}
                        </td>
                        {/* 当季 */}
                        <td className="px-2 py-1.5 md:px-4 md:py-2">
                          <code className="font-mono text-gray-600">{c?.ts_code ?? '—'}</code>
                        </td>
                        <td className="px-2 py-1.5 md:px-4 md:py-2 text-right font-mono text-gray-700">
                          {c?.index_close?.toFixed(2) ?? '—'}
                        </td>
                        <td className="px-2 py-1.5 md:px-4 md:py-2 text-right font-mono text-gray-700">
                          {c?.futures_close?.toFixed(2) ?? '—'}
                        </td>
                        <td className={`px-2 py-1.5 md:px-4 md:py-2 text-right font-mono font-medium ${calcColor(c?.basis)}`}>
                          {c?.basis != null ? (c.basis > 0 ? '+' : '') + c.basis.toFixed(2) : '—'}
                        </td>
                        <td className={`px-2 py-1.5 md:px-4 md:py-2 text-right font-mono font-medium ${calcColor(c?.annualized_basis_pct)}`}>
                          {formatPct(c?.annualized_basis_pct)}
                        </td>
                        <td className="px-2 py-1.5 md:px-4 md:py-2 text-right text-gray-500">
                          {c?.remaining_days ?? '—'}
                        </td>
                        {/* 下季 */}
                        <td className="px-2 py-1.5 md:px-4 md:py-2">
                          <code className="font-mono text-gray-600">{n?.ts_code ?? '—'}</code>
                        </td>
                        <td className="px-2 py-1.5 md:px-4 md:py-2 text-right font-mono text-gray-700">
                          {n?.index_close?.toFixed(2) ?? '—'}
                        </td>
                        <td className="px-2 py-1.5 md:px-4 md:py-2 text-right font-mono text-gray-700">
                          {n?.futures_close?.toFixed(2) ?? '—'}
                        </td>
                        <td className={`px-2 py-1.5 md:px-4 md:py-2 text-right font-mono font-medium ${calcColor(n?.basis)}`}>
                          {n?.basis != null ? (n.basis > 0 ? '+' : '') + n.basis.toFixed(2) : '—'}
                        </td>
                        <td className={`px-2 py-1.5 md:px-4 md:py-2 text-right font-mono font-medium ${calcColor(n?.annualized_basis_pct)}`}>
                          {formatPct(n?.annualized_basis_pct)}
                        </td>
                        <td className="px-2 py-1.5 md:px-4 md:py-2 text-right text-gray-500">
                          {n?.remaining_days ?? '—'}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )
        })()}

      </main>
    </div>
  )
}
