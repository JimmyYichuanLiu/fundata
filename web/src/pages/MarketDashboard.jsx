import { useState, useEffect, useRef, useMemo, useCallback } from 'react'
import { useNavigate } from 'react-router-dom'
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend,
  Filler,
} from 'chart.js'
import { Line } from 'react-chartjs-2'
import {
  fetchMarketIndices,
  fetchMarketFutures,
  fetchIndexDaily,
  fetchBasisDaily,
  fetchMarketSyncStatus,
  triggerMarketSync,
  fetchRealtimeIndices,
  fetchRealtimeFutures,
  triggerRealtimeSync,
} from '../api.js'

ChartJS.register(
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend,
  Filler,
)

// Indices that have corresponding stock-index futures
const INDEX_TO_FUTURES = {
  '000300.SH': 'IF',
  '000905.SH': 'IC',
  '000016.SH': 'IH',
  '000852.SH': 'IM',
}

const RANGE_OPTIONS = [
  { label: '近1月', days: 30 },
  { label: '近3月', days: 90 },
  { label: '近6月', days: 180 },
  { label: '近1年', days: 365 },
  { label: '全部', days: 0 },
]

// ── Helpers ──────────────────────────────────────────────────────────────────

function daysAgoYYYYMMDD(days) {
  const d = new Date()
  d.setDate(d.getDate() - days)
  return d.toISOString().slice(0, 10).replace(/-/g, '')
}

function dateToYYYYMMDD(dateStr) {
  return dateStr.replace(/-/g, '')
}

function dbDateToDisplay(d) {
  if (!d || d.length !== 8) return d
  return `${d.slice(0, 4)}-${d.slice(4, 6)}-${d.slice(6, 8)}`
}

function PctBadge({ value }) {
  if (value == null) return <span className="text-gray-400 text-sm">—</span>
  const color = value > 0 ? 'text-red-500' : value < 0 ? 'text-emerald-600' : 'text-gray-500'
  const sign = value > 0 ? '+' : ''
  return <span className={`font-semibold ${color}`}>{sign}{value.toFixed(2)}%</span>
}

function createGradient(ctx, chartArea) {
  const gradient = ctx.createLinearGradient(0, chartArea.top, 0, chartArea.bottom)
  gradient.addColorStop(0, 'rgba(59,130,246,0.3)')
  gradient.addColorStop(1, 'rgba(59,130,246,0.01)')
  return gradient
}

// ── Main component ────────────────────────────────────────────────────────────

export default function MarketDashboard() {
  const navigate = useNavigate()
  const chartRef = useRef(null)

  const [indices, setIndices] = useState([])
  const [futures, setFutures] = useState([])
  const [selectedCode, setSelectedCode] = useState(null)
  const [indexDaily, setIndexDaily] = useState([])
  const [marketStatus, setMarketStatus] = useState(null)
  const [syncing, setSyncing] = useState(false)
  const [loading, setLoading] = useState(true)
  const [chartLoading, setChartLoading] = useState(false)
  const [gradient, setGradient] = useState(null)
  const [syncError, setSyncError] = useState('')

  // Basis chart state
  const [basisData, setBasisData] = useState([])
  const [basisLoading, setBasisLoading] = useState(false)

  // Date range state for charts
  const [rangeIdx, setRangeIdx] = useState(3) // default 近1年
  const [customFrom, setCustomFrom] = useState('')
  const [customTo, setCustomTo] = useState('')

  // Real-time state
  const [realtimeIndices, setRealtimeIndices] = useState([])
  const [realtimeFutures, setRealtimeFutures] = useState([])
  const [realtimeUpdatedAt, setRealtimeUpdatedAt] = useState(null)
  const [autoRefresh, setAutoRefresh] = useState(true)
  const REFRESH_INTERVAL = 5 * 60 * 1000 // 5 minutes

  // Compute fetch params from range selection
  const chartDateParams = useMemo(() => {
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

  // ── Load overview data ──────────────────────────────────────────────────────
  useEffect(() => {
    const controller = new AbortController()
    const { signal } = controller

    setLoading(true)
    Promise.all([
      fetchMarketIndices(signal),
      fetchMarketFutures(signal),
      fetchMarketSyncStatus(signal),
    ])
      .then(([idxData, futData, status]) => {
        const idxItems = idxData.items || []
        setIndices(idxItems)
        setFutures(futData.items || [])
        setMarketStatus(status)
        if (idxItems.length > 0 && !selectedCode) {
          // Prefer an index that has a futures mapping so the basis chart is visible by default
          const withFutures = idxItems.find(i => INDEX_TO_FUTURES[i.ts_code])
          setSelectedCode((withFutures || idxItems[0]).ts_code)
        }
        setLoading(false)
      })
      .catch(err => {
        if (err.name !== 'AbortError') setLoading(false)
      })

    return () => controller.abort()
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  // ── Load real-time snapshots + auto refresh ───────────────────────────────
  const fetchRealtime = useCallback((signal) => {
    Promise.all([
      fetchRealtimeIndices(signal),
      fetchRealtimeFutures(signal),
    ])
      .then(([idxData, futData]) => {
        if (idxData.items?.length > 0) setRealtimeIndices(idxData.items)
        if (futData.items?.length > 0) setRealtimeFutures(futData.items)
        setRealtimeUpdatedAt(idxData.updated_at || futData.updated_at || null)
      })
      .catch(() => {})
  }, [])

  useEffect(() => {
    const controller = new AbortController()
    fetchRealtime(controller.signal)
    if (!autoRefresh) return () => controller.abort()

    const timer = setInterval(() => fetchRealtime(controller.signal), REFRESH_INTERVAL)
    return () => { clearInterval(timer); controller.abort() }
  }, [autoRefresh, fetchRealtime, REFRESH_INTERVAL])

  // Merge real-time prices into indices for display
  const displayIndices = useMemo(() => {
    if (realtimeIndices.length === 0) return indices
    const rtMap = new Map(realtimeIndices.map(r => [r.ts_code, r]))
    return indices.map(idx => {
      const rt = rtMap.get(idx.ts_code)
      if (!rt || rt.price == null) return idx
      return {
        ...idx,
        close: rt.price,
        open: rt.open ?? idx.open,
        high: rt.high ?? idx.high,
        low: rt.low ?? idx.low,
        pct_chg: rt.pct_chg ?? idx.pct_chg,
        _realtime: true,
      }
    })
  }, [indices, realtimeIndices])

  // ── Load daily chart for selected index ────────────────────────────────────
  useEffect(() => {
    if (!selectedCode) return
    const controller = new AbortController()
    setChartLoading(true)
    fetchIndexDaily(selectedCode, chartDateParams, controller.signal)
      .then(data => {
        setIndexDaily(data.items || [])
        setChartLoading(false)
      })
      .catch(err => {
        if (err.name !== 'AbortError') setChartLoading(false)
      })
    return () => controller.abort()
  }, [selectedCode, chartDateParams])

  // ── Load basis data when selected index has a futures mapping ──────────────
  useEffect(() => {
    if (!selectedCode) return
    const futuresSymbol = INDEX_TO_FUTURES[selectedCode]
    if (!futuresSymbol) {
      setBasisData([])
      return
    }
    const controller = new AbortController()
    setBasisLoading(true)
    fetchBasisDaily(futuresSymbol, chartDateParams, controller.signal)
      .then(data => {
        setBasisData(data.items || [])
        setBasisLoading(false)
      })
      .catch(err => {
        if (err.name !== 'AbortError') {
          setBasisData([])
          setBasisLoading(false)
        }
      })
    return () => controller.abort()
  }, [selectedCode, chartDateParams])

  // ── Index chart data ────────────────────────────────────────────────────────
  const chartData = useMemo(() => ({
    labels: indexDaily.map(i => dbDateToDisplay(i.trade_date)),
    datasets: [
      {
        label: '收盘价',
        data: indexDaily.map(i => i.close),
        borderColor: '#3b82f6',
        backgroundColor: gradient || 'rgba(59,130,246,0.15)',
        fill: true,
        tension: 0.3,
        pointRadius: 0,
        pointHoverRadius: 4,
        pointHoverBackgroundColor: '#3b82f6',
        borderWidth: 2,
      },
    ],
  }), [indexDaily, gradient])

  const chartOptions = useMemo(() => ({
    responsive: true,
    maintainAspectRatio: false,
    animation: { duration: 0 },
    interaction: { mode: 'index', intersect: false },
    plugins: {
      legend: { display: false },
      tooltip: {
        callbacks: {
          title: items => items[0]?.label || '',
          label: item => `收盘: ${Number(item.raw).toFixed(2)}`,
        },
      },
    },
    scales: {
      x: {
        grid: { display: false },
        ticks: { maxTicksLimit: 8, maxRotation: 0, font: { size: 11 }, color: '#9ca3af' },
      },
      y: {
        position: 'right',
        grid: { color: '#f3f4f6' },
        ticks: {
          callback: v => Number(v).toFixed(0),
          font: { size: 11 },
          color: '#9ca3af',
        },
      },
    },
    onResize: chart => {
      if (chart.chartArea) setGradient(createGradient(chart.ctx, chart.chartArea))
    },
  }), [])

  // ── Basis chart data ────────────────────────────────────────────────────────
  const basisChartData = useMemo(() => ({
    labels: basisData.map(i => dbDateToDisplay(i.trade_date)),
    datasets: [
      {
        label: '基差',
        data: basisData.map(i => i.basis),
        borderColor: '#3b82f6',
        backgroundColor: 'transparent',
        fill: false,
        tension: 0.3,
        pointRadius: 0,
        pointHoverRadius: 4,
        borderWidth: 1.5,
      },
      {
        label: '零线',
        data: basisData.map(() => 0),
        borderColor: '#d1d5db',
        backgroundColor: 'transparent',
        fill: false,
        tension: 0,
        pointRadius: 0,
        borderWidth: 1,
        borderDash: [4, 4],
      },
    ],
  }), [basisData])

  const basisChartOptions = useMemo(() => ({
    responsive: true,
    maintainAspectRatio: false,
    animation: { duration: 0 },
    interaction: { mode: 'index', intersect: false },
    plugins: {
      legend: { display: false },
      tooltip: {
        callbacks: {
          title: items => items[0]?.label || '',
          label: item => {
            if (item.datasetIndex === 1) return null
            const row = basisData[item.dataIndex]
            if (!row) return `基差: ${Number(item.raw).toFixed(2)}`
            return [
              `基差: ${Number(row.basis).toFixed(2)}`,
              `期货: ${Number(row.futures_close).toFixed(2)}`,
              `现货: ${Number(row.index_close).toFixed(2)}`,
              row.basis_pct != null ? `基差率: ${Number(row.basis_pct).toFixed(4)}%` : '',
            ].filter(Boolean)
          },
          filter: item => item.datasetIndex === 0,
        },
      },
    },
    scales: {
      x: {
        grid: { display: false },
        ticks: { maxTicksLimit: 8, maxRotation: 0, font: { size: 11 }, color: '#9ca3af' },
      },
      y: {
        position: 'right',
        grid: { color: '#f3f4f6' },
        ticks: {
          callback: v => Number(v).toFixed(0),
          font: { size: 11 },
          color: '#9ca3af',
        },
      },
    },
  }), [basisData])

  const handleChartRef = useCallback(ref => {
    chartRef.current = ref
    if (ref?.chartArea) setGradient(createGradient(ref.ctx, ref.chartArea))
  }, [])

  // ── Market sync ─────────────────────────────────────────────────────────────
  const handleMarketSync = useCallback(async () => {
    if (syncing) return
    setSyncing(true)
    setSyncError('')
    try {
      await triggerMarketSync()
    } catch (err) {
      setSyncError(err.message)
    } finally {
      setSyncing(false)
    }
  }, [syncing])

  // ── Selected index info ────────────────────────────────────────────────────
  const selectedInfo = displayIndices.find(i => i.ts_code === selectedCode)
  const selectedFuturesSymbol = selectedCode ? INDEX_TO_FUTURES[selectedCode] : null

  // ── Render ─────────────────────────────────────────────────────────────────
  return (
    <div className="min-h-screen bg-gray-50">
      {/* Header */}
      <header className="bg-white border-b border-gray-200 sticky top-14 lg:top-0 z-10">
        <div className="max-w-6xl mx-auto px-4 py-4 flex flex-wrap items-center gap-4">
          <h1 className="text-xl font-bold text-gray-900 flex items-center gap-2">
            <span className="inline-flex items-center justify-center w-8 h-8 bg-indigo-600 text-white text-sm font-bold rounded">行</span>
            A股行情
          </h1>
          <div className="ml-auto flex items-center gap-3">
            {realtimeUpdatedAt && (
              <span className="text-xs text-emerald-600">
                实时 {realtimeUpdatedAt.slice(11, 16)}
              </span>
            )}
            <button
              onClick={() => setAutoRefresh(v => !v)}
              className={`px-2 py-1 rounded text-xs font-medium transition-colors ${
                autoRefresh
                  ? 'bg-emerald-50 text-emerald-700 border border-emerald-200'
                  : 'bg-gray-50 text-gray-400 border border-gray-200'
              }`}
            >
              {autoRefresh ? '自动刷新 ON' : '自动刷新 OFF'}
            </button>
            {marketStatus && (
              <span className="text-xs text-gray-400">
                日线: {marketStatus.market_last_status || '未同步'}
                {marketStatus.market_index_last_date && (
                  <span className="ml-1">({dbDateToDisplay(marketStatus.market_index_last_date)})</span>
                )}
                {marketStatus.market_last_status === 'error' && (
                  <span className="ml-1 text-red-500">失败</span>
                )}
              </span>
            )}
            {syncError && (
              <span className="text-xs text-red-500">{syncError}</span>
            )}
            <button
              onClick={handleMarketSync}
              disabled={syncing}
              className="px-3 py-1.5 text-sm bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
            >
              {syncing ? '同步中…' : '立即同步'}
            </button>
          </div>
        </div>
      </header>

      <main className="max-w-6xl mx-auto px-4 py-6 space-y-6">

        {/* Empty state */}
        {!loading && indices.length === 0 && (
          <div className="bg-white rounded-xl shadow p-12 text-center">
            <p className="text-gray-400 text-sm mb-2">暂无行情数据</p>
            <p className="text-gray-300 text-xs">
              请点击「立即同步」拉取行情数据
            </p>
          </div>
        )}

        {/* Index cards */}
        {(loading || indices.length > 0) && (
          <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-5 gap-3">
            {loading
              ? Array.from({ length: 9 }, (_, i) => (
                  <div key={i} className="bg-white rounded-xl shadow p-3">
                    <div className="shimmer rounded h-3 w-20 mb-2" />
                    <div className="shimmer rounded h-6 w-24 mb-1" />
                    <div className="shimmer rounded h-3 w-16" />
                  </div>
                ))
              : displayIndices.map(idx => {
                  const isSelected = idx.ts_code === selectedCode
                  return (
                    <button
                      key={idx.ts_code}
                      onClick={() => setSelectedCode(idx.ts_code)}
                      className={`text-left bg-white rounded-xl shadow p-3 transition-all ${
                        isSelected
                          ? 'ring-2 ring-indigo-500 shadow-md'
                          : 'hover:shadow-md hover:ring-1 hover:ring-gray-200'
                      }`}
                    >
                      <div className="flex items-center gap-1 mb-1">
                        <p className="text-xs text-gray-500 truncate">{idx.name}</p>
                        {idx._realtime && (
                          <span className="w-1.5 h-1.5 rounded-full bg-emerald-400 flex-shrink-0" title="实时数据" />
                        )}
                      </div>
                      <p className="text-base font-bold text-gray-900 tabular-nums">
                        {idx.close != null ? idx.close.toFixed(2) : '—'}
                      </p>
                      <PctBadge value={idx.pct_chg} />
                    </button>
                  )
                })}
          </div>
        )}

        {/* Date range controls for charts */}
        {selectedCode && (
          <div className="flex flex-wrap items-center gap-2">
            {RANGE_OPTIONS.map((opt, idx) => (
              <button
                key={opt.label}
                onClick={() => { setRangeIdx(idx); setCustomFrom(''); setCustomTo('') }}
                className={`px-3 py-1.5 rounded text-xs font-medium transition-colors ${
                  rangeIdx === idx && !customFrom && !customTo
                    ? 'bg-indigo-600 text-white'
                    : 'bg-white text-gray-500 border border-gray-200 hover:border-indigo-400'
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
              className="px-2 py-1 border border-gray-200 rounded text-xs text-gray-600 focus:border-indigo-400 focus:outline-none"
            />
            <span className="text-xs text-gray-400">~</span>
            <input
              type="date"
              value={customTo}
              onChange={e => { setCustomTo(e.target.value); setRangeIdx(-1) }}
              className="px-2 py-1 border border-gray-200 rounded text-xs text-gray-600 focus:border-indigo-400 focus:outline-none"
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
        )}

        {/* Index chart */}
        {selectedCode && (
          <div className="bg-white rounded-xl shadow p-4">
            <div className="flex items-center justify-between mb-3">
              <h2 className="text-sm font-semibold text-gray-700">
                {selectedInfo?.name || selectedCode}
                <span className="ml-2 text-xs font-normal text-gray-400">{selectedCode}</span>
              </h2>
              {selectedInfo && (
                <div className="flex items-center gap-3 text-xs text-gray-500">
                  <span>开: {selectedInfo.open?.toFixed(2) ?? '—'}</span>
                  <span>高: {selectedInfo.high?.toFixed(2) ?? '—'}</span>
                  <span>低: {selectedInfo.low?.toFixed(2) ?? '—'}</span>
                </div>
              )}
            </div>
            {chartLoading ? (
              <div className="shimmer rounded-lg h-64" />
            ) : indexDaily.length === 0 ? (
              <div className="h-64 flex items-center justify-center text-gray-400 text-sm">暂无历史数据</div>
            ) : (
              <div className="h-64">
                <Line ref={handleChartRef} data={chartData} options={chartOptions} />
              </div>
            )}
            {!chartLoading && indexDaily.length > 0 && (
              <p className="text-xs text-gray-400 mt-2 text-right">
                共 {indexDaily.length} 个交易日
              </p>
            )}
          </div>
        )}

        {/* Basis chart — only when the selected index has a stock-index futures */}
        {selectedCode && selectedFuturesSymbol && (
          <div className="bg-white rounded-xl shadow p-4">
            <div className="flex items-center justify-between mb-3">
              <div>
                <h2 className="text-sm font-semibold text-gray-700">
                  基差走势（现货 − 期货）
                </h2>
                <p className="text-xs text-gray-400 mt-0.5">
                  {selectedFuturesSymbol} 主力合约 vs {selectedInfo?.name || selectedCode}
                </p>
              </div>
              <div className="flex items-center gap-3">
                {basisData.length > 0 && (
                  <span className="text-xs text-gray-400">共 {basisData.length} 个交易日</span>
                )}
                <a
                  href="/basis"
                  className="text-xs text-teal-600 hover:text-teal-700 font-medium"
                >
                  当季/下季详细分析 →
                </a>
              </div>
            </div>
            {basisLoading ? (
              <div className="shimmer rounded-lg h-48" />
            ) : basisData.length === 0 ? (
              <div className="h-48 flex items-center justify-center text-gray-400 text-sm">
                暂无基差数据（需同步指数与期货数据）
              </div>
            ) : (
              <div className="h-48">
                <Line data={basisChartData} options={basisChartOptions} />
              </div>
            )}
          </div>
        )}

        {/* Futures table — with real-time overlay */}
        {(loading || futures.length > 0 || realtimeFutures.length > 0) && (
          <div className="bg-white rounded-xl shadow overflow-hidden">
            <div className="px-4 py-3 border-b border-gray-100 flex items-center justify-between">
              <h2 className="text-sm font-semibold text-gray-700">金融期货（中金所主力合约）</h2>
              {realtimeUpdatedAt && realtimeFutures.length > 0 && (
                <span className="text-xs text-emerald-500">
                  实时 {realtimeUpdatedAt.slice(11, 16)}
                </span>
              )}
            </div>
            <table className="w-full text-sm">
              <thead>
                <tr className="bg-gray-50 border-b border-gray-200 text-left text-gray-600 font-medium text-xs">
                  <th className="px-2 py-2 md:px-4 md:py-3">合约代码</th>
                  <th className="px-2 py-2 md:px-4 md:py-3">品种</th>
                  <th className="px-2 py-2 md:px-4 md:py-3 text-right">最新价</th>
                  <th className="px-2 py-2 md:px-4 md:py-3 text-right">涨跌幅</th>
                  <th className="px-2 py-2 md:px-4 md:py-3 text-right">成交量</th>
                  <th className="px-2 py-2 md:px-4 md:py-3 text-right">持仓量</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-100">
                {loading
                  ? Array.from({ length: 7 }, (_, i) => (
                      <tr key={i}>
                        {Array.from({ length: 6 }, (__, j) => (
                          <td key={j} className="px-2 py-2 md:px-4 md:py-3">
                            <div className="shimmer rounded h-4 w-16" />
                          </td>
                        ))}
                      </tr>
                    ))
                  : (() => {
                      // Merge: prefer realtime, fallback to daily
                      const rtMap = new Map(realtimeFutures.map(r => [r.ts_code, r]))
                      const dailyMap = new Map(futures.map(f => [f.ts_code, f]))
                      const allCodes = [...new Set([...rtMap.keys(), ...dailyMap.keys()])]
                      return allCodes.map(code => {
                        const rt = rtMap.get(code)
                        const daily = dailyMap.get(code)
                        const price = rt?.price ?? daily?.close
                        const pctChg = rt?.pct_chg ?? (daily?.close && daily?.open
                          ? (daily.close - daily.open) / daily.open * 100
                          : null)
                        const vol = rt?.volume ?? daily?.vol
                        const oi = rt?.hold ?? daily?.oi
                        const symbol = rt?.base_symbol || daily?.symbol || code.replace(/\d+/g, '')
                        const isRt = !!rt
                        return (
                          <tr key={code} className="hover:bg-gray-50">
                            <td className="px-2 py-2 md:px-4 md:py-3">
                              <div className="flex items-center gap-1.5">
                                <code className="text-xs font-mono text-gray-600">{code}</code>
                                {isRt && <span className="w-1.5 h-1.5 rounded-full bg-emerald-400" title="实时" />}
                              </div>
                            </td>
                            <td className="px-2 py-2 md:px-4 md:py-3 text-gray-700 font-medium">{symbol}</td>
                            <td className="px-2 py-2 md:px-4 md:py-3 text-right font-mono text-gray-800">
                              {price?.toFixed(2) ?? '—'}
                            </td>
                            <td className="px-2 py-2 md:px-4 md:py-3 text-right">
                              <PctBadge value={pctChg} />
                            </td>
                            <td className="px-2 py-2 md:px-4 md:py-3 text-right text-gray-500 text-xs">
                              {vol != null ? Math.round(vol).toLocaleString() : '—'}
                            </td>
                            <td className="px-2 py-2 md:px-4 md:py-3 text-right text-gray-500 text-xs">
                              {oi != null ? Math.round(oi).toLocaleString() : '—'}
                            </td>
                          </tr>
                        )
                      })
                    })()}
                {!loading && futures.length === 0 && realtimeFutures.length === 0 && (
                  <tr>
                    <td colSpan={6} className="px-4 py-8 text-center text-gray-400 text-sm">
                      暂无期货数据，请先同步行情
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        )}
      </main>
    </div>
  )
}
