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
  fetchMarketSyncStatus,
  triggerMarketSync,
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

// ── Helpers ──────────────────────────────────────────────────────────────────

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
          setSelectedCode(idxItems[0].ts_code)
        }
        setLoading(false)
      })
      .catch(err => {
        if (err.name !== 'AbortError') setLoading(false)
      })

    return () => controller.abort()
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  // ── Load daily chart for selected index ────────────────────────────────────
  useEffect(() => {
    if (!selectedCode) return
    const controller = new AbortController()
    setChartLoading(true)
    fetchIndexDaily(selectedCode, { limit: 250 }, controller.signal)
      .then(data => {
        setIndexDaily(data.items || [])
        setChartLoading(false)
      })
      .catch(err => {
        if (err.name !== 'AbortError') setChartLoading(false)
      })
    return () => controller.abort()
  }, [selectedCode])

  // ── Chart data ──────────────────────────────────────────────────────────────
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
  const selectedInfo = indices.find(i => i.ts_code === selectedCode)

  // ── Render ─────────────────────────────────────────────────────────────────
  return (
    <div className="min-h-screen bg-gray-50">
      {/* Header */}
      <header className="bg-white border-b border-gray-200 sticky top-0 z-10">
        <div className="max-w-6xl mx-auto px-4 py-4 flex flex-wrap items-center gap-4">
          <h1 className="text-xl font-bold text-gray-900 flex items-center gap-2">
            <span className="inline-flex items-center justify-center w-8 h-8 bg-indigo-600 text-white text-sm font-bold rounded">行</span>
            A股行情看板
          </h1>
          <a
            href="/"
            className="text-sm text-gray-500 hover:text-gray-700"
          >
            ← 基金净值
          </a>
          <div className="ml-auto flex items-center gap-3">
            {marketStatus && (
              <span className="text-xs text-gray-400">
                行情同步: {marketStatus.market_last_status || '未同步'}
                {marketStatus.market_index_last_date && (
                  <span className="ml-1">({dbDateToDisplay(marketStatus.market_index_last_date)})</span>
                )}
                {marketStatus.market_last_status === 'error' && (
                  <span className="ml-1 text-red-500">⚠ 失败</span>
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
              请先配置 TUSHARE_TOKEN 并点击「立即同步」拉取数据
            </p>
          </div>
        )}

        {/* Index cards */}
        {(loading || indices.length > 0) && (
          <div className="grid grid-cols-3 sm:grid-cols-4 lg:grid-cols-5 gap-3">
            {loading
              ? Array.from({ length: 9 }, (_, i) => (
                  <div key={i} className="bg-white rounded-xl shadow p-3">
                    <div className="shimmer rounded h-3 w-20 mb-2" />
                    <div className="shimmer rounded h-6 w-24 mb-1" />
                    <div className="shimmer rounded h-3 w-16" />
                  </div>
                ))
              : indices.map(idx => {
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
                      <p className="text-xs text-gray-500 mb-1 truncate">{idx.name}</p>
                      <p className="text-base font-bold text-gray-900 tabular-nums">
                        {idx.close != null ? idx.close.toFixed(2) : '—'}
                      </p>
                      <PctBadge value={idx.pct_chg} />
                    </button>
                  )
                })}
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
                近 {indexDaily.length} 个交易日
              </p>
            )}
          </div>
        )}

        {/* Futures table */}
        {(loading || futures.length > 0) && (
          <div className="bg-white rounded-xl shadow overflow-hidden">
            <div className="px-4 py-3 border-b border-gray-100">
              <h2 className="text-sm font-semibold text-gray-700">金融期货（中金所主力合约）</h2>
            </div>
            <table className="w-full text-sm">
              <thead>
                <tr className="bg-gray-50 border-b border-gray-200 text-left text-gray-600 font-medium text-xs">
                  <th className="px-4 py-3">合约代码</th>
                  <th className="px-4 py-3">品种</th>
                  <th className="px-4 py-3">交易日</th>
                  <th className="px-4 py-3 text-right">收盘</th>
                  <th className="px-4 py-3 text-right">涨跌幅</th>
                  <th className="px-4 py-3 text-right">成交量</th>
                  <th className="px-4 py-3 text-right">持仓量</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-100">
                {loading
                  ? Array.from({ length: 7 }, (_, i) => (
                      <tr key={i}>
                        {Array.from({ length: 7 }, (__, j) => (
                          <td key={j} className="px-4 py-3">
                            <div className="shimmer rounded h-4 w-16" />
                          </td>
                        ))}
                      </tr>
                    ))
                  : futures.map(fut => {
                      const pctChg = fut.close && fut.open
                        ? (fut.close - fut.open) / fut.open * 100
                        : null
                      return (
                        <tr key={fut.ts_code} className="hover:bg-gray-50">
                          <td className="px-4 py-3">
                            <code className="text-xs font-mono text-gray-600">{fut.ts_code}</code>
                          </td>
                          <td className="px-4 py-3 text-gray-700 font-medium">{fut.symbol}</td>
                          <td className="px-4 py-3 text-gray-500 text-xs">
                            {dbDateToDisplay(fut.trade_date)}
                          </td>
                          <td className="px-4 py-3 text-right font-mono text-gray-800">
                            {fut.close?.toFixed(2) ?? '—'}
                          </td>
                          <td className="px-4 py-3 text-right">
                            <PctBadge value={pctChg} />
                          </td>
                          <td className="px-4 py-3 text-right text-gray-500 text-xs">
                            {fut.vol != null ? Math.round(fut.vol).toLocaleString() : '—'}
                          </td>
                          <td className="px-4 py-3 text-right text-gray-500 text-xs">
                            {fut.oi != null ? Math.round(fut.oi).toLocaleString() : '—'}
                          </td>
                        </tr>
                      )
                    })}
                {!loading && futures.length === 0 && (
                  <tr>
                    <td colSpan={7} className="px-4 py-8 text-center text-gray-400 text-sm">
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
