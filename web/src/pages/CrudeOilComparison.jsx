import { useState, useEffect, useRef, useCallback, useMemo } from 'react'
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
import {
  fetchCrudeDaily,
  fetchCrudeSyncStatus,
  triggerCrudeSync,
  daysAgoYYYYMMDD,
  parseCrudeDate,
} from '../api/crudeApi.js'
import RangeScrubber from '../components/RangeScrubber.jsx'

ChartJS.register(CategoryScale, LinearScale, PointElement, LineElement, Title, Tooltip, Legend)

// ---------------------------------------------------------------------------
// 常量
// ---------------------------------------------------------------------------

const RANGE_OPTIONS = [
  { label: '近1月',  days: 30  },
  { label: '近3月',  days: 90  },
  { label: '近6月',  days: 180 },
  { label: '近1年',  days: 365 },
  { label: '近3年',  days: 1095 },
  { label: '全部',   days: 0   },
]

const SYMBOL_COLORS = {
  WTI:   { border: '#2563eb', background: 'rgba(37,99,235,0.08)'  },  // 蓝
  BRENT: { border: '#16a34a', background: 'rgba(22,163,74,0.08)'  },  // 绿
  SC:    { border: '#ea580c', background: 'rgba(234,88,12,0.08)'  },  // 橙
}

const SYMBOL_META = {
  WTI:   { label: 'WTI原油',    unit: 'USD/桶', yAxis: 'y'  },
  BRENT: { label: 'Brent原油',  unit: 'USD/桶', yAxis: 'y'  },
  SC:    { label: '上海原油SC', unit: 'CNY/桶', yAxis: 'y1' },
}

// ---------------------------------------------------------------------------
// 工具函数
// ---------------------------------------------------------------------------

function yyyymmddToDisplay(s) {
  if (!s || s.length !== 8) return s || ''
  return `${s.slice(0, 4)}-${s.slice(4, 6)}-${s.slice(6, 8)}`
}

function statusBadge(status) {
  const map = {
    success:       'bg-emerald-100 text-emerald-700',
    partial_error: 'bg-yellow-100 text-yellow-700',
    error:         'bg-red-100 text-red-700',
    running:       'bg-blue-100 text-blue-700',
    never:         'bg-slate-100 text-slate-500',
  }
  return map[status] || 'bg-slate-100 text-slate-500'
}

// ---------------------------------------------------------------------------
// 主组件
// ---------------------------------------------------------------------------

export default function CrudeOilComparison() {
  const [rangeIdx, setRangeIdx]       = useState(3)            // 默认近1年
  const [customFrom, setCustomFrom]   = useState('')
  const [customTo,   setCustomTo]     = useState('')
  const [useCustom,  setUseCustom]    = useState(false)

  const [items,      setItems]        = useState([])
  const [latestDate, setLatestDate]   = useState(null)
  const [loading,    setLoading]      = useState(false)
  const [error,      setError]        = useState(null)

  // Scrubber indices into items
  const [scrubStart, setScrubStart] = useState(0)
  const [scrubEnd,   setScrubEnd]   = useState(0)

  // Reset scrubber to full range when items change
  useEffect(() => {
    setScrubStart(0)
    setScrubEnd(Math.max(0, items.length - 1))
  }, [items])

  // Visible slice for chart
  const visibleItems = useMemo(() => items.slice(scrubStart, scrubEnd + 1), [items, scrubStart, scrubEnd])

  const [syncStatus, setSyncStatus]   = useState(null)
  const [syncing,    setSyncing]      = useState(false)
  const [syncMsg,    setSyncMsg]      = useState('')

  // ── 计算查询参数 ───────────────────────────────────────────────────────────

  const queryParams = useCallback(() => {
    if (useCustom && customFrom) {
      return {
        date_from: customFrom.replace(/-/g, ''),
        date_to:   customTo ? customTo.replace(/-/g, '') : undefined,
        limit: 2000,
      }
    }
    const days = RANGE_OPTIONS[rangeIdx].days
    return {
      date_from: days > 0 ? daysAgoYYYYMMDD(days) : undefined,
      limit: 2000,
    }
  }, [rangeIdx, useCustom, customFrom, customTo])

  // ── 加载数据 ───────────────────────────────────────────────────────────────

  const loadData = useCallback(async (signal) => {
    setLoading(true)
    setError(null)
    try {
      const params = queryParams()
      const res = await fetchCrudeDaily(params, signal)
      setItems(res.items || [])
      setLatestDate(res.latest_date || null)
    } catch (e) {
      if (e.name !== 'AbortError') setError(e.message)
    } finally {
      setLoading(false)
    }
  }, [queryParams])

  useEffect(() => {
    const ac = new AbortController()
    loadData(ac.signal)
    return () => ac.abort()
  }, [loadData])

  // ── 加载同步状态 ──────────────────────────────────────────────────────────

  const loadSyncStatus = useCallback(async (signal) => {
    try {
      const s = await fetchCrudeSyncStatus(signal)
      setSyncStatus(s)
    } catch (e) {
      if (e.name !== 'AbortError') console.error('sync status error', e)
    }
  }, [])

  useEffect(() => {
    const ac = new AbortController()
    loadSyncStatus(ac.signal)
    return () => ac.abort()
  }, [loadSyncStatus])

  // ── 手动同步 ───────────────────────────────────────────────────────────────

  async function handleSync() {
    setSyncing(true)
    setSyncMsg('')
    try {
      await triggerCrudeSync()
      setSyncMsg('同步已启动，约1分钟后完成，请稍后刷新')
      setTimeout(() => loadSyncStatus(), 8000)
    } catch (e) {
      setSyncMsg(`同步失败: ${e.message}`)
    } finally {
      setSyncing(false)
    }
  }

  // ── Chart.js 数据 ──────────────────────────────────────────────────────────

  const chartData = {
    labels: visibleItems.map(it => yyyymmddToDisplay(it.trade_date)),
    datasets: [
      {
        label:           SYMBOL_META.WTI.label,
        data:            visibleItems.map(it => it.WTI ?? null),
        borderColor:     SYMBOL_COLORS.WTI.border,
        backgroundColor: SYMBOL_COLORS.WTI.background,
        borderWidth: 2,
        pointRadius: visibleItems.length > 200 ? 0 : 2,
        tension: 0.1,
        yAxisID: 'y',
        spanGaps: true,
      },
      {
        label:           SYMBOL_META.BRENT.label,
        data:            visibleItems.map(it => it.BRENT ?? null),
        borderColor:     SYMBOL_COLORS.BRENT.border,
        backgroundColor: SYMBOL_COLORS.BRENT.background,
        borderWidth: 2,
        pointRadius: visibleItems.length > 200 ? 0 : 2,
        tension: 0.1,
        yAxisID: 'y',
        spanGaps: true,
      },
      {
        label:           SYMBOL_META.SC.label,
        data:            visibleItems.map(it => it.SC ?? null),
        borderColor:     SYMBOL_COLORS.SC.border,
        backgroundColor: SYMBOL_COLORS.SC.background,
        borderWidth: 2,
        pointRadius: visibleItems.length > 200 ? 0 : 2,
        tension: 0.1,
        yAxisID: 'y1',
        spanGaps: true,
      },
    ],
  }

  const chartOptions = {
    responsive: true,
    interaction: { mode: 'index', intersect: false },
    plugins: {
      legend: { position: 'top' },
      title: {
        display: true,
        text: '全球原油价格对比（WTI / Brent / 上海SC）',
        font: { size: 15 },
      },
      tooltip: {
        callbacks: {
          label(ctx) {
            const sym   = ['WTI', 'BRENT', 'SC'][ctx.datasetIndex]
            const unit  = SYMBOL_META[sym].unit
            const val   = ctx.parsed.y
            return val == null ? '' : `${ctx.dataset.label}: ${val.toFixed(2)} ${unit}`
          },
        },
      },
    },
    scales: {
      x: {
        ticks: {
          maxTicksLimit: 12,
          maxRotation: 0,
        },
      },
      y: {
        type:     'linear',
        position: 'left',
        title:    { display: true, text: 'USD / 桶' },
        grid:     { color: 'rgba(0,0,0,0.05)' },
      },
      y1: {
        type:     'linear',
        position: 'right',
        title:    { display: true, text: 'CNY / 桶' },
        grid:     { drawOnChartArea: false },
      },
    },
  }

  // ── 最近30行的数据表格 ────────────────────────────────────────────────────

  const recentRows = items.slice(-30).reverse()

  // ── 渲染 ──────────────────────────────────────────────────────────────────

  return (
    <div className="p-6 max-w-7xl mx-auto space-y-6">

      {/* 页头 */}
      <div className="flex flex-wrap items-center justify-between gap-4">
        <div>
          <h1 className="text-2xl font-bold">原油价格对比</h1>
          <p className="text-slate-500 text-sm mt-1">
            WTI（NYMEX）· Brent（ICE）· 上海原油SC（INE）— 日频收盘价
          </p>
        </div>

        {/* 同步区域 */}
        <div className="flex items-center gap-3">
          {syncStatus && (
            <span className={`text-xs px-2 py-1 rounded-full font-medium ${statusBadge(syncStatus.last_status)}`}>
              {syncStatus.last_status === 'success' ? '数据正常' :
               syncStatus.last_status === 'running' ? '同步中' :
               syncStatus.last_status === 'never'   ? '未同步' :
               syncStatus.last_status === 'partial_error' ? '部分失败' : '同步异常'}
            </span>
          )}
          {syncStatus?.last_time && (
            <span className="text-xs text-slate-400">
              最近同步：{syncStatus.last_time.slice(0, 16)}
            </span>
          )}
          <button
            onClick={handleSync}
            disabled={syncing}
            className="px-4 py-2 bg-primary text-white rounded-lg text-sm font-medium hover:bg-primary/90 disabled:opacity-50 transition-colors"
          >
            {syncing ? '同步中…' : '立即同步'}
          </button>
        </div>
      </div>

      {syncMsg && (
        <div className="bg-blue-50 dark:bg-blue-900/20 border border-blue-200 dark:border-blue-800 rounded-lg px-4 py-2 text-sm text-blue-700 dark:text-blue-300">
          {syncMsg}
        </div>
      )}

      {syncStatus?.last_error && (
        <div className="bg-red-50 border border-red-200 rounded-lg px-4 py-2 text-sm text-red-700">
          同步错误：{syncStatus.last_error}
        </div>
      )}

      {/* 日期范围选择器 */}
      <div className="flex flex-wrap items-center gap-2">
        {RANGE_OPTIONS.map((opt, i) => (
          <button
            key={opt.label}
            onClick={() => { setRangeIdx(i); setUseCustom(false) }}
            className={`px-3 py-1.5 rounded-lg text-sm font-medium transition-colors ${
              !useCustom && rangeIdx === i
                ? 'bg-primary text-white'
                : 'bg-slate-100 dark:bg-slate-800 text-slate-600 dark:text-slate-400 hover:bg-slate-200 dark:hover:bg-slate-700'
            }`}
          >
            {opt.label}
          </button>
        ))}

        {/* 自定义日期 */}
        <div className="flex items-center gap-2 ml-2">
          <input
            type="date"
            value={customFrom}
            onChange={e => { setCustomFrom(e.target.value); setUseCustom(true) }}
            className="border border-slate-300 dark:border-slate-600 rounded px-2 py-1 text-sm bg-white dark:bg-slate-800"
          />
          <span className="text-slate-400 text-sm">—</span>
          <input
            type="date"
            value={customTo}
            onChange={e => { setCustomTo(e.target.value); setUseCustom(true) }}
            className="border border-slate-300 dark:border-slate-600 rounded px-2 py-1 text-sm bg-white dark:bg-slate-800"
          />
        </div>

        {latestDate && (
          <span className="text-xs text-slate-400 ml-auto">
            数据最新：{yyyymmddToDisplay(latestDate)}
          </span>
        )}
      </div>

      {/* 图表区域 */}
      <div className="bg-white dark:bg-slate-900 rounded-xl border border-slate-200 dark:border-slate-700 p-4 shadow-sm">
        {loading && (
          <div className="flex items-center justify-center h-64 text-slate-400">
            <span className="material-symbols-outlined animate-spin mr-2">progress_activity</span>
            加载中…
          </div>
        )}
        {error && !loading && (
          <div className="flex flex-col items-center justify-center h-64 text-red-500 gap-2">
            <span className="material-symbols-outlined text-3xl">error</span>
            <p>{error}</p>
            <p className="text-sm text-slate-400">请先点击「立即同步」拉取数据，或检查 API 是否已启动</p>
          </div>
        )}
        {!loading && !error && items.length === 0 && (
          <div className="flex flex-col items-center justify-center h-64 text-slate-400 gap-2">
            <span className="material-symbols-outlined text-4xl">data_usage</span>
            <p>暂无数据，请点击「立即同步」拉取原油行情</p>
          </div>
        )}
        {!loading && !error && items.length > 0 && (
          <>
            <Line data={chartData} options={chartOptions} />
            <RangeScrubber
              dates={items.map(it => it.trade_date)}
              startIdx={scrubStart}
              endIdx={scrubEnd}
              onChange={(s, e) => { setScrubStart(s); setScrubEnd(e) }}
            />
          </>
        )}
      </div>

      {/* 说明卡片 */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        {Object.entries(SYMBOL_META).map(([sym, meta]) => {
          const last = recentRows.find(r => r[sym] != null)
          return (
            <div
              key={sym}
              className="bg-white dark:bg-slate-900 rounded-xl border border-slate-200 dark:border-slate-700 p-4 shadow-sm"
              style={{ borderLeft: `4px solid ${SYMBOL_COLORS[sym].border}` }}
            >
              <div className="text-xs text-slate-400 mb-1">{meta.label}</div>
              <div className="text-2xl font-bold">
                {last?.[sym] != null ? last[sym].toFixed(2) : '—'}
              </div>
              <div className="text-xs text-slate-400 mt-1">{meta.unit}</div>
              {last && (
                <div className="text-xs text-slate-400 mt-1">
                  {yyyymmddToDisplay(last.trade_date)}
                </div>
              )}
            </div>
          )
        })}
      </div>

      {/* 最近30日数据表 */}
      {recentRows.length > 0 && (
        <div className="bg-white dark:bg-slate-900 rounded-xl border border-slate-200 dark:border-slate-700 shadow-sm overflow-hidden">
          <div className="px-4 py-3 border-b border-slate-200 dark:border-slate-700 font-medium text-sm">
            最近 {recentRows.length} 个交易日
          </div>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="bg-slate-50 dark:bg-slate-800">
                  <th className="px-4 py-2 text-left font-medium text-slate-600 dark:text-slate-400">日期</th>
                  <th className="px-4 py-2 text-right font-medium" style={{ color: SYMBOL_COLORS.WTI.border }}>WTI (USD/桶)</th>
                  <th className="px-4 py-2 text-right font-medium" style={{ color: SYMBOL_COLORS.BRENT.border }}>Brent (USD/桶)</th>
                  <th className="px-4 py-2 text-right font-medium" style={{ color: SYMBOL_COLORS.SC.border }}>上海SC (CNY/桶)</th>
                </tr>
              </thead>
              <tbody>
                {recentRows.map((row, idx) => (
                  <tr
                    key={row.trade_date}
                    className={`border-t border-slate-100 dark:border-slate-800 ${
                      idx % 2 === 0 ? '' : 'bg-slate-50/40 dark:bg-slate-800/20'
                    }`}
                  >
                    <td className="px-4 py-2 text-slate-500">{yyyymmddToDisplay(row.trade_date)}</td>
                    <td className="px-4 py-2 text-right font-mono">
                      {row.WTI != null ? row.WTI.toFixed(2) : <span className="text-slate-300">—</span>}
                    </td>
                    <td className="px-4 py-2 text-right font-mono">
                      {row.BRENT != null ? row.BRENT.toFixed(2) : <span className="text-slate-300">—</span>}
                    </td>
                    <td className="px-4 py-2 text-right font-mono">
                      {row.SC != null ? row.SC.toFixed(2) : <span className="text-slate-300">—</span>}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* 数据来源注释 */}
      <div className="text-xs text-slate-400 space-y-1">
        <p>数据来源：WTI / Brent — akshare（新浪财经国际期货）；上海SC — akshare（新浪财经，SC888主力连续合约）</p>
        <p>SC价格为人民币计价（右Y轴），WTI / Brent 为美元计价（左Y轴），单位均为"元/桶"</p>
      </div>

    </div>
  )
}
