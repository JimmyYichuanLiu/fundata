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
  fetchCrudeNews,
  fetchNewsSyncStatus,
  triggerNewsSync,
  fetchNewsSummary,
  fetchNewsSources,
  fetchHormuzNews,
  fetchPerspectives,
  fetchHormuzCurrent,
  fetchHormuzHistory,
  triggerHormuzSync,
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
  WTI:      { border: '#2563eb', background: 'rgba(37,99,235,0.08)'  },  // 蓝
  BRENT:    { border: '#16a34a', background: 'rgba(22,163,74,0.08)'  },  // 绿
  SC:       { border: '#ea580c', background: 'rgba(234,88,12,0.08)'  },  // 橙
  MURBAN:   { border: '#7c3aed', background: 'rgba(124,58,237,0.08)' },  // 紫
  DME_OMAN: { border: '#0891b2', background: 'rgba(8,145,178,0.08)'  },  // 青
}

const SYMBOL_META = {
  WTI:      { label: 'WTI原油',      unit: 'USD/桶', yAxis: 'y'  },
  BRENT:    { label: 'Brent原油',    unit: 'USD/桶', yAxis: 'y'  },
  SC:       { label: '上海原油SC',   unit: 'CNY/桶', yAxis: 'y1' },
  SC_USD:   { label: '上海原油SC（USD）', unit: 'USD/桶', yAxis: 'y'  },
  MURBAN:   { label: 'Murban原油',   unit: 'USD/桶', yAxis: 'y',  reference: true },
  DME_OMAN: { label: 'DME Oman原油', unit: 'USD/桶', yAxis: 'y',  reference: true },
}

// 新闻分类配置
const NEWS_CATEGORIES = [
  { key: '',               label: '全部'   },
  { key: 'conflict',      label: '冲突'   },
  { key: 'shipping',      label: '运输'   },
  { key: 'crude',         label: '原油'   },
  { key: 'official_west', label: '欧美官方' },
  { key: 'official_iran', label: '伊朗官方' },
  { key: 'official_china',label: '中国官方' },
]

// 分类标签样式
const CATEGORY_BADGE = {
  conflict:       'bg-rose-100 text-rose-600 dark:bg-rose-900/30 dark:text-rose-400',
  shipping:       'bg-sky-100 text-sky-700 dark:bg-sky-900/30 dark:text-sky-400',
  crude:          'bg-amber-100 text-amber-700 dark:bg-amber-900/30 dark:text-amber-400',
  official_west:  'bg-indigo-100 text-indigo-700 dark:bg-indigo-900/30 dark:text-indigo-400',
  official_iran:  'bg-purple-100 text-purple-700 dark:bg-purple-900/30 dark:text-purple-400',
  official_china: 'bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-400',
}

const CATEGORY_LABEL = {
  conflict:       '冲突',
  shipping:       '运输',
  crude:          '原油',
  official_west:  '欧美官方',
  official_iran:  '伊朗官方',
  official_china: '中国官方',
}

// priority 颜色指示点
function priorityDot(priority) {
  if (priority <= 2) return 'bg-red-500'
  if (priority <= 4) return 'bg-orange-400'
  return 'bg-yellow-400'
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

  // 新闻状态
  const [newsItems,      setNewsItems]      = useState([])
  const [newsTotal,      setNewsTotal]      = useState(0)
  const [newsCategory,   setNewsCategory]   = useState('')
  const [newsLoading,    setNewsLoading]    = useState(false)
  const [newsSyncing,    setNewsSyncing]    = useState(false)
  const [newsSyncStatus, setNewsSyncStatus] = useState(null)
  const [newsSyncMsg,    setNewsSyncMsg]    = useState('')

  // 今日观察摘要
  const [summary,        setSummary]        = useState(null)
  const [summaryLoading, setSummaryLoading] = useState(false)
  const [statsRange,     setStatsRange]     = useState('24h')   // '24h' | '7d' | '30d'

  // Hormuz 观察
  const [hormuzItems,   setHormuzItems]   = useState([])
  const [hormuzLoading, setHormuzLoading] = useState(false)

  // 多方视角
  const [perspectives,        setPerspectives]        = useState(null)
  const [perspectivesLoading, setPerspectivesLoading] = useState(false)

  // AIS 船舶监测
  const [aisSnapshot,    setAisSnapshot]    = useState(null)
  const [aisHistory,     setAisHistory]     = useState([])
  const [aisLoading,     setAisLoading]     = useState(false)
  const [aisSyncing,     setAisSyncing]     = useState(false)
  const [aisSyncMsg,     setAisSyncMsg]     = useState('')

  // 新闻排序
  const [newsSort, setNewsSort] = useState('time')  // 'time' | 'relevance'

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

  // ── 今日观察摘要 ──────────────────────────────────────────────────────────

  const loadSummary = useCallback(async (signal) => {
    setSummaryLoading(true)
    try {
      const res = await fetchNewsSummary(signal)
      setSummary(res)
    } catch (e) {
      if (e.name !== 'AbortError') console.error('summary fetch error', e)
    } finally {
      setSummaryLoading(false)
    }
  }, [])

  useEffect(() => {
    const ac = new AbortController()
    loadSummary(ac.signal)
    return () => ac.abort()
  }, [loadSummary])

  // ── 新闻加载 ───────────────────────────────────────────────────────────────

  const loadNews = useCallback(async (signal) => {
    setNewsLoading(true)
    try {
      const res = await fetchCrudeNews({ category: newsCategory || undefined, limit: 50, sort: newsSort }, signal)
      setNewsItems(res.items || [])
      setNewsTotal(res.total || 0)
    } catch (e) {
      if (e.name !== 'AbortError') console.error('news fetch error', e)
    } finally {
      setNewsLoading(false)
    }
  }, [newsCategory, newsSort])

  useEffect(() => {
    const ac = new AbortController()
    loadNews(ac.signal)
    return () => ac.abort()
  }, [loadNews])

  useEffect(() => {
    fetchNewsSyncStatus().then(setNewsSyncStatus).catch(() => {})
  }, [])

  // Hormuz 新闻加载
  const loadHormuz = useCallback(async (signal) => {
    setHormuzLoading(true)
    try {
      const res = await fetchHormuzNews(10, signal)
      setHormuzItems(res || [])
    } catch (e) {
      if (e.name !== 'AbortError') console.error('hormuz fetch error', e)
    } finally {
      setHormuzLoading(false)
    }
  }, [])

  useEffect(() => {
    const ac = new AbortController()
    loadHormuz(ac.signal)
    return () => ac.abort()
  }, [loadHormuz])

  // 多方视角加载
  const loadPerspectives = useCallback(async (signal) => {
    setPerspectivesLoading(true)
    try {
      const res = await fetchPerspectives(signal)
      setPerspectives(res)
    } catch (e) {
      if (e.name !== 'AbortError') console.error('perspectives fetch error', e)
    } finally {
      setPerspectivesLoading(false)
    }
  }, [])

  useEffect(() => {
    const ac = new AbortController()
    loadPerspectives(ac.signal)
    return () => ac.abort()
  }, [loadPerspectives])

  // AIS 船舶数据加载
  const loadAis = useCallback(async (signal) => {
    setAisLoading(true)
    try {
      const [cur, hist] = await Promise.all([
        fetchHormuzCurrent(signal),
        fetchHormuzHistory(signal),
      ])
      setAisSnapshot(cur)
      setAisHistory(hist.items || [])
    } catch (e) {
      if (e.name !== 'AbortError') console.error('AIS fetch error', e)
    } finally {
      setAisLoading(false)
    }
  }, [])

  useEffect(() => {
    const ac = new AbortController()
    loadAis(ac.signal)
    return () => ac.abort()
  }, [loadAis])

  async function handleAisSync() {
    setAisSyncing(true)
    setAisSyncMsg('')
    try {
      const res = await triggerHormuzSync()
      if (res.ok === false) {
        setAisSyncMsg(res.message || '采集失败')
      } else {
        setAisSyncMsg('AIS 采集已启动，约2分钟后完成')
        setTimeout(() => {
          const ac = new AbortController()
          loadAis(ac.signal)
        }, 130000)
      }
    } catch (e) {
      setAisSyncMsg(`失败: ${e.message}`)
    } finally {
      setAisSyncing(false)
    }
  }

  async function handleNewsSync() {
    setNewsSyncing(true)
    setNewsSyncMsg('')
    try {
      await triggerNewsSync()
      setNewsSyncMsg('新闻同步已启动，正在刷新…')
      const ac = new AbortController()
      await Promise.all([
        loadNews(ac.signal),
        loadSummary(ac.signal),
        loadHormuz(ac.signal),
        loadPerspectives(ac.signal)
      ])
      setNewsSyncMsg('新闻已刷新')
      setTimeout(async () => {
        const s = await fetchNewsSyncStatus().catch(() => null)
        if (s) setNewsSyncStatus(s)
      }, 30000)
    } catch (e) {
      setNewsSyncMsg(`失败: ${e.message}`)
    } finally {
      setNewsSyncing(false)
    }
  }

  // ── Chart.js 数据 ──────────────────────────────────────────────────────────

  const chartData = {
    labels: visibleItems.map(it => yyyymmddToDisplay(it.trade_date)),
    datasets: [
      {
        label:           'WTI原油',
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
        label:           'Brent原油',
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
        label:           '上海原油SC（USD）',
        data:            visibleItems.map(it => it.SC_USD ?? null),
        borderColor:     SYMBOL_COLORS.SC.border,
        backgroundColor: SYMBOL_COLORS.SC.background,
        borderWidth: 2,
        pointRadius: visibleItems.length > 200 ? 0 : 2,
        tension: 0.1,
        yAxisID: 'y',
        spanGaps: true,
      },
    ],
  }

  const _DATASET_SYMS = ['WTI', 'BRENT', 'SC_USD']

  const chartOptions = {
    responsive: true,
    interaction: { mode: 'index', intersect: false },
    plugins: {
      legend: { position: 'top' },
      title: {
        display: true,
        text: '全球原油价格对比（WTI / Brent / 上海SC，均以 USD/桶计）',
        font: { size: 15 },
      },
      tooltip: {
        callbacks: {
          label(ctx) {
            const val = ctx.parsed.y
            if (val == null) return ''
            const item = visibleItems[ctx.dataIndex]
            if (ctx.datasetIndex === 2 && item?.SC_RATE) {
              return `${ctx.dataset.label}: ${val.toFixed(2)} USD/桶（汇率 ${item.SC_RATE} CNY=${item.SC?.toFixed(2)} CNY）`
            }
            return `${ctx.dataset.label}: ${val.toFixed(2)} USD/桶`
          },
        },
      },
    },
    scales: {
      x: {
        ticks: { maxTicksLimit: 12, maxRotation: 0 },
      },
      y: {
        type:     'linear',
        position: 'left',
        title:    { display: true, text: 'USD / 桶' },
        grid:     { color: 'rgba(0,0,0,0.05)' },
      },
    },
  }

  // ── 最近30行的数据表格 ────────────────────────────────────────────────────

  const recentRows = items.slice(-30).reverse()

  // ── 渲染 ──────────────────────────────────────────────────────────────────

  return (
    <div className="px-4 py-4 md:p-6 max-w-7xl mx-auto space-y-6">

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

      {/* ── 价格卡片（2+1+2 布局）──────────────────────────────────────────── */}
      {(() => {
        // 交割规格 tooltip 内容
        const SPECS = {
          WTI:      'WTI（West Texas Intermediate）\n交割地：美国俄克拉荷马州 Cushing\n品质：轻质低硫（API≈39.6°，含硫≈0.24%）\n单位：1000桶/手\n结算：实物交割',
          BRENT:    'Brent原油\n交割地：北海（Sullom Voe终端）\n品质：轻质低硫（API≈38.3°，含硫≈0.37%）\n单位：1000桶/手\n结算：EFP现金结算为主',
          SC:       '上海原油期货（SC）\nINE上海国际能源交易中心主力连续合约\n交割地：上海/宁波/舟山等保税仓库\n品质：中质含硫（API 32°±2°，含硫≤2%）\n单位：1000桶/手\n结算：人民币计价实物交割',
          MURBAN:   'Murban原油\nICE IFAD（阿布扎比国际衍生品交易所）\n品质：超轻质低硫（API≈40°，含硫≈0.6%）\n产地：阿布扎比ADNOC\n结算：实物交割，交割地阿联酋',
          DME_OMAN: 'DME Oman原油\nDME（迪拜商品交易所）\n品质：中质含硫（API≈33°，含硫≈1.0%）\n产地：阿曼，亚洲定价基准\n单位：1000桶/手\n结算：实物交割',
        }

        // 找最近有数据的行
        function lastVal(sym) {
          return recentRows.find(r => r[sym] != null)
        }

        function PriceCard({ sym, color }) {
          const last = lastVal(sym)
          const isSC = sym === 'SC'
          const displayVal = isSC
            ? (last?.SC_USD != null ? `$${last.SC_USD.toFixed(2)}` : '—')
            : (last?.[sym] != null ? `$${last[sym].toFixed(2)}` : '—')

          return (
            <div className="group relative">
              <div
                className="bg-white dark:bg-slate-900 rounded-xl border border-slate-200 dark:border-slate-700 p-4 shadow-sm h-full"
                style={{ borderLeft: `4px solid ${color}` }}
              >
                <div className="text-xs text-slate-400 mb-0.5 flex items-center gap-1">
                  {SYMBOL_META[sym]?.label || sym}
                  <span className="text-slate-300 dark:text-slate-600 cursor-help">ⓘ</span>
                </div>
                <div className="text-2xl font-bold text-slate-800 dark:text-slate-100">
                  {displayVal}
                </div>
                <div className="text-xs text-slate-400 mt-0.5">USD/桶</div>
                {isSC && last?.SC != null && (
                  <div className="text-xs text-slate-400 mt-0.5">
                    ≈ ¥{last.SC.toFixed(2)} CNY
                    {last.SC_RATE && <span className="ml-1">(汇率 {last.SC_RATE})</span>}
                  </div>
                )}
                {last && (
                  <div className="text-xs text-slate-400 mt-1">{yyyymmddToDisplay(last.trade_date)}</div>
                )}
                {SYMBOL_META[sym]?.reference && (
                  <div className="text-[10px] text-amber-500 mt-1">⚠ 参考价</div>
                )}
              </div>
              {/* Hover tooltip */}
              <div className="pointer-events-none absolute bottom-full left-0 mb-2 z-50 hidden group-hover:block w-64">
                <div className="bg-slate-800 text-white text-xs rounded-lg px-3 py-2 shadow-xl whitespace-pre-line leading-relaxed">
                  {SPECS[sym]}
                </div>
                <div className="w-2 h-2 bg-slate-800 rotate-45 ml-4 -mt-1" />
              </div>
            </div>
          )
        }

        return (
          <div className="grid grid-cols-3 gap-3">
            {/* 左列：WTI 上 / Brent 下 */}
            <div className="flex flex-col gap-3">
              <PriceCard sym="WTI"   color={SYMBOL_COLORS.WTI.border} />
              <PriceCard sym="BRENT" color={SYMBOL_COLORS.BRENT.border} />
            </div>
            {/* 中列：上海SC（大卡片） */}
            <div>
              <PriceCard sym="SC" color={SYMBOL_COLORS.SC.border} />
            </div>
            {/* 右列：Murban 上 / DME Oman 下 */}
            <div className="flex flex-col gap-3">
              <PriceCard sym="MURBAN"   color={SYMBOL_COLORS.MURBAN.border} />
              <PriceCard sym="DME_OMAN" color={SYMBOL_COLORS.DME_OMAN.border} />
            </div>
          </div>
        )
      })()}

      {/* 更多数据链接 */}
      <div className="flex justify-end">
        <a
          href="/crude/data"
          className="text-sm text-primary hover:underline flex items-center gap-1"
        >
          <span className="material-symbols-outlined text-base">table_view</span>
          更多数据（近30交易日）
        </a>
      </div>

      {/* 最近30日数据表 — 已移至 /crude/data 页面 */}

      {/* 数据来源注释 + 时区说明 */}
      <div className="text-xs text-slate-400 space-y-1">
        <p>数据来源：WTI / Brent — akshare（新浪财经国际期货）；上海SC — akshare（新浪财经，SC888主力连续合约）；Murban / DME Oman — oilprice.com（参考价，非官方）</p>
        <p>SC价格为人民币计价（右Y轴），WTI / Brent / Murban / DME Oman 为美元计价（左Y轴），单位均为"元/桶"</p>
        <p className="flex flex-wrap gap-x-4 gap-y-0.5 mt-1">
          <span>🕐 <strong>WTI</strong>：NYMEX（纽约，UTC-5/UTC-4）</span>
          <span>🕐 <strong>Brent</strong>：ICE（伦敦，UTC+0/UTC+1）</span>
          <span>🕐 <strong>上海SC</strong>：INE（上海，UTC+8）</span>
          <span>🕐 <strong>Murban</strong>：ICE Abu Dhabi（阿布扎比，UTC+4）</span>
          <span>🕐 <strong>DME Oman</strong>：DME（迪拜，UTC+4）</span>
        </p>
        <p className="text-slate-300 dark:text-slate-600">虚线品种（Murban / DME Oman）为参考价，数据来源为第三方抓取，仅供参考</p>
      </div>

      {/* ── 中东冲突与原油新闻 ────────────────────────────────────────────── */}
      <div className="space-y-3">
        {/* 新闻区标题行 */}
        <div className="flex flex-wrap items-center justify-between gap-3">
          <div>
            <h2 className="text-lg font-semibold">中东冲突与原油观察</h2>
            <p className="text-xs text-slate-400 mt-0.5">
              来源：USNI News · OilPrice.com · Al Jazeera · IAEA · Iran International · White House · State Dept · The National · Reuters Energy · 航运聚合
              {newsTotal > 0 && <span className="ml-2">共 {newsTotal} 条</span>}
            </p>
          </div>

          <div className="flex items-center gap-2 flex-wrap">
            {/* 同步状态 */}
            {newsSyncStatus && (
              <span className={`text-xs px-2 py-1 rounded-full font-medium ${statusBadge(newsSyncStatus.last_status)}`}>
                {newsSyncStatus.last_status === 'success' ? '新闻正常'
                  : newsSyncStatus.last_status === 'running' ? '同步中'
                  : newsSyncStatus.last_status === 'never' ? '未同步'
                  : '同步异常'}
              </span>
            )}
            <button
              onClick={handleNewsSync}
              disabled={newsSyncing}
              className="px-3 py-1.5 bg-slate-700 text-white rounded-lg text-xs font-medium hover:bg-slate-600 disabled:opacity-50 transition-colors"
            >
              {newsSyncing ? '同步中…' : '抓取新闻'}
            </button>
          </div>
        </div>

        {newsSyncMsg && (
          <div className="text-xs text-blue-600 dark:text-blue-400 bg-blue-50 dark:bg-blue-900/20 border border-blue-200 dark:border-blue-800 rounded px-3 py-2">
            {newsSyncMsg}
          </div>
        )}

        {/* 今日观察摘要卡片 */}
        <div className="bg-slate-50 dark:bg-slate-800/60 rounded-xl border border-slate-200 dark:border-slate-700 p-4">
          <div className="flex items-center gap-2 mb-3">
            <span className="text-sm font-semibold text-slate-700 dark:text-slate-200">今日观察</span>
            {/* 时间维度切换 */}
            <div className="flex items-center gap-1 ml-2">
              {[['24h', '24小时'], ['7d', '7天'], ['30d', '30天']].map(([key, label]) => (
                <button
                  key={key}
                  onClick={() => setStatsRange(key)}
                  className={`px-2 py-0.5 rounded text-[11px] font-medium transition-colors ${
                    statsRange === key
                      ? 'bg-primary text-white'
                      : 'bg-slate-200 dark:bg-slate-700 text-slate-500 dark:text-slate-400 hover:bg-slate-300 dark:hover:bg-slate-600'
                  }`}
                >
                  {label}
                </button>
              ))}
            </div>
          </div>

          {summaryLoading && (
            <div className="space-y-2">
              {[1, 2, 3].map(i => (
                <div key={i} className="h-4 bg-slate-200 dark:bg-slate-700 rounded animate-pulse" />
              ))}
            </div>
          )}

          {!summaryLoading && summary && (() => {
            // 根据选中的时间段取对应分类统计
            const byCategory = statsRange === '7d'
              ? (summary.by_category_7d || {})
              : statsRange === '30d'
              ? (summary.by_category_30d || {})
              : (summary.by_category || {})
            const totalCount = statsRange === '24h'
              ? summary.last_24h_count
              : Object.values(byCategory).reduce((a, b) => a + b, 0)
            return (
              <>
                {/* 统计行 */}
                <div className="flex flex-wrap items-center gap-3 mb-3">
                  <span className="text-sm font-medium text-slate-700 dark:text-slate-200">
                    {totalCount} 条新闻
                  </span>
                  <div className="flex flex-wrap gap-2">
                    {Object.entries(byCategory)
                      .filter(([, cnt]) => cnt > 0)
                      .map(([cat, cnt]) => (
                        <span
                          key={cat}
                          className={`text-[11px] px-1.5 py-0.5 rounded font-medium ${CATEGORY_BADGE[cat] || 'bg-slate-100 text-slate-500'}`}
                        >
                          {CATEGORY_LABEL[cat] || cat} {cnt}
                        </span>
                      ))
                    }
                  </div>
                </div>

                {/* focus_text 焦点摘要（仅24小时模式显示） */}
                {statsRange === '24h' && summary.focus_text && (
                  <p className="text-xs text-slate-500 dark:text-slate-400 italic mb-2">{summary.focus_text}</p>
                )}

                {/* top5 高优先级新闻（仅24小时模式显示） */}
                {statsRange === '24h' && (
                  summary.last_24h_count === 0 ? (
                    <p className="text-sm text-slate-400">最近24小时暂无新闻</p>
                  ) : (
                    <div className="space-y-2">
                      {(summary.top5 || []).map(item => (
                        <div key={item.id} className="flex items-start gap-2">
                          <span className={`mt-1.5 shrink-0 w-2 h-2 rounded-full ${priorityDot(item.priority)}`} />
                          <div className="min-w-0">
                            <a
                              href={item.url}
                              target="_blank"
                              rel="noopener noreferrer"
                              title={item.title_zh ? item.title : undefined}
                              className="text-sm text-slate-800 dark:text-slate-100 hover:text-primary leading-snug line-clamp-1"
                            >
                              {item.title_zh || item.title}
                            </a>
                            <span className="text-[11px] text-slate-400">{item.source_name}</span>
                          </div>
                        </div>
                      ))}
                    </div>
                  )
                )}
              </>
            )
          })()}

          {!summaryLoading && !summary && (
            <p className="text-sm text-slate-400">请先点击「抓取新闻」加载数据</p>
          )}
        </div>

        {/* Hormuz / 航运观察 */}
        <div className="bg-white dark:bg-slate-900 rounded-xl border border-slate-200 dark:border-slate-700 shadow-sm p-4">
          <div className="flex items-center gap-2 mb-3">
            <span className="text-sm font-semibold text-slate-700 dark:text-slate-200">航运 / Hormuz 观察</span>
            <span className="text-xs text-slate-400">关键词：Hormuz · tanker · Red Sea · shipping · strait</span>
          </div>
          {hormuzLoading && (
            <div className="flex items-center gap-2 text-slate-400 text-sm">
              <span className="material-symbols-outlined animate-spin text-base">progress_activity</span>
              加载中…
            </div>
          )}
          {!hormuzLoading && hormuzItems.length === 0 && (
            <p className="text-sm text-slate-400">暂无相关新闻</p>
          )}
          {!hormuzLoading && hormuzItems.length > 0 && (
            <div className="space-y-2">
              {hormuzItems.map(item => (
                <div key={item.id} className="flex items-start gap-2">
                  <span className={`mt-1.5 shrink-0 w-2 h-2 rounded-full ${priorityDot(item.priority)}`} />
                  <div className="min-w-0 flex-1">
                    <a
                      href={item.url}
                      target="_blank"
                      rel="noopener noreferrer"
                      title={item.title_zh ? item.title : undefined}
                      className="text-sm text-slate-800 dark:text-slate-100 hover:text-primary leading-snug line-clamp-1"
                    >
                      {item.title_zh || item.title}
                    </a>
                    <div className="flex items-center gap-2 mt-0.5 text-[11px] text-slate-400">
                      <span>{item.source_name}</span>
                      {item.published_at && (
                        <span>{item.published_at.slice(0, 16).replace('T', ' ')}</span>
                      )}
                      <span className={`px-1.5 py-0.5 rounded font-medium ${CATEGORY_BADGE[item.category] || 'bg-slate-100 text-slate-500'}`}>
                        {CATEGORY_LABEL[item.category] || item.category}
                      </span>
                    </div>
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>

        {/* 全部新闻入口 */}
        <div className="flex justify-end">
          <a
            href="/news"
            className="flex items-center gap-1.5 px-4 py-2 bg-slate-100 dark:bg-slate-800 hover:bg-slate-200 dark:hover:bg-slate-700 text-slate-700 dark:text-slate-300 rounded-lg text-sm font-medium transition-colors"
          >
            <span className="material-symbols-outlined text-base">newspaper</span>
            查看全部新闻
          </a>
        </div>
      </div>

      {/* ── AIS 船舶监测 ──────────────────────────────────────────────────── */}
      <div className="bg-white dark:bg-slate-900 rounded-xl border border-slate-200 dark:border-slate-700 shadow-sm p-4">
        <div className="flex flex-wrap items-center justify-between gap-2 mb-3">
          <div>
            <span className="text-sm font-semibold text-slate-700 dark:text-slate-200">霍尔木兹海峡 AIS 船舶监测</span>
            <span className="text-xs text-slate-400 ml-2">数据源：AISStream.io · 每30分钟更新</span>
          </div>
          <button
            onClick={handleAisSync}
            disabled={aisSyncing}
            className="px-3 py-1.5 bg-slate-700 text-white rounded-lg text-xs font-medium hover:bg-slate-600 disabled:opacity-50 transition-colors"
          >
            {aisSyncing ? '采集中…' : '立即采集'}
          </button>
        </div>

        {aisSyncMsg && (
          <div className="text-xs text-blue-600 dark:text-blue-400 bg-blue-50 dark:bg-blue-900/20 border border-blue-200 dark:border-blue-800 rounded px-3 py-2 mb-3">
            {aisSyncMsg}
          </div>
        )}

        {aisLoading && (
          <div className="flex items-center gap-2 text-slate-400 text-sm">
            <span className="material-symbols-outlined animate-spin text-base">progress_activity</span>
            加载中…
          </div>
        )}

        {!aisLoading && aisSnapshot && !aisSnapshot.api_key_configured && (
          <p className="text-sm text-amber-600 dark:text-amber-400">
            AISSTREAM_API_KEY 未配置，请在 .env 中设置后重启服务。注册地址：aisstream.io
          </p>
        )}

        {!aisLoading && aisSnapshot?.api_key_configured && !aisSnapshot.snapshot && (
          <p className="text-sm text-red-500 font-medium flex items-center gap-1.5">
            <span className="material-symbols-outlined text-base">warning</span>
            数据缺失，尝试更新中 — 请点击「立即采集」或等待定时任务
          </p>
        )}

        {!aisLoading && aisSnapshot?.snapshot && (
          <div className="space-y-3">
            {/* 快照统计卡片 */}
            <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
              <div className="bg-slate-50 dark:bg-slate-800 rounded-lg p-3 text-center">
                <div className="text-2xl font-bold text-slate-800 dark:text-slate-100">
                  {aisSnapshot.snapshot.vessel_count}
                </div>
                <div className="text-xs text-slate-400 mt-0.5">区域内船只</div>
              </div>
              <div className="bg-amber-50 dark:bg-amber-900/20 rounded-lg p-3 text-center">
                <div className="text-2xl font-bold text-amber-700 dark:text-amber-400">
                  {aisSnapshot.snapshot.tanker_count}
                </div>
                <div className="text-xs text-slate-400 mt-0.5">油轮</div>
              </div>
              <div className="bg-slate-50 dark:bg-slate-800 rounded-lg p-3 text-center col-span-2">
                <div className="text-xs text-slate-500 dark:text-slate-400">
                  快照时间：{aisSnapshot.snapshot.snapshot_at?.slice(0, 16).replace('T', ' ')} UTC
                </div>
                <div className="text-xs text-slate-400 mt-1">
                  数据质量：{aisSnapshot.snapshot.data_quality === 'full' ? '完整' : '部分'}
                </div>
              </div>
            </div>

            {/* 24小时趋势 */}
            {aisHistory.length > 1 && (
              <div>
                <div className="text-xs text-slate-400 mb-1">近24小时趋势（船只数 / 油轮数）</div>
                <div className="flex items-end gap-0.5 h-12">
                  {aisHistory.map((snap, i) => {
                    const maxV = Math.max(...aisHistory.map(s => s.vessel_count || 0), 1)
                    const h = Math.round(((snap.vessel_count || 0) / maxV) * 100)
                    const ht = Math.round(((snap.tanker_count || 0) / maxV) * 100)
                    return (
                      <div key={i} className="flex-1 flex items-end gap-px" title={`${snap.snapshot_at?.slice(11,16)} — 船只:${snap.vessel_count} 油轮:${snap.tanker_count}`}>
                        <div className="flex-1 bg-slate-300 dark:bg-slate-600 rounded-t" style={{ height: `${h}%` }} />
                        <div className="flex-1 bg-amber-400 dark:bg-amber-500 rounded-t" style={{ height: `${ht}%` }} />
                      </div>
                    )
                  })}
                </div>
                <div className="flex items-center gap-3 mt-1 text-[10px] text-slate-400">
                  <span className="flex items-center gap-1"><span className="w-2 h-2 rounded bg-slate-300 dark:bg-slate-600 inline-block" />船只</span>
                  <span className="flex items-center gap-1"><span className="w-2 h-2 rounded bg-amber-400 inline-block" />油轮</span>
                </div>
              </div>
            )}

            {/* 船只列表（最多显示10条） */}
            {aisSnapshot.vessels?.length > 0 && (
              <div>
                <div className="text-xs text-slate-400 mb-1">当前区域船只（前10条）</div>
                <div className="overflow-x-auto">
                  <table className="w-full text-xs">
                    <thead>
                      <tr className="bg-slate-50 dark:bg-slate-800 text-slate-500">
                        <th className="px-2 py-1 text-left">船名</th>
                        <th className="px-2 py-1 text-left">类型</th>
                        <th className="px-2 py-1 text-right">速度(kn)</th>
                        <th className="px-2 py-1 text-right">纬度</th>
                        <th className="px-2 py-1 text-right">经度</th>
                      </tr>
                    </thead>
                    <tbody>
                      {aisSnapshot.vessels.slice(0, 10).map(v => (
                        <tr key={v.mmsi} className="border-t border-slate-100 dark:border-slate-800">
                          <td className="px-2 py-1 font-medium">{v.ship_name || v.mmsi}</td>
                          <td className="px-2 py-1">
                            {v.vessel_type >= 80 && v.vessel_type <= 89
                              ? <span className="text-amber-600 font-medium">油轮</span>
                              : v.vessel_type ?? '—'}
                          </td>
                          <td className="px-2 py-1 text-right font-mono">{v.speed?.toFixed(1) ?? '—'}</td>
                          <td className="px-2 py-1 text-right font-mono">{v.lat?.toFixed(3) ?? '—'}</td>
                          <td className="px-2 py-1 text-right font-mono">{v.lon?.toFixed(3) ?? '—'}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
                {aisSnapshot.vessels.length > 10 && (
                  <p className="text-xs text-slate-400 mt-1">…共 {aisSnapshot.vessels.length} 艘</p>
                )}
              </div>
            )}

            <p className="text-[10px] text-slate-300 dark:text-slate-600">{aisSnapshot.note}</p>
          </div>
        )}
      </div>

      {/* ── 多方视角对比 ──────────────────────────────────────────────────── */}
      <div className="bg-white dark:bg-slate-900 rounded-xl border border-slate-200 dark:border-slate-700 shadow-sm p-4">
        <div className="mb-3 flex items-center gap-2 flex-wrap">
          <span className="text-sm font-semibold text-slate-700 dark:text-slate-200">多方视角对比</span>
          <span className="text-xs text-slate-400">欧美官方 / 中国官方 / 伊朗官方 — 按相关度各取最高5条</span>
          {perspectives?.window && (
            <span className={`text-[11px] px-1.5 py-0.5 rounded font-medium ${
              perspectives.window === '24h'
                ? 'bg-emerald-100 text-emerald-700 dark:bg-emerald-900/30 dark:text-emerald-400'
                : perspectives.window === '7d'
                ? 'bg-yellow-100 text-yellow-700 dark:bg-yellow-900/30 dark:text-yellow-400'
                : 'bg-orange-100 text-orange-700 dark:bg-orange-900/30 dark:text-orange-400'
            }`}>
              {perspectives.window === '24h' ? '过去24小时' : perspectives.window === '7d' ? '回退至7天' : '回退至30天'}
            </span>
          )}
        </div>

        {perspectivesLoading && (
          <div className="flex items-center gap-2 text-slate-400 text-sm">
            <span className="material-symbols-outlined animate-spin text-base">progress_activity</span>
            加载中…
          </div>
        )}

        {!perspectivesLoading && perspectives && (
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            {[
              { key: 'west',  label: '欧美官方', badge: 'bg-indigo-100 text-indigo-700 dark:bg-indigo-900/30 dark:text-indigo-400' },
              { key: 'china', label: '中国官方', badge: 'bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-400' },
              { key: 'iran',  label: '伊朗官方', badge: 'bg-purple-100 text-purple-700 dark:bg-purple-900/30 dark:text-purple-400' },
            ].map(({ key, label, badge }) => (
              <div key={key} className="space-y-2">
                <span className={`inline-block text-[11px] px-2 py-0.5 rounded font-medium ${badge}`}>{label}</span>
                {(perspectives[key] || []).length === 0 ? (
                  <p className="text-xs text-slate-400">
                    {perspectives?.window === '24h' ? '过去24小时暂无相关新闻' : '近7天暂无相关新闻'}
                  </p>
                ) : (
                  (perspectives[key] || []).map(item => (
                    <div key={item.id} className="flex items-start gap-1.5">
                      <span className={`mt-1.5 shrink-0 w-1.5 h-1.5 rounded-full ${priorityDot(item.priority)}`} />
                      <div className="min-w-0">
                        <a
                          href={item.url}
                          target="_blank"
                          rel="noopener noreferrer"
                          title={item.title_zh ? item.title : undefined}
                          className="text-xs text-slate-800 dark:text-slate-100 hover:text-primary leading-snug line-clamp-2"
                        >
                          {item.title_zh || item.title}
                        </a>
                        <div className="text-[10px] text-slate-400 mt-0.5">
                          {item.source_name}
                          {item.published_at && <span className="ml-1">{item.published_at.slice(0, 10)}</span>}
                        </div>
                      </div>
                    </div>
                  ))
                )}
              </div>
            ))}
          </div>
        )}

        {!perspectivesLoading && !perspectives && (
          <p className="text-sm text-slate-400">请先点击「抓取新闻」加载数据</p>
        )}
      </div>

    </div>
  )
}
