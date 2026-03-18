import { useState, useEffect, useMemo, useCallback } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import { fetchFund, fetchFundNav, fetchFundIssues, fetchIndexDaily, subtractDays, setFundBenchmark } from '../api.js'
import { computeMetrics } from '../utils/metrics.js'
import ChartTab, { BENCHMARK_OPTIONS } from './fund-detail/ChartTab.jsx'
import MetricsTab from './fund-detail/MetricsTab.jsx'
import PerformanceTab from './fund-detail/PerformanceTab.jsx'

const TABS = [
  { key: 'chart', label: '业绩走势' },
  { key: 'metrics', label: '业绩指标' },
  { key: 'performance', label: '产品表现' },
]

function formatPctColor(val) {
  if (val == null) return 'text-gray-400'
  if (val > 0) return 'text-red-500'
  if (val < 0) return 'text-emerald-600'
  return 'text-gray-600'
}

function formatPct(val) {
  if (val == null) return '—'
  return `${val > 0 ? '+' : ''}${val.toFixed(2)}%`
}

export default function FundDetail() {
  const { id } = useParams()
  const navigate = useNavigate()

  // Core data
  const [fund, setFund] = useState(null)
  const [navItems, setNavItems] = useState([])
  const [fundIssues, setFundIssues] = useState({ anomalous: [], gaps: [] })
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [retryCount, setRetryCount] = useState(0)

  // View state
  const [activeTab, setActiveTab] = useState('chart')
  const [activeDays, setActiveDays] = useState(0)
  const [navType, setNavType] = useState('unit')

  // Custom date range
  const [isCustomRange, setIsCustomRange] = useState(false)
  const [customFrom, setCustomFrom] = useState('')
  const [customTo, setCustomTo] = useState('')

  // Benchmark
  const [benchmarkCode, setBenchmarkCode] = useState(null)
  const [benchmarkItems, setBenchmarkItems] = useState([])

  // ── Load fund data ──
  useEffect(() => {
    if (!id) { navigate('/'); return }
    const numId = parseInt(id, 10)
    if (isNaN(numId)) { navigate('/'); return }

    const controller = new AbortController()
    const { signal } = controller

    setLoading(true)
    setError(null)

    Promise.all([
      fetchFund(numId, signal),
      fetchFundNav(numId, { limit: 5000, apply_filter: false }, signal),
    ])
      .then(([f, items]) => {
        setFund(f)
        setNavItems(items)
        // Use saved benchmark if available
        if (f.benchmark_index) {
          setBenchmarkCode(f.benchmark_index)
        } else {
          setBenchmarkCode('000852.SH')
        }
        setLoading(false)
      })
      .catch(err => {
        if (err.name === 'AbortError') return
        setError(err.message)
        setLoading(false)
      })

    fetchFundIssues(numId, signal)
      .then(issues => setFundIssues(issues || { anomalous: [], gaps: [] }))
      .catch(err => { if (err.name !== 'AbortError') console.warn('fund issues load failed', err) })

    return () => controller.abort()
  }, [id, navigate, retryCount])

  // ── Filtered items ──
  const filteredItems = useMemo(() => {
    if (isCustomRange) {
      return navItems.filter(item => {
        if (customFrom && item.nav_date < customFrom) return false
        if (customTo && item.nav_date > customTo) return false
        return true
      })
    }
    if (activeDays === 0 || navItems.length === 0) return navItems
    const latestDate = navItems[navItems.length - 1].nav_date
    const from = subtractDays(latestDate, activeDays)
    if (!from) return navItems
    return navItems.filter(item => item.nav_date >= from)
  }, [navItems, activeDays, isCustomRange, customFrom, customTo])

  const filteredDateFrom = filteredItems.length > 0 ? filteredItems[0].nav_date : ''
  const filteredDateTo = filteredItems.length > 0 ? filteredItems[filteredItems.length - 1].nav_date : ''

  // ── Load benchmark data ──
  useEffect(() => {
    if (!benchmarkCode || !filteredDateFrom) {
      setBenchmarkItems([])
      return
    }
    const controller = new AbortController()
    fetchIndexDaily(
      benchmarkCode,
      {
        date_from: filteredDateFrom.replace(/-/g, ''),
        date_to: filteredDateTo.replace(/-/g, ''),
        limit: 2000,
      },
      controller.signal,
    )
      .then(data => setBenchmarkItems(data.items || []))
      .catch(err => { if (err.name !== 'AbortError') setBenchmarkItems([]) })
    return () => controller.abort()
  }, [benchmarkCode, filteredDateFrom, filteredDateTo])

  // ── Normalize benchmark to fund dates ──
  const normalizedData = useMemo(() => {
    if (!benchmarkCode || benchmarkItems.length === 0 || filteredItems.length === 0) return null

    const getVal = item => navType === 'unit' || navType === 'return'
      ? item.unit_nav
      : (item.accumulated_nav ?? item.unit_nav)
    const sortedBench = [...benchmarkItems].sort((a, b) => a.trade_date.localeCompare(b.trade_date))

    let benchIdx = 0
    let firstCommonFundIdx = -1
    let firstBenchClose = null

    for (let i = 0; i < filteredItems.length; i++) {
      const fundDateYmd = filteredItems[i].nav_date.replace(/-/g, '')
      while (benchIdx < sortedBench.length - 1 && sortedBench[benchIdx + 1].trade_date <= fundDateYmd) {
        benchIdx++
      }
      if (sortedBench[benchIdx].trade_date <= fundDateYmd) {
        firstBenchClose = sortedBench[benchIdx].close
        firstCommonFundIdx = i
        break
      }
    }

    if (firstCommonFundIdx === -1 || firstBenchClose == null) return null

    const baseFundVal = getVal(filteredItems[firstCommonFundIdx])
    const baseBenchVal = firstBenchClose
    if (!baseFundVal || !baseBenchVal) return null

    // normalizeBase: 1 in return mode, 100 otherwise (归一到1 vs 归一到100)
    const normalizeBase = navType === 'return' ? 1 : 100

    const labels = []
    const fundNorm = []
    const benchNorm = []

    benchIdx = 0
    let lastBenchClose = null

    for (let i = firstCommonFundIdx; i < filteredItems.length; i++) {
      const item = filteredItems[i]
      const fundDateYmd = item.nav_date.replace(/-/g, '')

      while (benchIdx < sortedBench.length - 1 && sortedBench[benchIdx + 1].trade_date <= fundDateYmd) {
        benchIdx++
      }
      if (sortedBench[benchIdx].trade_date <= fundDateYmd) {
        lastBenchClose = sortedBench[benchIdx].close
      }

      labels.push(item.nav_date)
      fundNorm.push(getVal(item) / baseFundVal * normalizeBase)
      benchNorm.push(lastBenchClose != null ? lastBenchClose / baseBenchVal * normalizeBase : null)
    }

    return { labels, fundNorm, benchNorm }
  }, [benchmarkCode, benchmarkItems, filteredItems, navType])

  // ── Derived ──
  const hasAccumulated = useMemo(
    () => navItems.some(item => item.accumulated_nav != null),
    [navItems],
  )

  // All-time metrics for hero section
  const allMetrics = useMemo(() => computeMetrics(navItems, navType), [navItems, navType])

  // YTD return
  const ytdReturn = useMemo(() => {
    if (navItems.length < 2) return null
    const lastDate = navItems[navItems.length - 1].nav_date
    const yearStart = lastDate.slice(0, 4) + '-01-01'
    const getVal = item => navType === 'unit' ? item.unit_nav : (item.accumulated_nav ?? item.unit_nav)
    let firstVal = null
    for (const item of navItems) {
      if (item.nav_date >= yearStart) {
        firstVal = getVal(item)
        break
      }
    }
    if (!firstVal || firstVal <= 0) return null
    const lastVal = getVal(navItems[navItems.length - 1])
    return (lastVal - firstVal) / firstVal * 100
  }, [navItems, navType])

  const handleBenchmarkChange = useCallback((code) => {
    setBenchmarkCode(code)
    // Persist benchmark choice
    if (fund) {
      setFundBenchmark(fund.fund_id, code).catch(() => {})
    }
  }, [fund])

  const onRetry = useCallback(() => setRetryCount(c => c + 1), [])

  // ── Error state ──
  if (error) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <div className="bg-white rounded-xl shadow p-8 max-w-md text-center">
          <div className="text-4xl mb-4">⚠️</div>
          <h2 className="text-xl font-semibold text-gray-800 mb-2">加载失败</h2>
          <p className="text-red-400 text-xs mt-2">{error}</p>
          <button
            onClick={() => navigate('/')}
            className="mt-4 px-4 py-2 bg-blue-600 text-white rounded-lg text-sm hover:bg-blue-700"
          >
            返回列表
          </button>
          <button
            onClick={onRetry}
            className="mt-4 ml-2 px-4 py-2 bg-gray-100 text-gray-700 rounded-lg text-sm hover:bg-gray-200"
          >
            重试
          </button>
        </div>
      </div>
    )
  }

  return (
    <div className="min-h-screen bg-gray-50">
      {/* Header */}
      <header className="bg-white border-b border-gray-200 sticky top-14 lg:top-0 z-10">
        <div className="max-w-5xl mx-auto px-4 py-4 flex items-center gap-4 min-w-0">
          <button
            onClick={() => navigate('/')}
            className="text-blue-600 hover:text-blue-800 text-sm flex items-center gap-1 shrink-0"
          >
            ← 返回全部基金
          </button>
          {fund && (
            <div className="flex items-baseline gap-2 ml-2 min-w-0 flex-1">
              <h1 className="text-lg font-bold text-gray-900 truncate">{fund.product_name || '—'}</h1>
              <code className="text-xs text-gray-400 font-mono shrink-0">{fund.product_code}</code>
            </div>
          )}
        </div>
      </header>

      <main className="max-w-5xl mx-auto px-4 py-6 space-y-4">
        {/* ── Hero area ── */}
        {loading ? (
          <div className="bg-white rounded-xl shadow p-6">
            <div className="flex gap-8">
              <div className="shimmer rounded h-12 w-40" />
              <div className="grid grid-cols-3 gap-6 flex-1">
                {Array.from({ length: 6 }, (_, i) => (
                  <div key={i}>
                    <div className="shimmer rounded h-3 w-16 mb-2" />
                    <div className="shimmer rounded h-5 w-20" />
                  </div>
                ))}
              </div>
            </div>
          </div>
        ) : fund && (
          <div className="bg-white rounded-xl shadow p-6">
            <div className="flex flex-col md:flex-row gap-6">
              {/* Left: large NAV */}
              <div className="flex-shrink-0">
                <p className="text-xs text-gray-500 mb-1">
                  单位净值
                  <span className="ml-2 text-gray-400">{fund.latest_date || ''}</span>
                </p>
                <p className="text-3xl font-bold text-gray-900 tracking-tight">
                  {fund.latest_nav != null ? fund.latest_nav.toFixed(4) : '—'}
                </p>
                <p className="text-xs text-gray-400 mt-1">
                  {fund.earliest_date && fund.latest_date
                    ? `${fund.earliest_date} ~ ${fund.latest_date}  ·  ${fund.record_count} 条记录`
                    : ''}
                </p>
              </div>

              {/* Right: 6 core metrics */}
              <div className="flex-1 grid grid-cols-3 md:grid-cols-6 gap-3 md:gap-4 md:border-l md:border-gray-100 md:pl-6">
                <HeroStat
                  label="累计净值"
                  value={navItems.length > 0 && navItems[navItems.length - 1].accumulated_nav != null
                    ? navItems[navItems.length - 1].accumulated_nav.toFixed(4)
                    : '—'}
                />
                <HeroStat
                  label="成立以来收益"
                  value={allMetrics ? formatPct(allMetrics.periodReturn) : '—'}
                  valueClass={allMetrics ? formatPctColor(allMetrics.periodReturn) : ''}
                />
                <HeroStat
                  label="今年以来收益"
                  value={formatPct(ytdReturn)}
                  valueClass={formatPctColor(ytdReturn)}
                />
                <HeroStat
                  label="成立以来年化"
                  value={allMetrics?.annualizedReturn != null ? formatPct(allMetrics.annualizedReturn) : '—'}
                  valueClass={allMetrics ? formatPctColor(allMetrics.annualizedReturn) : ''}
                />
                <HeroStat
                  label="最大回撤"
                  value={allMetrics?.maxDrawdown != null ? `${allMetrics.maxDrawdown.toFixed(2)}%` : '—'}
                  valueClass="text-red-500"
                />
                <HeroStat
                  label="夏普比率"
                  value={allMetrics?.sharpe != null ? allMetrics.sharpe.toFixed(3) : '—'}
                  valueClass={allMetrics ? formatPctColor(allMetrics.sharpe) : ''}
                />
              </div>
            </div>
          </div>
        )}

        {/* ── Tab navigation ── */}
        <div className="bg-white rounded-xl shadow">
          <div className="flex border-b border-gray-200">
            {TABS.map(tab => (
              <button
                key={tab.key}
                onClick={() => setActiveTab(tab.key)}
                className={`px-4 py-3 md:px-6 text-sm font-medium transition-colors relative ${
                  activeTab === tab.key
                    ? 'text-blue-600'
                    : 'text-gray-500 hover:text-gray-700'
                }`}
              >
                {tab.label}
                {activeTab === tab.key && (
                  <span className="absolute bottom-0 left-0 right-0 h-0.5 bg-blue-600 rounded-t" />
                )}
              </button>
            ))}
          </div>
        </div>

        {/* ── Tab content ── */}
        {activeTab === 'chart' && (
          <ChartTab
            fund={fund}
            navItems={navItems}
            filteredItems={filteredItems}
            fundIssues={fundIssues}
            benchmarkCode={benchmarkCode}
            setBenchmarkCode={handleBenchmarkChange}
            benchmarkItems={benchmarkItems}
            normalizedData={normalizedData}
            navType={navType}
            setNavType={setNavType}
            hasAccumulated={hasAccumulated}
            loading={loading}
            onRetry={onRetry}
            activeDays={activeDays}
            setActiveDays={setActiveDays}
            isCustomRange={isCustomRange}
            setIsCustomRange={setIsCustomRange}
            customFrom={customFrom}
            setCustomFrom={setCustomFrom}
            customTo={customTo}
            setCustomTo={setCustomTo}
          />
        )}

        {activeTab === 'metrics' && (
          <MetricsTab
            filteredItems={filteredItems}
            navType={navType}
            benchmarkCode={benchmarkCode}
            normalizedData={normalizedData}
          />
        )}

        {activeTab === 'performance' && (
          <PerformanceTab
            navItems={navItems}
            filteredItems={filteredItems}
            navType={navType}
          />
        )}
      </main>
    </div>
  )
}

function HeroStat({ label, value, valueClass }) {
  return (
    <div>
      <p className="text-xs text-gray-500 mb-0.5 whitespace-nowrap">{label}</p>
      <p className={`text-sm font-semibold font-mono ${valueClass || 'text-gray-900'}`}>{value}</p>
    </div>
  )
}
