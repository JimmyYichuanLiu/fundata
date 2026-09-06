import { useState, useEffect, useMemo, useCallback } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import { fetchFund, fetchFundNav, fetchFundIssues, fetchIndexDaily, subtractDays, setFundBenchmark, setFundStrategy } from '../api.js'
import { useAuth } from '../context/AuthContext.jsx'
import { computeMetrics } from '../utils/metrics.js'
import ChartTab, { BENCHMARK_OPTIONS } from './fund-detail/ChartTab.jsx'
import MetricsTab from './fund-detail/MetricsTab.jsx'
import PerformanceTab from './fund-detail/PerformanceTab.jsx'

// ── Strategy taxonomy (same as FundList) ──
const STRATEGY_TAXONOMY = [
  { l1: '期货策略',   l2: ['量化期货', '主观期货'] },
  { l1: '股票对冲',   l2: ['股票市场中性', '股票多空', '择时对冲', '股票T0'] },
  { l1: '股票多头',   l2: ['主观多头', '300指增', '500指增', 'A500指增', '1000指增', '2000指增', '转债指增', '红利指增', '行业指增', '风格指增', '量化选股', '可转债多头', '另类多头'] },
  { l1: '套利策略',   l2: ['股票套利', '期货套利', '期权套利', '基金套利', '可转债套利', '混合套利'] },
  { l1: '期权策略',   l2: ['场内期权', '场外期权'] },
  { l1: '多资产策略', l2: ['宏观策略', '复合策略'] },
  { l1: '债券策略',   l2: ['利率债', '信用债', '债券复合'] },
  { l1: '组合策略',   l2: ['FOF', 'MOM'] },
  { l1: '其他',       l2: [] },
]
const L2_MAP = Object.fromEntries(STRATEGY_TAXONOMY.map(s => [s.l1, s.l2]))

const L1_COLORS = {
  '期货策略':   { bg: '#fef3c7', text: '#92400e', border: '#fbbf24' },
  '股票对冲':   { bg: '#dbeafe', text: '#1e40af', border: '#60a5fa' },
  '股票多头':   { bg: '#fee2e2', text: '#991b1b', border: '#f87171' },
  '套利策略':   { bg: '#d1fae5', text: '#065f46', border: '#34d399' },
  '期权策略':   { bg: '#ede9fe', text: '#5b21b6', border: '#a78bfa' },
  '多资产策略': { bg: '#fce7f3', text: '#9d174d', border: '#f472b6' },
  '债券策略':   { bg: '#e0f2fe', text: '#0c4a6e', border: '#38bdf8' },
  '组合策略':   { bg: '#f0fdf4', text: '#14532d', border: '#4ade80' },
  '其他':       { bg: '#f1f5f9', text: '#475569', border: '#94a3b8' },
}
function strategyColor(l1) {
  return L1_COLORS[l1] || { bg: '#f1f5f9', text: '#475569', border: '#94a3b8' }
}

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
  const { canManage } = useAuth()
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
  const [activeSection, setActiveSection] = useState('chart')
  const [activeDays, setActiveDays] = useState(0)
  const [navType, setNavType] = useState('unit')

  // Custom date range
  const [isCustomRange, setIsCustomRange] = useState(false)
  const [customFrom, setCustomFrom] = useState('')
  const [customTo, setCustomTo] = useState('')

  // Benchmark
  const [benchmarkCode, setBenchmarkCode] = useState(null)
  const [benchmarkItems, setBenchmarkItems] = useState([])

  // Strategy edit state
  const [showStratEdit, setShowStratEdit] = useState(false)
  const [editL1, setEditL1] = useState('')
  const [editL2, setEditL2] = useState('')
  const [stratSaving, setStratSaving] = useState(false)

  // Benchmark controls (lifted from ChartTab so they live in the sticky header)
  const [excessMode, setExcessMode] = useState('off') // 'off' | 'arithmetic' | 'geometric'

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
        // Sync strategy edit state
        setEditL1(f.strategy_l1 || '')
        setEditL2(f.strategy_l2 || '')
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

    const getVal = item => {
      if (navType === 'unit' || navType === 'return') return item.unit_nav
      if (navType === 'adjusted') return item.adj_nav
      return item.accumulated_nav
    }
    const sortedBench = [...benchmarkItems].sort((a, b) => a.trade_date.localeCompare(b.trade_date))

    let benchIdx = 0
    let firstCommonFundIdx = -1
    let firstBenchClose = null

    for (let i = 0; i < filteredItems.length; i++) {
      const fundDateYmd = filteredItems[i].nav_date.replace(/-/g, '')
      while (benchIdx < sortedBench.length - 1 && sortedBench[benchIdx + 1].trade_date <= fundDateYmd) {
        benchIdx++
      }
      if (sortedBench[benchIdx].trade_date <= fundDateYmd && Number.isFinite(getVal(filteredItems[i])) && getVal(filteredItems[i]) > 0) {
        firstBenchClose = sortedBench[benchIdx].close
        firstCommonFundIdx = i
        break
      }
    }

    if (firstCommonFundIdx === -1 || firstBenchClose == null) return null

    const baseFundVal = getVal(filteredItems[firstCommonFundIdx])
    const baseBenchVal = firstBenchClose
    if (!baseFundVal || !baseBenchVal) return null

    // normalizeBase: always 1 (归一到1，统一显示收益率)
    const normalizeBase = 1

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
      fundNorm.push(getVal(item) == null ? null : getVal(item) / baseFundVal * normalizeBase)
      benchNorm.push(lastBenchClose != null ? lastBenchClose / baseBenchVal * normalizeBase : null)
    }

    return { labels, fundNorm, benchNorm }
  }, [benchmarkCode, benchmarkItems, filteredItems, navType])

  // ── Derived ──
  const hasAccumulated = useMemo(
    () => navItems.some(item => item.accumulated_nav != null),
    [navItems],
  )

  const hasAdjusted = useMemo(
    () => navItems.some(item => item.adj_nav != null),
    [navItems],
  )

  // All-time metrics for hero section
  const allMetrics = useMemo(() => computeMetrics(navItems, navType), [navItems, navType])

  // YTD return
  const ytdReturn = useMemo(() => {
    if (navItems.length < 2) return null
    const lastDate = navItems[navItems.length - 1].nav_date
    const yearStart = lastDate.slice(0, 4) + '-01-01'
    const getVal = item => navType === 'adjusted' ? item.adj_nav : navType === 'accumulated' ? item.accumulated_nav : item.unit_nav
    let firstVal = null
    for (const item of navItems) {
      if (item.nav_date >= yearStart) {
        firstVal = getVal(item)
        break
      }
    }
    if (!firstVal || firstVal <= 0) return null
    const lastVal = getVal(navItems[navItems.length - 1])
    return lastVal == null ? null : (lastVal - firstVal) / firstVal * 100
  }, [navItems, navType])

  const handleBenchmarkChange = useCallback((code) => {
    setBenchmarkCode(code)
    // Persist benchmark choice
    if (fund && canManage) {
      setFundBenchmark(fund.fund_id, code).catch(err => setError(err.message))
    }
  }, [fund, canManage])

  const handleSaveStrategy = useCallback(async () => {
    if (!fund || !canManage) return
    setStratSaving(true)
    try {
      await setFundStrategy(fund.fund_id, editL1 || null, editL2 || null)
      setFund(prev => ({ ...prev, strategy_l1: editL1 || null, strategy_l2: editL2 || null }))
      setShowStratEdit(false)
    } catch (err) {
      setError(err.message)
    } finally {
      setStratSaving(false)
    }
  }, [fund, editL1, editL2, canManage])

  const onRetry = useCallback(() => setRetryCount(c => c + 1), [])

  // Close strategy popover on outside click
  useEffect(() => {
    if (!showStratEdit) return
    const handler = (e) => {
      // Only close if click is truly outside (the popover has stopPropagation)
      setShowStratEdit(false)
    }
    document.addEventListener('click', handler)
    return () => document.removeEventListener('click', handler)
  }, [showStratEdit])

  // ── Scrollspy ──
  useEffect(() => {
    const navOffset = 160
    const handleScroll = () => {
      const scrollTop = window.scrollY + navOffset
      let current = TABS[0].key
      for (const tab of TABS) {
        const el = document.getElementById(`section-${tab.key}`)
        if (el && el.offsetTop <= scrollTop) current = tab.key
      }
      setActiveSection(current)
    }
    window.addEventListener('scroll', handleScroll, { passive: true })
    handleScroll()
    return () => window.removeEventListener('scroll', handleScroll)
  }, [])

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
            <div className="flex items-center gap-3 ml-2 min-w-0 flex-1 flex-wrap">
              <div className="flex items-baseline gap-2 min-w-0 flex-wrap">
                <h1 className="text-lg font-bold text-gray-900 break-words">{fund.product_name || '—'}</h1>
                <code className="text-xs text-gray-400 font-mono shrink-0">{fund.product_code}</code>
              </div>
              {/* Strategy badge */}
              <div className="relative shrink-0">
                {fund.strategy_l1 ? (() => {
                  const c = strategyColor(fund.strategy_l1)
                  return (
                    <button
                      onClick={() => setShowStratEdit(v => !v)}
                      className="inline-flex items-center gap-1 px-2 py-0.5 rounded text-xs font-medium border"
                      style={{ backgroundColor: c.bg, color: c.text, borderColor: c.border }}
                      disabled={!canManage}
                    >
                      {fund.strategy_l1}{fund.strategy_l2 ? ` · ${fund.strategy_l2}` : ''}
                      {canManage && <span className="opacity-60">✎</span>}
                    </button>
                  )
                })() : canManage ? (
                  <button
                    onClick={() => setShowStratEdit(v => !v)}
                    className="inline-flex items-center gap-1 px-2 py-0.5 rounded text-xs border border-dashed border-gray-300 text-gray-400 hover:border-blue-400 hover:text-blue-500"
                  >+ 设置策略</button>
                ) : <span className="badge">未分类</span>}
                {/* Strategy editor popover */}
                {canManage && showStratEdit && (
                  <div
                    className="absolute left-0 top-7 z-50 bg-white rounded-xl shadow-xl border border-slate-200 p-3 w-72"
                    onClick={e => e.stopPropagation()}
                  >
                    <div className="flex items-center justify-between mb-2">
                      <span className="text-xs font-semibold text-slate-600">策略分类</span>
                      <button onClick={() => setShowStratEdit(false)} className="text-slate-400 hover:text-slate-600 text-base leading-none">×</button>
                    </div>
                    <div className="mb-2">
                      <p className="text-[10px] text-slate-400 mb-1">一级策略</p>
                      <div className="flex flex-wrap gap-1">
                        {STRATEGY_TAXONOMY.map(s => {
                          const c = strategyColor(s.l1)
                          const active = editL1 === s.l1
                          return (
                            <button
                              key={s.l1}
                              onClick={() => { setEditL1(s.l1); setEditL2('') }}
                              className="px-2 py-0.5 rounded text-[11px] font-medium border transition-colors"
                              style={active
                                ? { backgroundColor: c.border, color: '#fff', borderColor: c.border }
                                : { backgroundColor: c.bg, color: c.text, borderColor: c.border }
                              }
                            >{s.l1}</button>
                          )
                        })}
                        {editL1 && (
                          <button
                            onClick={() => { setEditL1(''); setEditL2('') }}
                            className="px-2 py-0.5 rounded text-[11px] border border-slate-200 text-slate-400 hover:text-rose-500"
                          >清除</button>
                        )}
                      </div>
                    </div>
                    {editL1 && (L2_MAP[editL1] || []).length > 0 && (
                      <div className="mb-3">
                        <p className="text-[10px] text-slate-400 mb-1">二级策略</p>
                        <div className="flex flex-wrap gap-1">
                          {L2_MAP[editL1].map(l2 => (
                            <button
                              key={l2}
                              onClick={() => setEditL2(editL2 === l2 ? '' : l2)}
                              className={`px-2 py-0.5 rounded text-[11px] border transition-colors ${
                                editL2 === l2
                                  ? 'bg-slate-700 text-white border-slate-700'
                                  : 'bg-slate-50 text-slate-600 border-slate-200 hover:border-slate-400'
                              }`}
                            >{l2}</button>
                          ))}
                        </div>
                      </div>
                    )}
                    <button
                      onClick={handleSaveStrategy}
                      disabled={stratSaving}
                      className="w-full py-1.5 bg-blue-600 text-white text-xs rounded-lg hover:bg-blue-700 disabled:opacity-50 transition-colors"
                    >{stratSaving ? '保存…' : '确认'}</button>
                  </div>
                )}
              </div>
            </div>
          )}
        </div>
        {/* Row 2: benchmark selector + excess mode */}
        <div className="max-w-5xl mx-auto px-4 pb-2 flex flex-wrap items-center gap-3">
          <label className="text-xs text-gray-500 shrink-0">基准指数:</label>
          <select
            value={benchmarkCode || ''}
            onChange={e => handleBenchmarkChange(e.target.value || null)}
            className="text-xs border border-gray-200 rounded px-2 py-1 text-gray-700 focus:outline-none focus:ring-1 focus:ring-blue-500"
          >
            {BENCHMARK_OPTIONS.map(opt => (
              <option key={opt.label} value={opt.code || ''}>{opt.label}</option>
            ))}
          </select>
          {benchmarkCode && (
            <>
              <span className="text-xs text-gray-400">（收益率，以1为基准）</span>
              <div className="flex items-center gap-1">
                <span className="text-xs text-gray-500">超额曲线:</span>
                {[
                  { v: 'off',        label: '关' },
                  { v: 'arithmetic', label: '算术' },
                  { v: 'geometric',  label: '几何' },
                ].map(opt => (
                  <button
                    key={opt.v}
                    onClick={() => setExcessMode(opt.v)}
                    className={`px-2 py-0.5 rounded-full text-xs font-medium transition-colors ${
                      excessMode === opt.v
                        ? 'bg-orange-500 text-white'
                        : 'bg-gray-100 text-gray-600 hover:bg-gray-200'
                    }`}
                  >{opt.label}</button>
                ))}
              </div>
            </>
          )}
        </div>
      </header>

      <main className="max-w-5xl mx-auto px-4 py-6 space-y-4"><div className="eyebrow">FUND PROFILE / 基金档案</div>{fund && <p className="text-xs text-slate-500">有效净值截止 {navItems.at(-1)?.nav_date || '—'} · 数据来源 {[...new Set(navItems.map(item => item.data_source).filter(Boolean))].map(source => ({ email: '邮件采集', zx_excel: 'ZX 数据', manual: '手动录入' })[source] || source).join(' / ') || '—'}</p>}
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

        {/* ── Scrollspy nav ── */}
        <ScrollspyNav activeSection={activeSection} />

        {/* ── Scrollable sections ── */}
        <SectionAnchor id="chart">
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
            hasAdjusted={hasAdjusted}
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
            excessMode={excessMode}
            setExcessMode={setExcessMode}
          />
        </SectionAnchor>

        <SectionAnchor id="metrics">
          <MetricsTab
            navItems={navItems}
            filteredItems={filteredItems}
            navType={navType}
            benchmarkCode={benchmarkCode}
            normalizedData={normalizedData}
            benchmarkItems={benchmarkItems}
          />
        </SectionAnchor>

        <SectionAnchor id="performance">
          <PerformanceTab
            navItems={navItems}
            filteredItems={filteredItems}
            navType={navType}
          />
        </SectionAnchor>
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

function ScrollspyNav({ activeSection }) {
  const scrollTo = (key) => {
    const el = document.getElementById(`section-${key}`)
    if (!el) return
    const offset = 120
    const top = el.getBoundingClientRect().top + window.scrollY - offset
    window.scrollTo({ top, behavior: 'smooth' })
  }
  return (
    <div className="bg-white rounded-xl shadow sticky top-[calc(3.5rem+1px)] lg:top-[1px] z-10">
      <div className="flex border-b border-gray-200">
        {TABS.map(tab => (
          <button
            key={tab.key}
            onClick={() => scrollTo(tab.key)}
            className={`px-4 py-3 md:px-6 text-sm font-medium transition-colors relative ${
              activeSection === tab.key
                ? 'text-blue-600'
                : 'text-gray-500 hover:text-gray-700'
            }`}
          >
            {tab.label}
            {activeSection === tab.key && (
              <span className="absolute bottom-0 left-0 right-0 h-0.5 bg-blue-600 rounded-t" />
            )}
          </button>
        ))}
      </div>
    </div>
  )
}

function SectionAnchor({ id, children }) {
  return (
    <div id={`section-${id}`}>
      {children}
    </div>
  )
}
