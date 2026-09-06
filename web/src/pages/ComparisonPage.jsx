import { useState, useEffect, useMemo, useCallback } from 'react'
import { useNavigate, useSearchParams } from 'react-router-dom'
import { useCompare } from '../context/CompareContext.jsx'
import { fetchFundNav, fetchIndexDaily, fetchFunds } from '../api.js'
import { BENCHMARK_OPTIONS, FUND_COLORS } from '../utils/metricDefs.js'
import ComparisonHero from './fund-comparison/ComparisonHero.jsx'
import ComparisonChart from './fund-comparison/ComparisonChart.jsx'
import ComparisonMetrics from './fund-comparison/ComparisonMetrics.jsx'
import ComparisonCorrelation from './fund-comparison/ComparisonCorrelation.jsx'
import ComparisonRadar from './fund-comparison/ComparisonRadar.jsx'

import FundPicker from '../components/FundPicker.jsx'
import PageState from '../components/PageState.jsx'
import { parseFundIds, fundSelectionSearch } from '../utils/selection.js'
const SECTIONS = [
  { key: 'chart',       label: '业绩走势' },
  { key: 'metrics',     label: '业绩指标' },
  { key: 'correlation', label: '相关性' },
  { key: 'performance', label: '风险收益画像' },
]

export default function ComparisonPage() {
  const navigate = useNavigate()
  const { compareList, remove, clear, toggle, setSelection } = useCompare()
  const [, setParams] = useSearchParams()
  const [funds, setFunds] = useState([])
  const [ready, setReady] = useState(false)
  const [error, setError] = useState('')
  const [from, setFrom] = useState('')
  const [to, setTo] = useState('')
  const [navType, setNavType] = useState('unit_nav')
  const [revision, setRevision] = useState(0)

  useEffect(() => {
    const controller = new AbortController()
    fetchFunds(controller.signal).then(rows => {
      setFunds(rows)
      const ids = parseFundIds(window.location.search)
      if (ids.length) setSelection(ids.map(id => rows.find(f => f.fund_id === id)).filter(Boolean))
      setReady(true)
    }).catch(err => { if (err.name !== 'AbortError') setError(err.message) })
    return () => controller.abort()
  }, [revision])
  useEffect(() => {
    if (ready) setParams(fundSelectionSearch(compareList), { replace: true })
  }, [compareList, ready, setParams])
  useEffect(() => {
    const restore = () => {
      const ids = parseFundIds(window.location.search)
      setSelection(ids.map(id => funds.find(f => f.fund_id === id)).filter(Boolean))
    }
    window.addEventListener('popstate', restore)
    return () => window.removeEventListener('popstate', restore)
  }, [funds, setSelection])

  const [benchmarkCode, setBenchmarkCode] = useState('000852.SH')
  const [navDataMap, setNavDataMap]       = useState({})
  const [benchItems, setBenchItems]       = useState([])
  const [loading, setLoading]             = useState(true)
  const [activeSection, setActiveSection] = useState('chart')

  useEffect(() => {
    if (!ready || compareList.length === 0) { setLoading(false); setNavDataMap({}); return }
    const controller = new AbortController()
    setLoading(true); setError('')
    Promise.all(compareList.map(f => fetchFundNav(f.fund_id, { limit: 5000, apply_filter: true }, controller.signal).then(items => ({ fund_id: f.fund_id, items }))))
      .then(results => setNavDataMap(Object.fromEntries(results.map(r => [r.fund_id, r.items]))))
      .catch(err => { if (err.name !== 'AbortError') setError(err.message) })
      .finally(() => { if (!controller.signal.aborted) setLoading(false) })
    return () => controller.abort()
  }, [compareList, ready, revision])
  const displayMap = useMemo(() => Object.fromEntries(Object.entries(navDataMap).map(([id, items]) => [id, items.filter(item => (!from || item.nav_date >= from) && (!to || item.nav_date <= to)).map(item => ({ ...item, unit_nav: Number.isFinite(item[navType]) && item[navType] > 0 ? item[navType] : null }))])), [navDataMap, from, to, navType])

  // ── commonStart: latest of all funds' first dates (公共区间起点) ──
  const commonStart = useMemo(() => {
    const firstDates = Object.values(navDataMap)
      .map(items => items.length > 0 ? items[0].nav_date : null)
      .filter(Boolean)
    if (firstDates.length === 0) return ''
    // latest first date = the date from which ALL funds have data
    return firstDates.reduce((a, b) => (a > b ? a : b))
  }, [navDataMap])

  // ── dateRange for benchmark fetch: from commonStart to latest overall ──
  const dateRange = useMemo(() => {
    const allDates = Object.values(navDataMap).flatMap(items => items.map(i => i.nav_date)).filter(Boolean)
    if (allDates.length === 0) return { from: '', to: '' }
    return {
      from: allDates.reduce((a, b) => (a < b ? a : b)),
      to:   allDates.reduce((a, b) => (a > b ? a : b)),
    }
  }, [navDataMap])

  useEffect(() => {
    if (!Object.keys(navDataMap).length || from || to) return
    const lists = Object.values(navDataMap).filter(items => items.length)
    if (lists.length) {
      const commonFrom = lists.map(items => items[0].nav_date).sort().at(-1)
      const commonTo = lists.map(items => items.at(-1).nav_date).sort()[0]
      setFrom(commonFrom <= commonTo ? commonFrom : dateRange.from)
      setTo(commonFrom <= commonTo ? commonTo : dateRange.to)
    }
  }, [navDataMap, from, to, dateRange])
  const noCommonRange = useMemo(() => {
    const lists = Object.values(navDataMap).filter(items => items.length)
    return lists.length > 1 && lists.map(items => items[0].nav_date).sort().at(-1) > lists.map(items => items.at(-1).nav_date).sort()[0]
  }, [navDataMap])
  const setWindow = days => {
    if (!dateRange.to) return
    setTo(dateRange.to)
    if (!days) { setFrom(dateRange.from); return }
    const date = new Date(dateRange.to + 'T00:00:00Z')
    date.setUTCDate(date.getUTCDate() - days)
    setFrom(date.toISOString().slice(0, 10))
  }
  const visibleBench = useMemo(() => benchItems.filter(item => {
    const date = String(item.trade_date).replaceAll('-', '')
    return (!from || date >= from.replaceAll('-', '')) && (!to || date <= to.replaceAll('-', ''))
  }), [benchItems, from, to])
  // ── Load benchmark ──
  useEffect(() => {
    if (!benchmarkCode || !dateRange.from) { setBenchItems([]); return }
    const controller = new AbortController()
    fetchIndexDaily(benchmarkCode, {
      date_from: dateRange.from.replace(/-/g, ''),
      date_to:   dateRange.to.replace(/-/g, ''),
      limit: 2000,
    }, controller.signal)
      .then(data => setBenchItems(data.items || []))
      .catch(() => setBenchItems([]))
    return () => controller.abort()
  }, [benchmarkCode, dateRange.from, dateRange.to])

  // ── Scrollspy ──
  useEffect(() => {
    const offset = 160
    const handler = () => {
      const top = window.scrollY + offset
      let current = SECTIONS[0].key
      for (const s of SECTIONS) {
        const el = document.getElementById(`cmp-section-${s.key}`)
        if (el && el.offsetTop <= top) current = s.key
      }
      setActiveSection(current)
    }
    window.addEventListener('scroll', handler, { passive: true })
    handler()
    return () => window.removeEventListener('scroll', handler)
  }, [])

  const scrollTo = useCallback((key) => {
    const el = document.getElementById(`cmp-section-${key}`)
    if (!el) return
    window.scrollTo({ top: el.getBoundingClientRect().top + window.scrollY - 120, behavior: 'smooth' })
  }, [])

  if (!ready && error) return <PageState error={error} onRetry={() => setRevision(v => v + 1)} />

  return (
    <div className="min-h-screen">
      {/* Header */}
      <header className="bg-white border-b border-gray-200 sticky top-14 lg:top-0 z-10">
        <div className="max-w-6xl mx-auto px-4 py-4 flex items-center gap-4">
          <button onClick={() => navigate('/')} className="text-blue-600 hover:text-blue-800 text-sm shrink-0">
            ← 返回列表
          </button>
          <h1 className="text-lg font-bold text-gray-900">基金对比</h1>
          <button onClick={clear} className="ml-auto text-xs text-gray-400 hover:text-red-500 transition-colors">
            清空全部
          </button>
        </div>
      </header>

      <main className="max-w-6xl mx-auto px-4 py-6 space-y-4">
        <div><div className="eyebrow">COMPARE / 多维基金研究</div><h2 className="text-2xl font-bold mb-3">同一视角，看见表现差异</h2><p className="text-xs text-slate-500 mb-5">选择最多八只基金。选择项保存在网址中，可直接分享或刷新继续研究。</p><FundPicker funds={funds} selectedIds={compareList.map(f => f.fund_id)} onToggle={toggle} /></div>
        <div className="toolbar panel"><label className="text-xs">起始日期 <input className="control" aria-label="对比起始日期" type="date" value={from} onChange={e => setFrom(e.target.value)} /></label><label className="text-xs">截止日期 <input className="control" aria-label="对比截止日期" type="date" value={to} onChange={e => setTo(e.target.value)} /></label><select className="control" aria-label="净值类型" value={navType} onChange={e => setNavType(e.target.value)}><option value="unit_nav">单位净值</option><option value="accumulated_nav">累计净值</option><option value="adj_nav">复权净值</option></select></div>
        <div className="flex flex-wrap gap-2">{[[30, '近一月'], [90, '近三月'], [365, '近一年'], [0, '全部区间']].map(([days, label]) => <button className="button-secondary" key={days} onClick={() => setWindow(days)}>{label}</button>)}</div>
        {!loading && compareList.length > 1 && Object.values(displayMap).some(items => !items.length || items.some(item => item.unit_nav == null)) && <div className="notice">部分基金在所选区间或净值类型下存在缺失记录。图表保留缺口，指标只使用有效观测值。</div>}
        {error && <PageState error={error} onRetry={() => setRevision(v => v + 1)} />}
        {compareList.length < 2 && <PageState title="开始一次基金对比">请在上方选择至少两只基金。</PageState>}
        {noCommonRange && <div className="notice" role="status">所选基金没有公共净值区间。可减少基金，或选择“全部区间”查看各自历史；各基金起点不同，相关性与共同区间指标可能缺失。</div>}
        {from && to && from > to && <div className="notice notice-error" role="alert">起始日期不能晚于截止日期。</div>}
        {compareList.length >= 2 && <>
        {/* Hero */}
        <ComparisonHero
          compareList={compareList}
          navDataMap={displayMap}
          loading={loading}
          onRemove={remove}
        />

        {/* Scrollspy nav + benchmark selector */}
        <div className="bg-white rounded-xl shadow sticky top-[calc(3.5rem+1px)] lg:top-[1px] z-10">
          <div className="flex border-b border-gray-200 items-center overflow-x-auto">
            {SECTIONS.map(s => (
              <button
                key={s.key}
                onClick={() => scrollTo(s.key)}
                className={`px-4 py-3 md:px-6 text-sm font-medium transition-colors relative shrink-0 ${
                  activeSection === s.key ? 'text-blue-600' : 'text-gray-500 hover:text-gray-700'
                }`}
              >
                {s.label}
                {activeSection === s.key && (
                  <span className="absolute bottom-0 left-0 right-0 h-0.5 bg-blue-600 rounded-t" />
                )}
              </button>
            ))}
            <div className="ml-auto flex items-center gap-2 px-4 shrink-0">
              <label className="text-xs text-gray-500 shrink-0">对标指数:</label>
              <select
                value={benchmarkCode || ''}
                onChange={e => setBenchmarkCode(e.target.value || null)}
                className="text-xs border border-gray-200 rounded px-2 py-1 focus:outline-none focus:ring-1 focus:ring-blue-500"
              >
                {BENCHMARK_OPTIONS.map(opt => (
                  <option key={opt.label} value={opt.code || ''}>{opt.label}</option>
                ))}
              </select>
            </div>
          </div>
        </div>

        {/* Section: Chart */}
        <div id="cmp-section-chart">
          <ComparisonChart
            compareList={compareList}
            navDataMap={displayMap}
            benchItems={visibleBench}
            benchmarkCode={benchmarkCode}
            loading={loading}
            commonStart={from}
          />
        </div>

        {/* Section: Metrics */}
        <div id="cmp-section-metrics">
          <ComparisonMetrics
            compareList={compareList}
            navDataMap={displayMap}
            benchItems={visibleBench}
            benchmarkCode={benchmarkCode}
          />
        </div>

        {/* Section: Correlation */}
        <div id="cmp-section-correlation">
          <ComparisonCorrelation
            compareList={compareList}
            navDataMap={displayMap}
            benchItems={visibleBench}
            benchmarkCode={benchmarkCode}
          />
        </div>

        {/* Section: Radar */}
        <div id="cmp-section-performance">
          <ComparisonRadar
            compareList={compareList}
            navDataMap={displayMap}
            commonStart={commonStart}
          />
        </div>
        </>}
      </main>
    </div>
  )
}
