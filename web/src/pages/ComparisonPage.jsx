import { useState, useEffect, useMemo, useCallback } from 'react'
import { useNavigate } from 'react-router-dom'
import { useCompare } from '../context/CompareContext.jsx'
import { fetchFundNav, fetchIndexDaily } from '../api.js'
import { BENCHMARK_OPTIONS, FUND_COLORS } from '../utils/metricDefs.js'
import ComparisonHero from './fund-comparison/ComparisonHero.jsx'
import ComparisonChart from './fund-comparison/ComparisonChart.jsx'
import ComparisonMetrics from './fund-comparison/ComparisonMetrics.jsx'
import ComparisonCorrelation from './fund-comparison/ComparisonCorrelation.jsx'
import ComparisonRadar from './fund-comparison/ComparisonRadar.jsx'

const SECTIONS = [
  { key: 'chart',       label: '业绩走势' },
  { key: 'metrics',     label: '业绩指标' },
  { key: 'correlation', label: '相关性' },
  { key: 'performance', label: '风险收益画像' },
]

export default function ComparisonPage() {
  const navigate = useNavigate()
  const { compareList, remove, clear } = useCompare()

  useEffect(() => {
    if (compareList.length < 2) navigate('/')
  }, [compareList, navigate])

  const [benchmarkCode, setBenchmarkCode] = useState('000852.SH')
  const [navDataMap, setNavDataMap]       = useState({})
  const [benchItems, setBenchItems]       = useState([])
  const [loading, setLoading]             = useState(true)
  const [activeSection, setActiveSection] = useState('chart')

  // ── Load all fund NAVs ──
  useEffect(() => {
    if (compareList.length === 0) return
    setLoading(true)
    Promise.all(
      compareList.map(f =>
        fetchFundNav(f.fund_id, { limit: 5000, apply_filter: false })
          .then(items => ({ fund_id: f.fund_id, items }))
          .catch(() => ({ fund_id: f.fund_id, items: [] }))
      )
    ).then(results => {
      const map = {}
      results.forEach(r => { map[r.fund_id] = r.items })
      setNavDataMap(map)
      setLoading(false)
    })
  }, [compareList])

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

  if (compareList.length < 2) return null

  return (
    <div className="min-h-screen bg-gray-50">
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
        {/* Hero */}
        <ComparisonHero
          compareList={compareList}
          navDataMap={navDataMap}
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
            navDataMap={navDataMap}
            benchItems={benchItems}
            benchmarkCode={benchmarkCode}
            loading={loading}
            commonStart={commonStart}
          />
        </div>

        {/* Section: Metrics */}
        <div id="cmp-section-metrics">
          <ComparisonMetrics
            compareList={compareList}
            navDataMap={navDataMap}
            benchItems={benchItems}
            benchmarkCode={benchmarkCode}
          />
        </div>

        {/* Section: Correlation */}
        <div id="cmp-section-correlation">
          <ComparisonCorrelation
            compareList={compareList}
            navDataMap={navDataMap}
            benchItems={benchItems}
            benchmarkCode={benchmarkCode}
          />
        </div>

        {/* Section: Radar */}
        <div id="cmp-section-performance">
          <ComparisonRadar
            compareList={compareList}
            navDataMap={navDataMap}
            commonStart={commonStart}
          />
        </div>
      </main>
    </div>
  )
}
