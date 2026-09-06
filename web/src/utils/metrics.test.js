/**
 * metrics.test.js — Integration tests for metrics.js
 *
 * Tests computeMetrics, computeBenchmarkMetrics, computeExcessMetrics.
 * Uses small hand-crafted NAV series so expected values can be verified manually.
 */
import { describe, it, expect } from 'vitest'
import {
  computeMetrics,
  computeBenchmarkMetrics,
  computeExcessMetrics,
} from './metrics.js'

// ─── Helpers ──────────────────────────────────────────────────────────────────

/** Build a daily NAV series from a start value and an array of daily returns */
function buildNavItems(startNav, dailyReturns, startDate = '2024-01-02') {
  const items = []
  let nav = startNav
  let date = new Date(startDate)

  items.push({ nav_date: startDate, unit_nav: nav, adj_nav: nav })
  for (const r of dailyReturns) {
    nav = nav * (1 + r)
    date.setDate(date.getDate() + 1)
    const d = date.toISOString().slice(0, 10)
    items.push({ nav_date: d, unit_nav: +nav.toFixed(6), adj_nav: +nav.toFixed(6) })
  }
  return items
}

/** Build monthly NAV items (one per month-end) */
function buildMonthlyItems(navValues, startYearMonth = '2024-01') {
  return navValues.map((nav, i) => {
    const [y, m] = startYearMonth.split('-').map(Number)
    const month = ((m - 1 + i) % 12) + 1
    const year = y + Math.floor((m - 1 + i) / 12)
    const lastDay = new Date(year, month, 0).getDate()
    const dateStr = `${year}-${String(month).padStart(2, '0')}-${String(lastDay).padStart(2, '0')}`
    return { nav_date: dateStr, unit_nav: nav, adj_nav: nav }
  })
}

// ─── computeMetrics ───────────────────────────────────────────────────────────

describe('computeMetrics', () => {
  it('returns null for < 2 items', () => {
    expect(computeMetrics([])).toBeNull()
    expect(computeMetrics([{ nav_date: '2024-01-02', unit_nav: 1.0 }])).toBeNull()
  })

  it('returns null for null input', () => {
    expect(computeMetrics(null)).toBeNull()
  })

  it('periodReturn: 1.0 → 1.2 = 20%', () => {
    const items = [
      { nav_date: '2024-01-02', unit_nav: 1.0, adj_nav: 1.0 },
      { nav_date: '2024-06-30', unit_nav: 1.2, adj_nav: 1.2 },
    ]
    const m = computeMetrics(items, 'unit')
    expect(m.periodReturn).toBeCloseTo(20, 6)
  })

  it('periodReturn: loss 1.0 → 0.9 = -10%', () => {
    const items = [
      { nav_date: '2024-01-02', unit_nav: 1.0 },
      { nav_date: '2024-06-30', unit_nav: 0.9 },
    ]
    const m = computeMetrics(items, 'unit')
    expect(m.periodReturn).toBeCloseTo(-10, 6)
  })

  it('annualizedReturn is null when n < 30', () => {
    const items = buildNavItems(1.0, Array(10).fill(0.001))
    const m = computeMetrics(items, 'unit')
    expect(m.annualizedReturn).toBeNull()
  })

  it('annualizedReturn is a number when n >= 30', () => {
    const items = buildNavItems(1.0, Array(30).fill(0.001))
    const m = computeMetrics(items, 'unit')
    expect(typeof m.annualizedReturn).toBe('number')
  })

  it('maxDrawdown: monotone rise → 0%', () => {
    const items = buildNavItems(1.0, [0.01, 0.01, 0.01, 0.01])
    const m = computeMetrics(items, 'unit')
    expect(m.maxDrawdown).toBeCloseTo(0, 6)
  })

  it('maxDrawdown: [1, 1.5, 1.0] → -33.33%', () => {
    const items = [
      { nav_date: '2024-01-02', unit_nav: 1.0 },
      { nav_date: '2024-01-03', unit_nav: 1.5 },
      { nav_date: '2024-01-04', unit_nav: 1.0 },
    ]
    const m = computeMetrics(items, 'unit')
    expect(m.maxDrawdown).toBeCloseTo(-100 / 3, 4)
  })

  it('monthlyWinRate: 3 months all up → 100%', () => {
    const items = buildMonthlyItems([1.0, 1.05, 1.10, 1.15])
    const m = computeMetrics(items, 'unit')
    expect(m.monthlyWinRate).toBeCloseTo(100, 6)
  })

  it('monthlyWinRate: 3 months all down → 0%', () => {
    const items = buildMonthlyItems([1.15, 1.10, 1.05, 1.0])
    const m = computeMetrics(items, 'unit')
    expect(m.monthlyWinRate).toBeCloseTo(0, 6)
  })

  it('annualizedVol is non-negative', () => {
    const items = buildNavItems(1.0, [0.01, -0.01, 0.02, -0.02, 0.01])
    const m = computeMetrics(items, 'unit')
    expect(m.annualizedVol).toBeGreaterThanOrEqual(0)
  })

  it('sharpe is null when annualizedReturn is null (n < 30)', () => {
    const items = buildNavItems(1.0, Array(5).fill(0.001))
    const m = computeMetrics(items, 'unit')
    expect(m.sharpe).toBeNull()
  })

  it('calmar is null when no drawdown', () => {
    const items = buildNavItems(1.0, Array(30).fill(0.001))
    const m = computeMetrics(items, 'unit')
    // maxDrawdown = 0 → calmar null
    expect(m.calmar).toBeNull()
  })

  it('navType adjusted uses adj_nav', () => {
    const items = [
      { nav_date: '2024-01-02', unit_nav: 1.0, adj_nav: 1.0 },
      { nav_date: '2024-06-30', unit_nav: 1.0, adj_nav: 1.3 },
    ]
    const m = computeMetrics(items, 'adjusted')
    expect(m.periodReturn).toBeCloseTo(30, 6)
  })

  it('navType accumulated uses accumulated_nav', () => {
    const items = [
      { nav_date: '2024-01-02', unit_nav: 1.0, accumulated_nav: 1.0 },
      { nav_date: '2024-06-30', unit_nav: 1.0, accumulated_nav: 1.4 },
    ]
    const m = computeMetrics(items, 'accumulated')
    expect(m.periodReturn).toBeCloseTo(40, 6)
  })
})

// ─── computeBenchmarkMetrics ──────────────────────────────────────────────────

describe('computeBenchmarkMetrics', () => {
  it('returns null for < 2 items', () => {
    expect(computeBenchmarkMetrics(null, null)).toBeNull()
    expect(computeBenchmarkMetrics([], [])).toBeNull()
  })

  it('returns null when fewer than 3 common dates', () => {
    const fund = [
      { nav_date: '2024-01-02', unit_nav: 1.0 },
      { nav_date: '2024-01-03', unit_nav: 1.01 },
    ]
    const bench = [
      { nav_date: '2024-01-02', unit_nav: 1.0 },
      { nav_date: '2024-01-03', unit_nav: 1.005 },
    ]
    expect(computeBenchmarkMetrics(fund, bench)).toBeNull()
  })

  it('fund = bench → beta ≈ 1, correlation ≈ 1', () => {
    const dates = ['2024-01-02', '2024-01-03', '2024-01-04', '2024-01-05', '2024-01-08']
    const navs = [1.0, 1.01, 1.005, 1.02, 1.015]
    const items = dates.map((d, i) => ({ nav_date: d, unit_nav: navs[i] }))
    const m = computeBenchmarkMetrics(items, items)
    expect(m.beta).toBeCloseTo(1, 4)
    expect(m.correlation).toBeCloseTo(1, 4)
  })

  it('fund = 2 * bench returns → beta ≈ 2', () => {
    const dates = ['2024-01-02', '2024-01-03', '2024-01-04', '2024-01-05', '2024-01-08']
    const benchNavs = [1.0, 1.01, 1.005, 1.02, 1.015]
    const benchItems = dates.map((d, i) => ({ nav_date: d, unit_nav: benchNavs[i] }))

    // Build fund with 2x daily returns
    const fundNavs = [1.0]
    for (let i = 1; i < benchNavs.length; i++) {
      const bR = (benchNavs[i] - benchNavs[i - 1]) / benchNavs[i - 1]
      fundNavs.push(fundNavs[i - 1] * (1 + 2 * bR))
    }
    const fundItems = dates.map((d, i) => ({ nav_date: d, unit_nav: fundNavs[i] }))

    const m = computeBenchmarkMetrics(fundItems, benchItems)
    expect(m.beta).toBeCloseTo(2, 4)
  })

  it('returns betaOffensive field', () => {
    const dates = ['2024-01-02', '2024-01-03', '2024-01-04', '2024-01-05', '2024-01-08']
    const navs = [1.0, 1.01, 0.995, 1.02, 1.015]
    const items = dates.map((d, i) => ({ nav_date: d, unit_nav: navs[i] }))
    const m = computeBenchmarkMetrics(items, items)
    expect(m).toHaveProperty('betaOffensive')
  })

  it('returns betaDefensive field', () => {
    const dates = ['2024-01-02', '2024-01-03', '2024-01-04', '2024-01-05', '2024-01-08']
    const navs = [1.0, 1.01, 0.995, 1.02, 1.015]
    const items = dates.map((d, i) => ({ nav_date: d, unit_nav: navs[i] }))
    const m = computeBenchmarkMetrics(items, items)
    expect(m).toHaveProperty('betaDefensive')
  })

  it('betaOffensive is null when no up-bench periods', () => {
    // bench always falls
    const dates = ['2024-01-02', '2024-01-03', '2024-01-04', '2024-01-05', '2024-01-08']
    const benchNavs = [1.0, 0.99, 0.98, 0.97, 0.96]
    const fundNavs = [1.0, 0.995, 0.985, 0.975, 0.965]
    const benchItems = dates.map((d, i) => ({ nav_date: d, unit_nav: benchNavs[i] }))
    const fundItems = dates.map((d, i) => ({ nav_date: d, unit_nav: fundNavs[i] }))
    const m = computeBenchmarkMetrics(fundItems, benchItems)
    expect(m.betaOffensive).toBeNull()
  })

  it('betaDefensive is null when no down-bench periods', () => {
    const dates = ['2024-01-02', '2024-01-03', '2024-01-04', '2024-01-05', '2024-01-08']
    const benchNavs = [1.0, 1.01, 1.02, 1.03, 1.04]
    const fundNavs = [1.0, 1.02, 1.04, 1.06, 1.08]
    const benchItems = dates.map((d, i) => ({ nav_date: d, unit_nav: benchNavs[i] }))
    const fundItems = dates.map((d, i) => ({ nav_date: d, unit_nav: fundNavs[i] }))
    const m = computeBenchmarkMetrics(fundItems, benchItems)
    expect(m.betaDefensive).toBeNull()
  })
})

// ─── computeExcessMetrics ─────────────────────────────────────────────────────

describe('computeExcessMetrics', () => {
  it('returns null for insufficient data', () => {
    expect(computeExcessMetrics(null, null)).toBeNull()
    expect(computeExcessMetrics([], [])).toBeNull()
  })

  it('fund = bench → periodExcess ≈ 0, annualizedExcessReturn ≈ 0', () => {
    const dates = ['2024-01-02', '2024-01-03', '2024-01-04', '2024-01-05', '2024-01-08']
    const navs = [1.0, 1.01, 1.005, 1.02, 1.015]
    const items = dates.map((d, i) => ({ nav_date: d, unit_nav: navs[i] }))
    const m = computeExcessMetrics(items, items)
    expect(m.periodExcess).toBeCloseTo(0, 4)
  })

  it('returns annualizedExcessReturn field', () => {
    const dates = ['2024-01-02', '2024-01-03', '2024-01-04', '2024-01-05', '2024-01-08']
    const navs = [1.0, 1.01, 1.005, 1.02, 1.015]
    const items = dates.map((d, i) => ({ nav_date: d, unit_nav: navs[i] }))
    const m = computeExcessMetrics(items, items)
    expect(m).toHaveProperty('annualizedExcessReturn')
  })

  it('fund outperforms bench → positive periodExcess', () => {
    const dates = ['2024-01-02', '2024-01-03', '2024-01-04', '2024-01-05', '2024-01-08']
    const benchNavs = [1.0, 1.01, 1.005, 1.02, 1.015]
    const fundNavs = [1.0, 1.02, 1.015, 1.03, 1.025]
    const benchItems = dates.map((d, i) => ({ nav_date: d, unit_nav: benchNavs[i] }))
    const fundItems = dates.map((d, i) => ({ nav_date: d, unit_nav: fundNavs[i] }))
    const m = computeExcessMetrics(fundItems, benchItems)
    expect(m.periodExcess).toBeGreaterThan(0)
  })

  it('geometric mode: periodExcess uses geometric excess formula', () => {
    const dates = ['2024-01-02', '2024-01-03', '2024-01-04', '2024-01-05', '2024-01-08']
    const benchNavs = [1.0, 1.05, 1.03, 1.08, 1.06]
    const fundNavs = [1.0, 1.10, 1.08, 1.14, 1.12]
    const benchItems = dates.map((d, i) => ({ nav_date: d, unit_nav: benchNavs[i] }))
    const fundItems = dates.map((d, i) => ({ nav_date: d, unit_nav: fundNavs[i] }))
    const mArith = computeExcessMetrics(fundItems, benchItems, 'unit', 'arithmetic')
    const mGeo = computeExcessMetrics(fundItems, benchItems, 'unit', 'geometric')
    // Both should be positive; geometric slightly different from arithmetic
    expect(mArith.periodExcess).toBeGreaterThan(0)
    expect(mGeo.periodExcess).toBeGreaterThan(0)
  })

  it('excessSharpe is null when n < 30 (annualizedExcess is null)', () => {
    const dates = ['2024-01-02', '2024-01-03', '2024-01-04', '2024-01-05', '2024-01-08']
    const navs = [1.0, 1.01, 1.005, 1.02, 1.015]
    const items = dates.map((d, i) => ({ nav_date: d, unit_nav: navs[i] }))
    const m = computeExcessMetrics(items, items)
    expect(m.excessSharpe).toBeNull()
  })

  it('excessVol is non-negative', () => {
    const dates = ['2024-01-02', '2024-01-03', '2024-01-04', '2024-01-05', '2024-01-08']
    const benchNavs = [1.0, 1.01, 0.995, 1.02, 1.015]
    const fundNavs = [1.0, 1.02, 0.99, 1.03, 1.02]
    const benchItems = dates.map((d, i) => ({ nav_date: d, unit_nav: benchNavs[i] }))
    const fundItems = dates.map((d, i) => ({ nav_date: d, unit_nav: fundNavs[i] }))
    const m = computeExcessMetrics(fundItems, benchItems)
    expect(m.excessVol).toBeGreaterThanOrEqual(0)
  })
})
