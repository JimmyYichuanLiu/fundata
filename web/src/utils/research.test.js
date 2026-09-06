import { describe, it, expect } from 'vitest'
import { parseFundIds, fundSelectionSearch } from './selection.js'
import { validatePortfolio, buildEqualWeights, buildPortfolioCalculatePayload } from './portfolio.js'
import { computeMetrics, computePeriodicReturns } from './metrics.js'
import { alignComparisonSeries } from './series.js'
import { formatTimestamp } from './display.js'

describe('shareable compare selection', () => {
  it('restores repeated and comma-separated ids, removes invalid values and duplicates', () => {
    expect(parseFundIds('?fund_ids=3,4&fund_ids=3&fund_ids=-1&fund_ids=NaN&fund_ids=0&fund_ids=2.5')).toEqual([3, 4])
  })
  it('limits restored selection to eight and roundtrips share URLs', () => {
    const ids = parseFundIds('?fund_ids=1,2,3,4,5,6,7,8,9')
    expect(ids).toHaveLength(8)
    expect(parseFundIds(fundSelectionSearch(ids.map(fund_id => ({ fund_id }))))).toEqual(ids)
  })
})
describe('portfolio validation prevents invalid saves', () => {
  const input = { portfolioName: '研究组合', method: 'UNIFIED_START', weights: buildEqualWeights([1, 2]), effectiveDate: '2026-02-01', dates: { 1: { first: '2026-01-01', last: '2026-04-01' }, 2: { first: '2026-02-01', last: '2026-05-01' } } }
  it('accepts equal percentages on the common date', () => expect(validatePortfolio(input)).toBe(''))
  it.each([NaN, Infinity, -0.1, 1.1])('rejects invalid weight %s', weight => expect(validatePortfolio({ ...input, weights: [{ fund_id: 1, weight }, { fund_id: 2, weight: 0.5 }] })).not.toBe(''))
  it('rejects non-100 totals and missing dates', () => {
    expect(validatePortfolio({ ...input, weights: [{ fund_id: 1, weight: 0.4 }, { fund_id: 2, weight: 0.5 }] })).toContain('100%')
    expect(validatePortfolio({ ...input, effectiveDate: '' })).not.toBe('')
  })
  it('rejects dates before the common window and no overlap', () => {
    expect(validatePortfolio({ ...input, effectiveDate: '2026-01-01' })).toContain('公共区间')
    expect(validatePortfolio({ ...input, dates: { ...input.dates, 2: { first: '2026-06-01', last: '2026-07-01' } } })).toContain('没有公共')
  })
  it('batch inclusion honors later first NAV dates', () => {
    const result = buildPortfolioCalculatePayload({ ...input, method: 'BATCH_INCLUDE', effectiveDate: '2026-01-01' })
    expect(result.constituents[0].effective_date).toBe('2026-01-01')
    expect(result.constituents[1].effective_date).toBe('2026-02-01')
    expect(result.constituents[1].target_amount).toBe(50)
  })
})
describe('NAV mode integrity', () => {
  it('preserves explicit missing NAV gaps and never extends beyond the fund end', () => {
    const values = alignComparisonSeries([{ nav_date: '2026-01-01', unit_nav: 2 }, { nav_date: '2026-01-02', unit_nav: null }, { nav_date: '2026-01-04', unit_nav: 2.2 }], ['2026-01-01', '2026-01-02', '2026-01-03', '2026-01-04', '2026-01-05'])
    expect(values).toEqual([1, null, null, 1.1, null])
  })
  it('normalizes at first valid observation and supports absolute values', () => {
    const items = [{ nav_date: '2026-01-01', unit_nav: null }, { nav_date: '2026-01-02', unit_nav: 2 }]
    expect(alignComparisonSeries(items, items.map(i => i.nav_date))).toEqual([null, 1])
    expect(alignComparisonSeries(items, items.map(i => i.nav_date), true)).toEqual([null, 2])
  })
  it('displays timezone-aware sync timestamps in Beijing time', () => expect(formatTimestamp('2026-09-04T10:00:00+00:00')).toBe('2026-09-04 18:00:00'))
  it('reports missing periodic return when only one selected-type NAV is available', () => {
    expect(computePeriodicReturns([{ nav_date: '2026-07-01', unit_nav: 1, adj_nav: null }, { nav_date: '2026-07-31', unit_nav: 1.1, adj_nav: 1.1 }], 'adjusted', 'monthly')[0].return).toBeNull()
  })
  it('never falls back to unit NAV when adjusted values are missing', () => {
    const items = [{ nav_date: '2026-01-01', unit_nav: 10, adj_nav: null }, { nav_date: '2026-02-01', unit_nav: 20, adj_nav: 1 }, { nav_date: '2026-03-01', unit_nav: 30, adj_nav: 1.1 }]
    expect(computeMetrics(items, 'adjusted').periodReturn).toBeCloseTo(10)
    expect(computeMetrics(items.slice(0, 2), 'adjusted')).toBeNull()
  })
  it('return mode uses unit NAV consistently', () => {
    const items = [{ nav_date: '2026-01-01', unit_nav: 1 }, { nav_date: '2026-02-01', unit_nav: 1.2 }]
    expect(computeMetrics(items, 'return').periodReturn).toBeCloseTo(20)
  })
})
