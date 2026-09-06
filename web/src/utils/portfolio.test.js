import { describe, it, expect } from 'vitest'
import {
  shouldShowPortfolioEntry,
  buildEqualWeights,
  updateWeights,
  sumWeights,
  buildPortfolioCalculatePayload,
} from './portfolio.js'

describe('portfolio utils', () => {
  it('shows portfolio entry when >=2 selected', () => {
    expect(shouldShowPortfolioEntry(1)).toBe(false)
    expect(shouldShowPortfolioEntry(2)).toBe(true)
  })

  it('builds default equal weights and supports update linkage', () => {
    const base = buildEqualWeights([1, 2])
    expect(base[0].weight).toBe(0.5)
    expect(base[1].weight).toBe(0.5)

    const next = updateWeights(base, 1, 0.6)
    expect(next[0].weight).toBe(0.6)
    expect(next[1].weight).toBe(0.5)
    expect(sumWeights(next)).toBe(1.1)
  })

  it('switches method payload shape', () => {
    const weights = [{ fund_id: 1, weight: 0.5 }, { fund_id: 2, weight: 0.5 }]
    const unified = buildPortfolioCalculatePayload({
      method: 'UNIFIED_START',
      portfolioName: 'P1',
      weights,
      effectiveDate: '2024-01-05',
    })
    expect(unified.constituents[0].target_weight).toBe(0.5)

    const batch = buildPortfolioCalculatePayload({
      method: 'BATCH_INCLUDE',
      portfolioName: 'P2',
      weights,
      effectiveDate: '2024-01-05',
    })
    expect(batch.constituents[0].target_amount).toBe(50)
  })
})
