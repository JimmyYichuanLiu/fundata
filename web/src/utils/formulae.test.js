/**
 * formulae.test.js — Unit tests for formulae.js
 *
 * All expected values are hand-calculated or derived from first principles.
 * Tolerance: toBeCloseTo(value, 6) unless noted.
 */
import { describe, it, expect } from 'vitest'
import {
  RF,
  ANNUAL_DAYS,
  ANNUAL_WEEKS,
  periodReturn,
  annualizedReturn,
  sampleStd,
  annualizedVol,
  annualizedDownsideRisk,
  maxDrawdown,
  beta,
  betaOffensive,
  betaDefensive,
  alpha,
  sharpe,
  sortino,
  calmar,
  arithmeticExcess,
  geometricExcess,
  excessAnnualized,
  excessSharpe,
  monthlyWinRate,
} from './formulae.js'

// ─── Constants ────────────────────────────────────────────────────────────────

describe('constants', () => {
  it('RF is 0.0175', () => {
    expect(RF).toBe(0.0175)
  })

  it('ANNUAL_DAYS is 250', () => {
    expect(ANNUAL_DAYS).toBe(250)
  })

  it('ANNUAL_WEEKS is 50', () => {
    expect(ANNUAL_WEEKS).toBe(50)
  })
})

// ─── periodReturn ─────────────────────────────────────────────────────────────

describe('periodReturn', () => {
  it('basic: 1.0 → 1.2 = 0.2', () => {
    expect(periodReturn(1.0, 1.2)).toBeCloseTo(0.2, 10)
  })

  it('loss: 1.0 → 0.8 = -0.2', () => {
    expect(periodReturn(1.0, 0.8)).toBeCloseTo(-0.2, 10)
  })

  it('no change: 1.0 → 1.0 = 0', () => {
    expect(periodReturn(1.0, 1.0)).toBe(0)
  })

  it('startNav = 0 returns 0', () => {
    expect(periodReturn(0, 1.5)).toBe(0)
  })

  it('startNav = null returns 0', () => {
    expect(periodReturn(null, 1.5)).toBe(0)
  })
})

// ─── annualizedReturn ─────────────────────────────────────────────────────────

describe('annualizedReturn', () => {
  it('geometric: 25% over 251 daily points = 25% annualized', () => {
    // n=251 → exponent = 250/250 = 1 → (1.25)^1 - 1 = 0.25
    expect(annualizedReturn(0.25, 251, 250, 'geometric')).toBeCloseTo(0.25, 10)
  })

  it('geometric: 0% return = 0', () => {
    expect(annualizedReturn(0, 251, 250, 'geometric')).toBeCloseTo(0, 10)
  })

  it('geometric: n=1 returns 0', () => {
    expect(annualizedReturn(0.1, 1, 250, 'geometric')).toBe(0)
  })

  it('linear: 10% over 51 weekly points = 10% annualized', () => {
    // 0.10 / 50 * 50 = 0.10
    expect(annualizedReturn(0.10, 51, 50, 'linear')).toBeCloseTo(0.10, 10)
  })

  it('linear: n=1 returns 0', () => {
    expect(annualizedReturn(0.1, 1, 50, 'linear')).toBe(0)
  })

  it('geometric default mode', () => {
    const r = annualizedReturn(0.25, 251, 250)
    expect(r).toBeCloseTo(0.25, 10)
  })
})

// ─── sampleStd ────────────────────────────────────────────────────────────────

describe('sampleStd', () => {
  it('[2, 4, 4, 4, 5, 5, 7, 9] → sample std = sqrt(32/7)', () => {
    // mean=5, sum_sq_dev=32, sample variance=32/7, sample std=sqrt(32/7)≈2.1381
    // Note: population std=2 (÷n), but sampleStd divides by n-1
    expect(sampleStd([2, 4, 4, 4, 5, 5, 7, 9])).toBeCloseTo(Math.sqrt(32 / 7), 8)
  })

  it('two identical values → 0', () => {
    expect(sampleStd([3, 3])).toBe(0)
  })

  it('empty array → 0', () => {
    expect(sampleStd([])).toBe(0)
  })

  it('single element → 0', () => {
    expect(sampleStd([5])).toBe(0)
  })

  it('null → 0', () => {
    expect(sampleStd(null)).toBe(0)
  })

  it('[0, 1] → std = sqrt(0.5) ≈ 0.7071', () => {
    expect(sampleStd([0, 1])).toBeCloseTo(Math.sqrt(0.5), 10)
  })
})

// ─── annualizedVol ────────────────────────────────────────────────────────────

describe('annualizedVol', () => {
  it('daily returns [0, 1] → sampleStd * sqrt(250)', () => {
    const std = Math.sqrt(0.5)
    expect(annualizedVol([0, 1], 250)).toBeCloseTo(std * Math.sqrt(250), 8)
  })

  it('all-zero returns → 0', () => {
    expect(annualizedVol([0, 0, 0], 250)).toBe(0)
  })
})

// ─── annualizedDownsideRisk ───────────────────────────────────────────────────

describe('annualizedDownsideRisk', () => {
  it('no negative returns → null', () => {
    expect(annualizedDownsideRisk([0.01, 0.02, 0.03], 250)).toBeNull()
  })

  it('only one negative return → null (need ≥2)', () => {
    expect(annualizedDownsideRisk([0.01, -0.02, 0.03], 250)).toBeNull()
  })

  it('two negative returns → sampleStd(negatives) * sqrt(250)', () => {
    const neg = [-0.01, -0.03]
    const expected = sampleStd(neg) * Math.sqrt(250)
    expect(annualizedDownsideRisk([0.02, -0.01, 0.05, -0.03], 250)).toBeCloseTo(expected, 8)
  })
})

// ─── maxDrawdown ──────────────────────────────────────────────────────────────

describe('maxDrawdown', () => {
  it('monotone rise → 0', () => {
    expect(maxDrawdown([1, 1.1, 1.2, 1.3])).toBe(0)
  })

  it('single drop: [1, 0.8] → -0.2', () => {
    expect(maxDrawdown([1, 0.8])).toBeCloseTo(-0.2, 10)
  })

  it('peak then valley: [1, 1.5, 1.0] → -1/3', () => {
    // (1.0 - 1.5) / 1.5 = -0.3333...
    expect(maxDrawdown([1, 1.5, 1.0])).toBeCloseTo(-1 / 3, 8)
  })

  it('multiple drawdowns picks worst: [1, 2, 1.5, 3, 1] → -2/3', () => {
    // peak=3, trough=1 → (1-3)/3 = -0.6667
    expect(maxDrawdown([1, 2, 1.5, 3, 1])).toBeCloseTo(-2 / 3, 8)
  })

  it('empty → 0', () => {
    expect(maxDrawdown([])).toBe(0)
  })

  it('single element → 0', () => {
    expect(maxDrawdown([1])).toBe(0)
  })
})

// ─── beta ─────────────────────────────────────────────────────────────────────

describe('beta', () => {
  it('fund = bench → beta = 1', () => {
    const r = [0.01, -0.02, 0.03, -0.01, 0.02]
    expect(beta(r, r)).toBeCloseTo(1, 8)
  })

  it('fund = 2 * bench → beta = 2', () => {
    const b = [0.01, -0.02, 0.03, -0.01, 0.02]
    const f = b.map(v => v * 2)
    expect(beta(f, b)).toBeCloseTo(2, 8)
  })

  it('fund = -bench → beta = -1', () => {
    const b = [0.01, -0.02, 0.03, -0.01, 0.02]
    const f = b.map(v => -v)
    expect(beta(f, b)).toBeCloseTo(-1, 8)
  })

  it('n < 2 → null', () => {
    expect(beta([0.01], [0.01])).toBeNull()
  })

  it('bench constant (zero variance) → null', () => {
    expect(beta([0.01, 0.02, 0.03], [0.01, 0.01, 0.01])).toBeNull()
  })
})

// ─── betaOffensive / betaDefensive ───────────────────────────────────────────

describe('betaOffensive', () => {
  it('only up-bench periods used', () => {
    // bench: [+0.02, -0.01, +0.03], fund: [+0.04, -0.02, +0.06]
    // offensive periods: i=0 (bench=0.02, fund=0.04), i=2 (bench=0.03, fund=0.06)
    // fund = 2 * bench in both → beta = 2
    const bench = [0.02, -0.01, 0.03]
    const fund = [0.04, -0.02, 0.06]
    expect(betaOffensive(fund, bench)).toBeCloseTo(2, 6)
  })

  it('no up-bench periods → null', () => {
    expect(betaOffensive([0.01, 0.02], [-0.01, -0.02])).toBeNull()
  })
})

describe('betaDefensive', () => {
  it('only down-bench periods used', () => {
    const bench = [0.02, -0.01, -0.03]
    const fund = [0.04, -0.02, -0.06]
    // defensive: i=1 (bench=-0.01, fund=-0.02), i=2 (bench=-0.03, fund=-0.06)
    // fund = 2 * bench → beta = 2
    expect(betaDefensive(fund, bench)).toBeCloseTo(2, 6)
  })

  it('no down-bench periods → null', () => {
    expect(betaDefensive([0.01, 0.02], [0.01, 0.02])).toBeNull()
  })
})

// ─── alpha ────────────────────────────────────────────────────────────────────

describe('alpha', () => {
  it('CAPM: fundAnn=0.10, benchAnn=0.08, beta=1 → 0.10 - 0.0175 - 1*(0.08-0.0175)', () => {
    // = 0.10 - 0.0175 - 0.0625 = 0.02
    expect(alpha(0.10, 0.08, 1)).toBeCloseTo(0.02, 10)
  })

  it('beta=0 → alpha = fundAnn - RF', () => {
    expect(alpha(0.10, 0.08, 0)).toBeCloseTo(0.10 - RF, 10)
  })

  it('fund = bench, beta = 1 → alpha = 0', () => {
    expect(alpha(0.08, 0.08, 1)).toBeCloseTo(0, 10)
  })
})

// ─── sharpe ───────────────────────────────────────────────────────────────────

describe('sharpe', () => {
  it('(0.10 - 0.0175) / 0.15 ≈ 0.55', () => {
    expect(sharpe(0.10, 0.15)).toBeCloseTo((0.10 - 0.0175) / 0.15, 8)
  })

  it('annVol = 0 → null', () => {
    expect(sharpe(0.10, 0)).toBeNull()
  })

  it('annVol = null → null', () => {
    expect(sharpe(0.10, null)).toBeNull()
  })

  it('negative excess return gives negative sharpe', () => {
    expect(sharpe(0.01, 0.15)).toBeLessThan(0)
  })
})

// ─── sortino ──────────────────────────────────────────────────────────────────

describe('sortino', () => {
  it('(0.10 - 0.0175) / 0.08 ≈ 1.03125', () => {
    expect(sortino(0.10, 0.08)).toBeCloseTo((0.10 - 0.0175) / 0.08, 8)
  })

  it('downsideRisk = 0 → null', () => {
    expect(sortino(0.10, 0)).toBeNull()
  })

  it('downsideRisk = null → null', () => {
    expect(sortino(0.10, null)).toBeNull()
  })
})

// ─── calmar ───────────────────────────────────────────────────────────────────

describe('calmar', () => {
  it('0.20 / 0.10 = 2 (maxDD = -0.10)', () => {
    expect(calmar(0.20, -0.10)).toBeCloseTo(2, 10)
  })

  it('maxDD = 0 → null', () => {
    expect(calmar(0.20, 0)).toBeNull()
  })

  it('maxDD = null → null', () => {
    expect(calmar(0.20, null)).toBeNull()
  })

  it('maxDD positive (invalid) → null', () => {
    expect(calmar(0.20, 0.05)).toBeNull()
  })
})

// ─── arithmeticExcess / geometricExcess ──────────────────────────────────────

describe('arithmeticExcess', () => {
  it('0.10 - 0.06 = 0.04', () => {
    expect(arithmeticExcess(0.10, 0.06)).toBeCloseTo(0.04, 10)
  })

  it('negative excess', () => {
    expect(arithmeticExcess(0.03, 0.06)).toBeCloseTo(-0.03, 10)
  })
})

describe('geometricExcess', () => {
  it('(1.10 / 1.06) - 1 ≈ 0.03774', () => {
    expect(geometricExcess(0.10, 0.06)).toBeCloseTo(1.10 / 1.06 - 1, 8)
  })

  it('bench = -1 (denominator = 0) → 0', () => {
    expect(geometricExcess(0.10, -1)).toBe(0)
  })

  it('fund = bench → 0', () => {
    expect(geometricExcess(0.05, 0.05)).toBeCloseTo(0, 10)
  })
})

// ─── excessAnnualized ────────────────────────────────────────────────────────

describe('excessAnnualized', () => {
  it('geometric: 10% over 251 points = 10% annualized', () => {
    expect(excessAnnualized(0.10, 251, 250, 'geometric')).toBeCloseTo(0.10, 10)
  })

  it('linear: 10% over 51 weekly points = 10% annualized', () => {
    expect(excessAnnualized(0.10, 51, 50, 'linear')).toBeCloseTo(0.10, 10)
  })

  it('n=1 → 0', () => {
    expect(excessAnnualized(0.10, 1, 250, 'geometric')).toBe(0)
  })
})

// ─── excessSharpe ─────────────────────────────────────────────────────────────

describe('excessSharpe', () => {
  it('0.05 / 0.10 = 0.5', () => {
    expect(excessSharpe(0.05, 0.10)).toBeCloseTo(0.5, 10)
  })

  it('excessVol = 0 → null', () => {
    expect(excessSharpe(0.05, 0)).toBeNull()
  })

  it('excessVol = null → null', () => {
    expect(excessSharpe(0.05, null)).toBeNull()
  })
})

// ─── monthlyWinRate ───────────────────────────────────────────────────────────

describe('monthlyWinRate', () => {
  const getNav = item => item.nav

  it('3 months all up → 1.0', () => {
    const items = [
      { nav_date: '2024-01-31', nav: 1.0 },
      { nav_date: '2024-02-29', nav: 1.05 },
      { nav_date: '2024-03-31', nav: 1.10 },
    ]
    expect(monthlyWinRate(items, getNav)).toBeCloseTo(1.0, 10)
  })

  it('3 months all down → 0', () => {
    const items = [
      { nav_date: '2024-01-31', nav: 1.10 },
      { nav_date: '2024-02-29', nav: 1.05 },
      { nav_date: '2024-03-31', nav: 1.00 },
    ]
    expect(monthlyWinRate(items, getNav)).toBeCloseTo(0, 10)
  })

  it('2 up 1 down → 2/3', () => {
    // Jan→Feb up, Feb→Mar up, Mar→Apr down
    const items = [
      { nav_date: '2024-01-31', nav: 1.00 },
      { nav_date: '2024-02-29', nav: 1.05 },
      { nav_date: '2024-03-31', nav: 1.10 },
      { nav_date: '2024-04-30', nav: 1.08 },
    ]
    expect(monthlyWinRate(items, getNav)).toBeCloseTo(2 / 3, 8)
  })

  it('multiple entries per month: last entry wins', () => {
    // Jan ends at 1.05 (last entry), Feb ends at 1.03 → down
    const items = [
      { nav_date: '2024-01-15', nav: 1.10 },
      { nav_date: '2024-01-31', nav: 1.05 },
      { nav_date: '2024-02-28', nav: 1.03 },
    ]
    expect(monthlyWinRate(items, getNav)).toBeCloseTo(0, 10)
  })

  it('single item → null', () => {
    expect(monthlyWinRate([{ nav_date: '2024-01-31', nav: 1.0 }], getNav)).toBeNull()
  })

  it('null input → null', () => {
    expect(monthlyWinRate(null, getNav)).toBeNull()
  })

  it('items missing nav_date are skipped', () => {
    const items = [
      { nav_date: '2024-01-31', nav: 1.0 },
      { nav: 1.05 },  // no nav_date
      { nav_date: '2024-02-29', nav: 1.10 },
    ]
    expect(monthlyWinRate(items, getNav)).toBeCloseTo(1.0, 10)
  })
})
