/**
 * formulae.js — Pure formula library for fund performance metrics.
 *
 * Conventions:
 *   - 1 year = 250 trading days = 50 weeks
 *   - All inputs/outputs in decimal (0.05 = 5%), never percentage
 *   - NAV type: adj_nav preferred, fallback unit_nav
 *   - Risk-free rate RF = 1.75% annualized
 */

/** @type {number} Annualized risk-free rate (decimal) */
export const RF = 0.0175

/** @type {number} Trading days per year */
export const ANNUAL_DAYS = 250

/** @type {number} Trading weeks per year */
export const ANNUAL_WEEKS = 50

/**
 * Period return = endNav / startNav - 1
 * @param {number} startNav
 * @param {number} endNav
 * @returns {number} decimal
 */
export function periodReturn(startNav, endNav) {
  if (!startNav || startNav === 0) return 0
  return endNav / startNav - 1
}

/**
 * Annualized return.
 * geometric: (1 + periodRet)^(annualFactor / (n-1)) - 1
 * linear:    periodRet / (n-1) * annualFactor
 * @param {number} periodRet - decimal
 * @param {number} n - data point count (trading days or weeks)
 * @param {number} annualFactor - 250 (daily) or 50 (weekly)
 * @param {'geometric'|'linear'} mode
 * @returns {number} decimal
 */
export function annualizedReturn(periodRet, n, annualFactor, mode = 'geometric') {
  if (n <= 1) return 0
  if (mode === 'linear') return periodRet / (n - 1) * annualFactor
  return Math.pow(1 + periodRet, annualFactor / (n - 1)) - 1
}

/**
 * Sample standard deviation (STD.S, divides by n-1).
 * @param {number[]} arr
 * @returns {number} decimal
 */
export function sampleStd(arr) {
  if (!arr || arr.length < 2) return 0
  const n = arr.length
  const mean = arr.reduce((s, v) => s + v, 0) / n
  const variance = arr.reduce((s, v) => s + (v - mean) ** 2, 0) / (n - 1)
  return Math.sqrt(variance)
}

/**
 * Annualized volatility = sampleStd(returns) * sqrt(annualFactor).
 * @param {number[]} returns - array of period returns (decimal)
 * @param {number} annualFactor - 250 or 50
 * @returns {number} decimal
 */
export function annualizedVol(returns, annualFactor) {
  return sampleStd(returns) * Math.sqrt(annualFactor)
}

/**
 * Annualized downside risk = sampleStd(negative returns) * sqrt(annualFactor).
 * @param {number[]} returns
 * @param {number} annualFactor
 * @returns {number|null} decimal, or null if no negative returns
 */
export function annualizedDownsideRisk(returns, annualFactor) {
  const neg = returns.filter(r => r < 0)
  if (neg.length < 2) return null
  return sampleStd(neg) * Math.sqrt(annualFactor)
}

/**
 * Maximum drawdown from a NAV series.
 * @param {number[]} navSeries - ascending time order
 * @returns {number} negative decimal (e.g. -0.15 = -15%), 0 if no drawdown
 */
export function maxDrawdown(navSeries) {
  if (!navSeries || navSeries.length < 2) return 0
  let peak = navSeries[0]
  let mdd = 0
  for (let i = 1; i < navSeries.length; i++) {
    if (navSeries[i] > peak) peak = navSeries[i]
    const dd = (navSeries[i] - peak) / peak
    if (dd < mdd) mdd = dd
  }
  return mdd
}

/**
 * Beta = Cov(fund, bench) / Var(bench), sample covariance (n-1).
 * @param {number[]} fundReturns
 * @param {number[]} benchReturns
 * @returns {number|null}
 */
export function beta(fundReturns, benchReturns) {
  const n = Math.min(fundReturns.length, benchReturns.length)
  if (n < 2) return null
  const meanF = fundReturns.slice(0, n).reduce((s, v) => s + v, 0) / n
  const meanB = benchReturns.slice(0, n).reduce((s, v) => s + v, 0) / n
  let covFB = 0, varB = 0
  for (let i = 0; i < n; i++) {
    const df = fundReturns[i] - meanF
    const db = benchReturns[i] - meanB
    covFB += df * db
    varB += db * db
  }
  covFB /= (n - 1)
  varB /= (n - 1)
  return varB > 0 ? covFB / varB : null
}

/**
 * Offensive beta: only periods where benchReturns[i] > 0.
 * @param {number[]} fundReturns
 * @param {number[]} benchReturns
 * @returns {number|null}
 */
export function betaOffensive(fundReturns, benchReturns) {
  const fR = [], bR = []
  const n = Math.min(fundReturns.length, benchReturns.length)
  for (let i = 0; i < n; i++) {
    if (benchReturns[i] > 0) { fR.push(fundReturns[i]); bR.push(benchReturns[i]) }
  }
  return beta(fR, bR)
}

/**
 * Defensive beta: only periods where benchReturns[i] < 0.
 * @param {number[]} fundReturns
 * @param {number[]} benchReturns
 * @returns {number|null}
 */
export function betaDefensive(fundReturns, benchReturns) {
  const fR = [], bR = []
  const n = Math.min(fundReturns.length, benchReturns.length)
  for (let i = 0; i < n; i++) {
    if (benchReturns[i] < 0) { fR.push(fundReturns[i]); bR.push(benchReturns[i]) }
  }
  return beta(fR, bR)
}

/**
 * CAPM Alpha = fundAnnRet - RF - betaVal * (benchAnnRet - RF).
 * @param {number} fundAnnRet - decimal
 * @param {number} benchAnnRet - decimal
 * @param {number} betaVal
 * @returns {number} decimal
 */
export function alpha(fundAnnRet, benchAnnRet, betaVal) {
  return fundAnnRet - RF - betaVal * (benchAnnRet - RF)
}

/**
 * Sharpe = (annRet - RF) / annVol.
 * @param {number} annRet - decimal
 * @param {number} annVol - decimal
 * @returns {number|null}
 */
export function sharpe(annRet, annVol) {
  if (annVol == null || annVol <= 0) return null
  return (annRet - RF) / annVol
}

/**
 * Sortino = (annRet - RF) / annDownsideRisk.
 * @param {number} annRet - decimal
 * @param {number} annDownsideRisk - decimal
 * @returns {number|null}
 */
export function sortino(annRet, annDownsideRisk) {
  if (annDownsideRisk == null || annDownsideRisk <= 0) return null
  return (annRet - RF) / annDownsideRisk
}

/**
 * Calmar = annRet / |maxDD|. maxDD is negative.
 * @param {number} annRet - decimal
 * @param {number} maxDD - negative decimal
 * @returns {number|null}
 */
export function calmar(annRet, maxDD) {
  if (maxDD == null || maxDD >= 0) return null
  return annRet / Math.abs(maxDD)
}

/**
 * Tracking error = sampleStd(excess returns) * sqrt(annualFactor).
 * @param {number[]} fundReturns
 * @param {number[]} benchReturns
 * @param {number} annualFactor
 * @returns {number} decimal
 */
export function trackingError(fundReturns, benchReturns, annualFactor) {
  const n = Math.min(fundReturns.length, benchReturns.length)
  const excess = []
  for (let i = 0; i < n; i++) excess.push(fundReturns[i] - benchReturns[i])
  return sampleStd(excess) * Math.sqrt(annualFactor)
}

/**
 * Information ratio = annualizedExcessRet / trackingErr.
 * @param {number} annualizedExcessRet - decimal
 * @param {number} trackingErr - decimal
 * @returns {number|null}
 */
export function informationRatio(annualizedExcessRet, trackingErr) {
  if (trackingErr == null || trackingErr <= 0) return null
  return annualizedExcessRet / trackingErr
}

/**
 * Correlation = Cov(fund, bench) / (STD(fund) * STD(bench)).
 * @param {number[]} fundReturns
 * @param {number[]} benchReturns
 * @returns {number|null} [-1, 1]
 */
export function correlation(fundReturns, benchReturns) {
  const n = Math.min(fundReturns.length, benchReturns.length)
  if (n < 2) return null
  const meanF = fundReturns.slice(0, n).reduce((s, v) => s + v, 0) / n
  const meanB = benchReturns.slice(0, n).reduce((s, v) => s + v, 0) / n
  let covFB = 0, varF = 0, varB = 0
  for (let i = 0; i < n; i++) {
    const df = fundReturns[i] - meanF
    const db = benchReturns[i] - meanB
    covFB += df * db
    varF += df * df
    varB += db * db
  }
  if (varF <= 0 || varB <= 0) return null
  return covFB / Math.sqrt(varF * varB)
}

/**
 * Arithmetic excess = fundPeriodRet - benchPeriodRet.
 * @param {number} fundPeriodRet - decimal
 * @param {number} benchPeriodRet - decimal
 * @returns {number}
 */
export function arithmeticExcess(fundPeriodRet, benchPeriodRet) {
  return fundPeriodRet - benchPeriodRet
}

/**
 * Geometric excess = (1 + fundPeriodRet) / (1 + benchPeriodRet) - 1.
 * @param {number} fundPeriodRet - decimal
 * @param {number} benchPeriodRet - decimal
 * @returns {number}
 */
export function geometricExcess(fundPeriodRet, benchPeriodRet) {
  if (1 + benchPeriodRet === 0) return 0
  return (1 + fundPeriodRet) / (1 + benchPeriodRet) - 1
}

/**
 * Annualized excess return (same structure as annualizedReturn).
 * @param {number} excessPeriodRet - decimal
 * @param {number} n - data points
 * @param {number} annualFactor
 * @param {'geometric'|'linear'} mode
 * @returns {number} decimal
 */
export function excessAnnualized(excessPeriodRet, n, annualFactor, mode = 'geometric') {
  if (n <= 1) return 0
  if (mode === 'linear') return excessPeriodRet / (n - 1) * annualFactor
  return Math.pow(1 + excessPeriodRet, annualFactor / (n - 1)) - 1
}

/**
 * Excess Sharpe = annualizedExcessRet / excessVol (no RF deduction).
 * @param {number} annualizedExcessRet - decimal
 * @param {number} excessVol - decimal
 * @returns {number|null}
 */
export function excessSharpe(annualizedExcessRet, excessVol) {
  if (excessVol == null || excessVol <= 0) return null
  return annualizedExcessRet / excessVol
}

/**
 * Monthly win rate: fraction of months with positive return.
 * Uses last trading day of each month for NAV.
 * @param {Array} navItems - [{nav_date: 'YYYY-MM-DD', ...}]
 * @param {function} getNavFn - item => number (adj_nav value)
 * @returns {number|null} [0, 1] decimal, or null
 */
export function monthlyWinRate(navItems, getNavFn) {
  if (!navItems || navItems.length < 2) return null
  // Group by YYYY-MM, keep last entry per month
  const monthEnd = new Map()
  navItems.forEach(item => {
    if (!item.nav_date) return
    const v = getNavFn(item)
    if (v == null || isNaN(v)) return
    const month = item.nav_date.slice(0, 7)
    monthEnd.set(month, v) // last entry wins (data is sorted ascending)
  })
  const months = [...monthEnd.keys()].sort()
  if (months.length < 2) return null
  let wins = 0
  let total = 0
  for (let i = 1; i < months.length; i++) {
    const prev = monthEnd.get(months[i - 1])
    const curr = monthEnd.get(months[i])
    if (prev > 0) {
      total++
      if (curr / prev - 1 > 0) wins++
    }
  }
  return total > 0 ? wins / total : null
}
