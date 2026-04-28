import {
  RF, ANNUAL_DAYS,
  periodReturn as _periodReturn,
  annualizedReturn as _annualizedReturn,
  annualizedVol as _annualizedVol,
  annualizedDownsideRisk as _annualizedDownsideRisk,
  maxDrawdown as _maxDrawdown,
  sharpe as _sharpe,
  sortino as _sortino,
  calmar as _calmar,
  monthlyWinRate as _monthlyWinRate,
  beta as _beta,
  betaOffensive as _betaOffensive,
  betaDefensive as _betaDefensive,
  alpha as _alpha,
  trackingError as _trackingError,
  informationRatio as _informationRatio,
  correlation as _correlation,
  geometricExcess,
  arithmeticExcess,
  excessAnnualized as _excessAnnualized,
  annualizedVol as _excessVol,
  excessSharpe as _excessSharpe,
  sampleStd,
} from './formulae.js'

function normalizeDate(dateStr) {
  if (typeof dateStr !== 'string') return null
  if (/^\d{8}$/.test(dateStr)) return `${dateStr.slice(0, 4)}-${dateStr.slice(4, 6)}-${dateStr.slice(6, 8)}`
  if (/^\d{4}-\d{2}-\d{2}$/.test(dateStr)) return dateStr
  return null
}

function parseDateSafe(dateStr) {
  const d = normalizeDate(dateStr)
  if (!d) return null
  const parsed = new Date(`${d}T00:00:00`)
  return Number.isNaN(parsed.getTime()) ? null : parsed
}

/**
 * Compute performance metrics for a NAV series.
 * @param {Array} items - Array of {nav_date, unit_nav, adj_nav?, accumulated_nav?}
 * @param {'unit'|'accumulated'|'adjusted'} navType
 * @returns {object|null}
 */
export function computeMetrics(items, navType = 'unit') {
  if (!items || items.length < 2) return null

  const getVal = item => {
    if (navType === 'adjusted') return item.adj_nav ?? item.unit_nav
    if (navType === 'unit') return item.unit_nav
    return item.accumulated_nav ?? item.unit_nav
  }

  const vals = items.map(getVal).filter(v => v != null && !isNaN(v) && isFinite(v))
  if (vals.length < 2) return null

  const dates = items.map(i => normalizeDate(i.nav_date)).filter(Boolean)
  if (dates.length < 2) return null
  const firstDate = parseDateSafe(dates[0])
  const lastDate = parseDateSafe(dates[dates.length - 1])
  if (!firstDate || !lastDate) return null
  const days = Math.max(1, Math.round((lastDate - firstDate) / (1000 * 60 * 60 * 24)))

  const firstVal = vals[0]
  const lastVal = vals[vals.length - 1]
  if (!firstVal || firstVal === 0) return null

  // Period return (decimal → ×100 at end)
  const pRet = _periodReturn(firstVal, lastVal)
  const periodReturn = pRet * 100

  // Annualized return: geometric, using data-point count n
  const n = vals.length
  const annualizedReturn = n >= 30
    ? _annualizedReturn(pRet, n, ANNUAL_DAYS, 'geometric') * 100
    // linear alternative: _annualizedReturn(pRet, n, ANNUAL_DAYS, 'linear') * 100
    : null

  // Daily returns
  const dailyReturns = []
  for (let i = 1; i < vals.length; i++) {
    if (vals[i - 1] > 0) dailyReturns.push((vals[i] - vals[i - 1]) / vals[i - 1])
  }

  // Annualized volatility
  const annualizedVol = dailyReturns.length > 1
    ? _annualizedVol(dailyReturns, ANNUAL_DAYS) * 100
    : null

  // Downside risk
  const downsideRisk = dailyReturns.length > 1
    ? (_annualizedDownsideRisk(dailyReturns, ANNUAL_DAYS) ?? null)
    : null
  const downsideRiskPct = downsideRisk != null ? downsideRisk * 100 : null
  // Skewness, kurtosis, VaR 95%
  let skewness = null
  let kurtosis = null
  let var95 = null
  if (dailyReturns.length > 2) {
    const mean = dailyReturns.reduce((s, v) => s + v, 0) / dailyReturns.length
    const nR = dailyReturns.length
    const std = sampleStd(dailyReturns)
    if (std > 0) {
      const m3 = dailyReturns.reduce((s, v) => s + ((v - mean) / std) ** 3, 0) / nR
      skewness = m3
      const m4 = dailyReturns.reduce((s, v) => s + ((v - mean) / std) ** 4, 0) / nR
      kurtosis = m4 - 3
      var95 = (mean - 1.645 * std) * Math.sqrt(ANNUAL_DAYS) * 100
    }
  }

  // Sharpe (decimal inputs)
  const sharpe = (annualizedVol != null && annualizedVol > 0 && annualizedReturn != null)
    ? _sharpe(annualizedReturn / 100, annualizedVol / 100)
    : null

  // Sortino
  const sortino = (downsideRiskPct != null && downsideRiskPct > 0 && annualizedReturn != null)
    ? _sortino(annualizedReturn / 100, downsideRiskPct / 100)
    : null

  // Max drawdown (returns negative decimal)
  const mdd = _maxDrawdown(vals)
  const maxDrawdown = mdd * 100

  // Max drawdown recovery period
  let maxDDRecoveryDays = null
  let maxDDRecovered = false
  let maxDDPeakIdx = 0
  let maxDDTroughIdx = 0
  {
    let runningPeak = vals[0]
    let peakIdx = 0
    let worstDD = 0
    for (let i = 1; i < vals.length; i++) {
      if (vals[i] > runningPeak) { runningPeak = vals[i]; peakIdx = i }
      const dd = (vals[i] - runningPeak) / runningPeak
      if (dd < worstDD) { worstDD = dd; maxDDPeakIdx = peakIdx; maxDDTroughIdx = i }
    }
    if (worstDD < 0) {
      const peakVal = vals[maxDDPeakIdx]
      const troughDate = parseDateSafe(dates[maxDDTroughIdx])
      for (let i = maxDDTroughIdx + 1; i < vals.length; i++) {
        const currentDate = parseDateSafe(dates[i])
        if (vals[i] >= peakVal && troughDate && currentDate) {
          maxDDRecoveryDays = Math.round((currentDate - troughDate) / (1000 * 60 * 60 * 24))
          maxDDRecovered = true
          break
        }
      }
    }
  }

  // Calmar (decimal inputs)
  const calmar = (mdd < 0 && annualizedReturn != null)
    ? _calmar(annualizedReturn / 100, mdd)
    : null

  // Monthly win rate
  const mwr = _monthlyWinRate(items, getVal)
  const monthlyWinRate = mwr != null ? mwr * 100 : null
  // Longest consecutive no-new-high (calendar days)
  let longestNoNewHigh = 0
  let ath = vals[0]
  let athDate = parseDateSafe(dates[0])
  let noNewHighStartDate = null
  for (let i = 1; i < vals.length; i++) {
    const currentDate = parseDateSafe(dates[i])
    if (!currentDate || !athDate) continue
    if (vals[i] > ath) { ath = vals[i]; athDate = currentDate; noNewHighStartDate = null }
    else {
      if (noNewHighStartDate === null) noNewHighStartDate = athDate
      const elapsed = Math.round((currentDate - noNewHighStartDate) / (1000 * 60 * 60 * 24))
      if (elapsed > longestNoNewHigh) longestNoNewHigh = elapsed
    }
  }

  return {
    periodReturn,
    annualizedReturn,
    annualizedVol,
    downsideRisk: downsideRiskPct,
    sharpe,
    sortino,
    calmar,
    maxDrawdown,
    maxDDRecoveryDays,
    maxDDRecovered,
    monthlyWinRate,
    longestNoNewHigh,
    skewness,
    kurtosis,
    var95,
    days,
  }
}

/**
 * Compute benchmark-relative metrics.
 */
export function computeBenchmarkMetrics(fundItems, benchItems, navType = 'unit') {
  if (!fundItems || !benchItems || fundItems.length < 2 || benchItems.length < 2) return null

  const getVal = item => {
    if (navType === 'adjusted') return item.adj_nav ?? item.unit_nav
    if (navType === 'unit') return item.unit_nav
    return item.accumulated_nav ?? item.unit_nav
  }

  const fundMap = new Map()
  fundItems.forEach(item => {
    const v = getVal(item)
    if (v != null && !isNaN(v) && item.nav_date) fundMap.set(item.nav_date, v)
  })
  const benchMap = new Map()
  benchItems.forEach(item => {
    const v = item.unit_nav ?? item.close
    if (v != null && !isNaN(v) && item.nav_date) benchMap.set(item.nav_date, v)
  })

  const commonDates = [...fundMap.keys()].filter(d => benchMap.has(d)).sort()
  if (commonDates.length < 3) return null

  const fundReturns = []
  const benchReturns = []
  for (let i = 1; i < commonDates.length; i++) {
    const pF = fundMap.get(commonDates[i - 1])
    const cF = fundMap.get(commonDates[i])
    const pB = benchMap.get(commonDates[i - 1])
    const cB = benchMap.get(commonDates[i])
    if (pF > 0 && pB > 0) {
      fundReturns.push((cF - pF) / pF)
      benchReturns.push((cB - pB) / pB)
    }
  }
  if (fundReturns.length < 2) return null

  const corr = _correlation(fundReturns, benchReturns)
  const betaVal = _beta(fundReturns, benchReturns)
  const betaOff = _betaOffensive(fundReturns, benchReturns)
  const betaDef = _betaDefensive(fundReturns, benchReturns)

  const nR = fundReturns.length
  const meanF = fundReturns.reduce((s, v) => s + v, 0) / nR
  const meanB = benchReturns.reduce((s, v) => s + v, 0) / nR
  const annRetF = meanF * ANNUAL_DAYS
  const annRetB = meanB * ANNUAL_DAYS
  const alphaVal = betaVal != null ? _alpha(annRetF, annRetB, betaVal) : null

  const te = _trackingError(fundReturns, benchReturns, ANNUAL_DAYS)
  const excessReturns = fundReturns.map((r, i) => r - benchReturns[i])
  const meanExcess = excessReturns.reduce((s, v) => s + v, 0) / nR
  const annExcessRet = meanExcess * ANNUAL_DAYS
  const ir = _informationRatio(annExcessRet, te)

  return {
    correlation: corr != null ? +corr.toFixed(4) : null,
    beta: betaVal != null ? +betaVal.toFixed(4) : null,
    betaOffensive: betaOff != null ? +betaOff.toFixed(4) : null,
    betaDefensive: betaDef != null ? +betaDef.toFixed(4) : null,
    alpha: alphaVal != null ? +(alphaVal * 100).toFixed(2) : null,
    trackingError: +(te * 100).toFixed(2),
    informationRatio: ir != null ? +ir.toFixed(3) : null,
  }
}

/**
 * Extract Top N drawdown events.
 */
export function computeTopDrawdowns(items, navType = 'unit', count = 5) {
  if (!items || items.length < 2) return []

  const getVal = item => {
    if (navType === 'adjusted') return item.adj_nav ?? item.unit_nav
    if (navType === 'unit') return item.unit_nav
    return item.accumulated_nav ?? item.unit_nav
  }

  const vals = []
  const dates = []
  items.forEach(item => {
    const v = getVal(item)
    const d = normalizeDate(item.nav_date)
    if (v != null && !isNaN(v) && isFinite(v) && d) {
      vals.push(v)
      dates.push(d)
    }
  })
  if (vals.length < 2) return []

  const ddSeries = []
  let peak = vals[0]
  let peakIdx = 0
  for (let i = 0; i < vals.length; i++) {
    if (vals[i] > peak) { peak = vals[i]; peakIdx = i }
    ddSeries.push({ dd: (vals[i] - peak) / peak, peakIdx, idx: i })
  }

  const events = []
  let i = 0
  while (i < ddSeries.length && events.length < count * 3) {
    while (i < ddSeries.length && ddSeries[i].dd >= 0) i++
    if (i >= ddSeries.length) break
    let troughDD = 0, troughIdx = i, eventPeakIdx = ddSeries[i].peakIdx, j = i
    while (j < ddSeries.length && ddSeries[j].dd < 0) {
      if (ddSeries[j].dd < troughDD) { troughDD = ddSeries[j].dd; troughIdx = j }
      j++
    }
    let recoveryDate = null, recoveryDays = null
    if (j < ddSeries.length) {
      recoveryDate = dates[j]
      const troughDate = parseDateSafe(dates[troughIdx])
      const recoveredDate = parseDateSafe(dates[j])
      if (troughDate && recoveredDate) {
        recoveryDays = Math.round((recoveredDate - troughDate) / (1000 * 60 * 60 * 24))
      }
    }
    events.push({ peakDate: dates[eventPeakIdx], troughDate: dates[troughIdx], recoveryDate, drawdown: +(troughDD * 100).toFixed(2), recoveryDays })
    i = j
  }
  events.sort((a, b) => a.drawdown - b.drawdown)
  return events.slice(0, count)
}

/**
 * Compute periodic returns grouped by frequency.
 */
export function computePeriodicReturns(items, navType = 'unit', frequency = 'monthly') {
  if (!items || items.length < 2) return []

  const getVal = item => {
    if (navType === 'adjusted') return item.adj_nav ?? item.unit_nav
    if (navType === 'unit') return item.unit_nav
    return item.accumulated_nav ?? item.unit_nav
  }

  function getPeriodKey(dateStr) {
    const normalized = normalizeDate(dateStr)
    if (!normalized) return null
    const y = normalized.slice(0, 4)
    const m = normalized.slice(5, 7)
    const mNum = parseInt(m)
    switch (frequency) {
      case 'weekly': {
        const d = parseDateSafe(normalized)
        if (!d) return null
        const startOfYear = new Date(d.getFullYear(), 0, 1)
        const week = Math.ceil(((d - startOfYear) / 86400000 + startOfYear.getDay() + 1) / 7)
        return `${y}-W${String(week).padStart(2, '0')}`
      }
      case 'monthly': return `${y}-${m}`
      case 'quarterly': return `${y}-Q${Math.ceil(mNum / 3)}`
      case 'semiannual': return `${y}-H${mNum <= 6 ? 1 : 2}`
      case 'annual': return y
      default: return `${y}-${m}`
    }
  }

  const groups = new Map()
  items.forEach(item => {
    const v = getVal(item)
    if (v == null || isNaN(v) || !item.nav_date) return
    const key = getPeriodKey(item.nav_date)
    if (!key) return
    if (!groups.has(key)) groups.set(key, { first: v, last: v, date: item.nav_date })
    else groups.get(key).last = v
  })

  const result = []
  for (const [period, g] of groups) {
    if (g.first > 0) result.push({ period, return: +((g.last - g.first) / g.first * 100).toFixed(4) })
  }
  return result
}

/**
 * Compute excess return metrics relative to a benchmark.
 */
export function computeExcessMetrics(fundItems, benchItems, navType = 'unit', mode = 'arithmetic') {
  if (!fundItems || !benchItems || fundItems.length < 2 || benchItems.length < 2) return null

  const getVal = item => {
    if (navType === 'adjusted') return item.adj_nav ?? item.unit_nav
    if (navType === 'unit') return item.unit_nav
    return item.accumulated_nav ?? item.unit_nav
  }

  const fundMap = new Map()
  fundItems.forEach(item => {
    const v = getVal(item)
    if (v != null && !isNaN(v) && item.nav_date) fundMap.set(item.nav_date, v)
  })
  const benchMap = new Map()
  benchItems.forEach(item => {
    const v = item.unit_nav ?? item.close
    if (v != null && !isNaN(v) && item.nav_date) benchMap.set(item.nav_date, v)
  })

  const commonDates = [...fundMap.keys()].filter(d => benchMap.has(d)).sort()
  if (commonDates.length < 3) return null

  const excessDaily = []
  const cumSeries = [1]

  for (let i = 1; i < commonDates.length; i++) {
    const pF = fundMap.get(commonDates[i - 1])
    const cF = fundMap.get(commonDates[i])
    const pB = benchMap.get(commonDates[i - 1])
    const cB = benchMap.get(commonDates[i])
    if (pF <= 0 || pB <= 0) continue
    const fR = (cF - pF) / pF
    const bR = (cB - pB) / pB
    const e = mode === 'geometric' ? geometricExcess(fR, bR) : arithmeticExcess(fR, bR)
    excessDaily.push(e)
    cumSeries.push(cumSeries[cumSeries.length - 1] * (1 + e))
  }

  if (excessDaily.length < 2) return null

  const nPts = commonDates.length
  const excessPeriodRet = cumSeries[cumSeries.length - 1] - 1
  const periodExcess = excessPeriodRet * 100

  const annualizedExcess = nPts >= 30
    ? _excessAnnualized(excessPeriodRet, nPts, ANNUAL_DAYS, 'geometric') * 100
    // linear: _excessAnnualized(excessPeriodRet, nPts, ANNUAL_DAYS, 'linear') * 100
    : null

  const excessVol = _excessVol(excessDaily, ANNUAL_DAYS) * 100
  const excessMaxDD = _maxDrawdown(cumSeries) * 100

  const exSharpe = (excessVol > 0 && annualizedExcess != null)
    ? _excessSharpe(annualizedExcess / 100, excessVol / 100)
    : null

  return {
    periodExcess: +periodExcess.toFixed(2),
    annualizedExcess: annualizedExcess != null ? +annualizedExcess.toFixed(2) : null,
    annualizedExcessReturn: annualizedExcess,
    excessVol: +excessVol.toFixed(2),
    excessMaxDD: +excessMaxDD.toFixed(2),
    excessSharpe: exSharpe != null ? +exSharpe.toFixed(3) : null,
  }
}

export function computeAnnualMetrics(items, navType = 'unit') {
  if (!items || items.length < 2) return []

  const yearGroups = new Map()
  items.forEach(item => {
    if (!item.nav_date) return
    const year = item.nav_date.slice(0, 4)
    if (!yearGroups.has(year)) yearGroups.set(year, [])
    yearGroups.get(year).push(item)
  })

  const result = []
  for (const [year, group] of yearGroups) {
    if (group.length < 2) continue
    const metrics = computeMetrics(group, navType)
    if (metrics) result.push({ year, metrics })
  }
  return result
}