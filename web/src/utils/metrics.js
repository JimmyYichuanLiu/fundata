/**
 * Compute performance metrics for a NAV series.
 * @param {Array} items - Array of {nav_date: 'YYYY-MM-DD', unit_nav, accumulated_nav?}
 * @param {'unit'|'accumulated'} navType
 * @returns {object|null} metrics object, or null if insufficient data
 */
export function computeMetrics(items, navType = 'unit') {
  if (!items || items.length < 2) return null

  const getVal = item => {
    if (navType === 'adjusted') return item.adjusted_nav ?? item.unit_nav
    if (navType === 'unit') return item.unit_nav
    return item.accumulated_nav ?? item.unit_nav
  }

  const vals = items.map(getVal).filter(v => v != null && !isNaN(v) && isFinite(v))
  if (vals.length < 2) return null

  const dates = items.map(i => i.nav_date).filter(Boolean)
  if (dates.length < 2) return null

  const firstDate = new Date(dates[0])
  const lastDate = new Date(dates[dates.length - 1])
  const days = Math.max(1, Math.round((lastDate - firstDate) / (1000 * 60 * 60 * 24)))

  // Period return
  const firstVal = vals[0]
  const lastVal = vals[vals.length - 1]
  if (!firstVal || firstVal === 0) return null
  const periodReturn = (lastVal - firstVal) / firstVal * 100

  // Annualized return (only meaningful when >= 30 days)
  const annualizedReturn = days >= 30
    ? (Math.pow(1 + periodReturn / 100, 365 / days) - 1) * 100
    : null

  // Daily returns
  const dailyReturns = []
  for (let i = 1; i < vals.length; i++) {
    if (vals[i - 1] > 0) {
      dailyReturns.push((vals[i] - vals[i - 1]) / vals[i - 1])
    }
  }

  // Annualized volatility (daily std dev × √250)
  let annualizedVol = null
  if (dailyReturns.length > 1) {
    const mean = dailyReturns.reduce((s, v) => s + v, 0) / dailyReturns.length
    const variance = dailyReturns.reduce((s, v) => s + (v - mean) ** 2, 0) / (dailyReturns.length - 1)
    annualizedVol = Math.sqrt(variance * 250) * 100
  }

  // Downside risk (negative-only returns std dev × √250)
  let downsideRisk = null
  const negReturns = dailyReturns.filter(r => r < 0)
  if (negReturns.length > 1) {
    const dsVariance = negReturns.reduce((s, v) => s + v ** 2, 0) / negReturns.length
    downsideRisk = Math.sqrt(dsVariance * 250) * 100
  }

  // Skewness, kurtosis, VaR 95%
  let skewness = null
  let kurtosis = null
  let var95 = null
  if (dailyReturns.length > 2) {
    const mean = dailyReturns.reduce((s, v) => s + v, 0) / dailyReturns.length
    const n = dailyReturns.length
    const variance = dailyReturns.reduce((s, v) => s + (v - mean) ** 2, 0) / (n - 1)
    const std = Math.sqrt(variance)
    if (std > 0) {
      const m3 = dailyReturns.reduce((s, v) => s + ((v - mean) / std) ** 3, 0) / n
      skewness = m3
      const m4 = dailyReturns.reduce((s, v) => s + ((v - mean) / std) ** 4, 0) / n
      kurtosis = m4 - 3 // excess kurtosis
      // Parametric VaR 95%: annualized
      var95 = (mean - 1.645 * std) * Math.sqrt(250) * 100
    }
  }

  // Risk-free rate: 2.5% annualized
  const rfAnnual = 0.025

  // Sharpe ratio
  let sharpe = null
  if (annualizedVol != null && annualizedVol > 0 && annualizedReturn != null) {
    sharpe = (annualizedReturn / 100 - rfAnnual) / (annualizedVol / 100)
  }

  // Sortino ratio
  let sortino = null
  if (downsideRisk != null && downsideRisk > 0 && annualizedReturn != null) {
    sortino = (annualizedReturn / 100 - rfAnnual) / (downsideRisk / 100)
  }

  // Max drawdown
  let maxDrawdown = 0
  let runningPeak = vals[0]
  let peakIdx = 0
  let maxDDPeakIdx = 0
  let maxDDTroughIdx = 0

  for (let i = 1; i < vals.length; i++) {
    if (vals[i] > runningPeak) {
      runningPeak = vals[i]
      peakIdx = i
    }
    const dd = (vals[i] - runningPeak) / runningPeak * 100
    if (dd < maxDrawdown) {
      maxDrawdown = dd
      maxDDPeakIdx = peakIdx
      maxDDTroughIdx = i
    }
  }

  // Max drawdown recovery period
  let maxDDRecoveryDays = null
  let maxDDRecovered = false
  if (maxDrawdown < 0) {
    const peakVal = vals[maxDDPeakIdx]
    const troughDate = new Date(dates[maxDDTroughIdx])
    for (let i = maxDDTroughIdx + 1; i < vals.length; i++) {
      if (vals[i] >= peakVal) {
        const recoveryDate = new Date(dates[i])
        maxDDRecoveryDays = Math.round((recoveryDate - troughDate) / (1000 * 60 * 60 * 24))
        maxDDRecovered = true
        break
      }
    }
  }

  // Calmar ratio: annualized return / |max drawdown|
  let calmar = null
  if (maxDrawdown < 0 && annualizedReturn != null) {
    calmar = (annualizedReturn / 100) / Math.abs(maxDrawdown / 100)
  }

  // Monthly win rate
  const monthlyMap = new Map()
  items.forEach(item => {
    if (!item.nav_date) return
    const month = item.nav_date.slice(0, 7)
    const val = getVal(item)
    if (val == null || isNaN(val)) return
    if (!monthlyMap.has(month)) {
      monthlyMap.set(month, { first: val, last: val })
    } else {
      monthlyMap.get(month).last = val
    }
  })
  let wins = 0
  const months = Array.from(monthlyMap.values())
  months.forEach(m => { if (m.last > m.first) wins++ })
  const monthlyWinRate = months.length > 0 ? (wins / months.length * 100) : null

  // Longest consecutive no-new-high (calendar days)
  let longestNoNewHigh = 0
  let ath = vals[0]
  let athDate = new Date(dates[0])
  let noNewHighStartDate = null

  for (let i = 1; i < vals.length; i++) {
    const currentDate = new Date(dates[i])
    if (vals[i] > ath) {
      ath = vals[i]
      athDate = currentDate
      noNewHighStartDate = null
    } else {
      if (noNewHighStartDate === null) noNewHighStartDate = athDate
      const elapsed = Math.round((currentDate - noNewHighStartDate) / (1000 * 60 * 60 * 24))
      if (elapsed > longestNoNewHigh) longestNoNewHigh = elapsed
    }
  }

  return {
    periodReturn,
    annualizedReturn,
    annualizedVol,
    downsideRisk,
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
 * Compute benchmark-relative metrics: correlation, beta, alpha, tracking error, information ratio.
 */
export function computeBenchmarkMetrics(fundItems, benchItems, navType = 'unit') {
  if (!fundItems || !benchItems || fundItems.length < 2 || benchItems.length < 2) return null

  const getVal = item =>
    navType === 'unit' ? item.unit_nav : (item.accumulated_nav ?? item.unit_nav)

  // Build date→value maps
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

  // Align on common dates
  const commonDates = [...fundMap.keys()].filter(d => benchMap.has(d)).sort()
  if (commonDates.length < 3) return null

  // Daily returns for both
  const fundReturns = []
  const benchReturns = []
  for (let i = 1; i < commonDates.length; i++) {
    const prevFund = fundMap.get(commonDates[i - 1])
    const currFund = fundMap.get(commonDates[i])
    const prevBench = benchMap.get(commonDates[i - 1])
    const currBench = benchMap.get(commonDates[i])
    if (prevFund > 0 && prevBench > 0) {
      fundReturns.push((currFund - prevFund) / prevFund)
      benchReturns.push((currBench - prevBench) / prevBench)
    }
  }
  if (fundReturns.length < 2) return null

  const n = fundReturns.length
  const meanF = fundReturns.reduce((s, v) => s + v, 0) / n
  const meanB = benchReturns.reduce((s, v) => s + v, 0) / n

  let covFB = 0, varF = 0, varB = 0
  for (let i = 0; i < n; i++) {
    const df = fundReturns[i] - meanF
    const db = benchReturns[i] - meanB
    covFB += df * db
    varF += df * df
    varB += db * db
  }
  covFB /= (n - 1)
  varF /= (n - 1)
  varB /= (n - 1)

  const correlation = (varF > 0 && varB > 0) ? covFB / Math.sqrt(varF * varB) : null
  const beta = varB > 0 ? covFB / varB : null

  const rfDaily = 0.025 / 250
  const annualizedReturnF = meanF * 250
  const annualizedReturnB = meanB * 250
  const alpha = beta != null ? (annualizedReturnF - rfDaily * 250) - beta * (annualizedReturnB - rfDaily * 250) : null

  // Tracking error
  const excessReturns = fundReturns.map((r, i) => r - benchReturns[i])
  const meanExcess = excessReturns.reduce((s, v) => s + v, 0) / n
  const teVariance = excessReturns.reduce((s, v) => s + (v - meanExcess) ** 2, 0) / (n - 1)
  const trackingError = Math.sqrt(teVariance * 250) * 100

  const informationRatio = trackingError > 0 ? (meanExcess * 250 * 100) / trackingError : null

  return {
    correlation: correlation != null ? +correlation.toFixed(4) : null,
    beta: beta != null ? +beta.toFixed(4) : null,
    alpha: alpha != null ? +(alpha * 100).toFixed(2) : null,
    trackingError: +trackingError.toFixed(2),
    informationRatio: informationRatio != null ? +informationRatio.toFixed(3) : null,
  }
}

/**
 * Extract Top N drawdown events.
 * Returns array of { peakDate, troughDate, recoveryDate, drawdown (%), recoveryDays }.
 */
export function computeTopDrawdowns(items, navType = 'unit', count = 5) {
  if (!items || items.length < 2) return []

  const getVal = item => {
    if (navType === 'adjusted') return item.adjusted_nav ?? item.unit_nav
    if (navType === 'unit') return item.unit_nav
    return item.accumulated_nav ?? item.unit_nav
  }

  const vals = []
  const dates = []
  items.forEach(item => {
    const v = getVal(item)
    if (v != null && !isNaN(v) && isFinite(v) && item.nav_date) {
      vals.push(v)
      dates.push(item.nav_date)
    }
  })
  if (vals.length < 2) return []

  // Build drawdown series
  const ddSeries = []
  let peak = vals[0]
  let peakIdx = 0
  for (let i = 0; i < vals.length; i++) {
    if (vals[i] > peak) {
      peak = vals[i]
      peakIdx = i
    }
    ddSeries.push({ dd: (vals[i] - peak) / peak, peakIdx, idx: i })
  }

  // Find drawdown events by segmenting at recovery points
  const events = []
  let i = 0
  while (i < ddSeries.length && events.length < count * 3) {
    // Skip to next drawdown
    while (i < ddSeries.length && ddSeries[i].dd >= 0) i++
    if (i >= ddSeries.length) break

    let troughDD = 0
    let troughIdx = i
    let eventPeakIdx = ddSeries[i].peakIdx
    let j = i

    // Find trough within this drawdown segment
    while (j < ddSeries.length && ddSeries[j].dd < 0) {
      if (ddSeries[j].dd < troughDD) {
        troughDD = ddSeries[j].dd
        troughIdx = j
      }
      j++
    }

    // Recovery point
    let recoveryDate = null
    let recoveryDays = null
    if (j < ddSeries.length) {
      recoveryDate = dates[j]
      const troughDate = new Date(dates[troughIdx])
      const recDate = new Date(dates[j])
      recoveryDays = Math.round((recDate - troughDate) / (1000 * 60 * 60 * 24))
    }

    events.push({
      peakDate: dates[eventPeakIdx],
      troughDate: dates[troughIdx],
      recoveryDate,
      drawdown: +(troughDD * 100).toFixed(2),
      recoveryDays,
    })

    i = j
  }

  // Sort by severity and take top N
  events.sort((a, b) => a.drawdown - b.drawdown)
  return events.slice(0, count)
}

/**
 * Compute periodic returns grouped by frequency.
 * frequency: 'weekly' | 'monthly' | 'quarterly' | 'semiannual' | 'annual'
 * Returns array of { period, return (%) }
 */
export function computePeriodicReturns(items, navType = 'unit', frequency = 'monthly') {
  if (!items || items.length < 2) return []

  const getVal = item => {
    if (navType === 'adjusted') return item.adjusted_nav ?? item.unit_nav
    if (navType === 'unit') return item.unit_nav
    return item.accumulated_nav ?? item.unit_nav
  }

  function getPeriodKey(dateStr) {
    const y = dateStr.slice(0, 4)
    const m = dateStr.slice(5, 7)
    const mNum = parseInt(m)
    switch (frequency) {
      case 'weekly': {
        const d = new Date(dateStr)
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
    if (!groups.has(key)) {
      groups.set(key, { first: v, last: v, date: item.nav_date })
    } else {
      groups.get(key).last = v
    }
  })

  const result = []
  for (const [period, g] of groups) {
    if (g.first > 0) {
      result.push({
        period,
        return: +((g.last - g.first) / g.first * 100).toFixed(4),
      })
    }
  }
  return result
}

/**
 * Compute excess return metrics relative to a benchmark.
 *
 * mode: 'arithmetic' — daily excess = fund_r - bench_r
 *       'geometric'  — daily excess = (1+fund_r)/(1+bench_r) - 1
 *
 * Returns: { periodExcess, annualizedExcess, excessVol, excessMaxDD, excessSharpe }
 * All percentage values as numbers (e.g. 5.23 means 5.23%).
 */
export function computeExcessMetrics(fundItems, benchItems, navType = 'unit', mode = 'arithmetic') {
  if (!fundItems || !benchItems || fundItems.length < 2 || benchItems.length < 2) return null

  const getVal = item =>
    navType === 'unit' ? item.unit_nav : (item.accumulated_nav ?? item.unit_nav)

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
    const e = mode === 'geometric' ? (1 + fR) / (1 + bR) - 1 : fR - bR
    excessDaily.push(e)
    cumSeries.push(cumSeries[cumSeries.length - 1] * (1 + e))
  }

  if (excessDaily.length < 2) return null

  const firstDate = new Date(commonDates[0])
  const lastDate = new Date(commonDates[commonDates.length - 1])
  const days = Math.max(1, Math.round((lastDate - firstDate) / (1000 * 60 * 60 * 24)))

  const periodExcess = (cumSeries[cumSeries.length - 1] - 1) * 100

  const annualizedExcess = days >= 30
    ? (Math.pow(1 + periodExcess / 100, 365 / days) - 1) * 100
    : null

  const n = excessDaily.length
  const meanE = excessDaily.reduce((s, v) => s + v, 0) / n
  const varE = excessDaily.reduce((s, v) => s + (v - meanE) ** 2, 0) / (n - 1)
  const excessVol = Math.sqrt(varE * 250) * 100

  let excessMaxDD = 0
  let peak = cumSeries[0]
  for (let i = 1; i < cumSeries.length; i++) {
    if (cumSeries[i] > peak) peak = cumSeries[i]
    const dd = (cumSeries[i] - peak) / peak * 100
    if (dd < excessMaxDD) excessMaxDD = dd
  }

  const excessSharpe = (excessVol > 0 && annualizedExcess != null)
    ? (annualizedExcess / 100) / (excessVol / 100)
    : null

  return {
    periodExcess: +periodExcess.toFixed(2),
    annualizedExcess: annualizedExcess != null ? +annualizedExcess.toFixed(2) : null,
    excessVol: +excessVol.toFixed(2),
    excessMaxDD: +excessMaxDD.toFixed(2),
    excessSharpe: excessSharpe != null ? +excessSharpe.toFixed(3) : null,
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
    if (metrics) {
      result.push({ year, metrics })
    }
  }
  return result
}
