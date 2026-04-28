export function shouldShowPortfolioEntry(selectedCount) {
  return selectedCount >= 2
}

export function buildEqualWeights(fundIds) {
  if (!fundIds || fundIds.length === 0) return []
  const w = 1 / fundIds.length
  return fundIds.map((fundId) => ({ fund_id: fundId, weight: w }))
}

export function updateWeights(weights, fundId, nextWeight) {
  return weights.map((w) => (w.fund_id === fundId ? { ...w, weight: nextWeight } : w))
}

export function sumWeights(weights) {
  return weights.reduce((acc, w) => acc + Number(w.weight || 0), 0)
}

export function buildPortfolioCalculatePayload({ method, portfolioName, weights, effectiveDate }) {
  const build_method = method
  const constituents = weights.map((w) => {
    if (method === 'UNIFIED_START') {
      return {
        fund_id: w.fund_id,
        target_weight: Number(w.weight),
        effective_date: effectiveDate,
      }
    }
    return {
      fund_id: w.fund_id,
      target_amount: Number(w.weight) * 100,
      effective_date: effectiveDate,
    }
  })
  return {
    portfolio_name: portfolioName,
    build_method,
    constituents,
  }
}
