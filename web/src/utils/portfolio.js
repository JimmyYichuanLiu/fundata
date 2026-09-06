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
  return weights.reduce((acc, w) => acc + Number(w.weight), 0)
}

export function validatePortfolio({ weights, portfolioName, effectiveDate, dates = {}, method }) {
  if (!portfolioName.trim()) return '请填写组合名称。'
  if (weights.length < 2) return '请选择至少两只基金。'
  if (weights.some(w => !Number.isFinite(w.weight) || w.weight < 0 || w.weight > 1)) return '每只基金的权重必须为 0 到 100 之间的有效数字。'
  if (Math.abs(sumWeights(weights) - 1) > 0.000001) return '权重合计必须为 100%。'
  if (!effectiveDate) return '请选择有效起始日期。'
  if (weights.some(w => !dates[w.fund_id]?.first)) return '部分基金缺少有效净值，无法构建组合。'
  if (method === 'UNIFIED_START') {
    const start = weights.map(w => dates[w.fund_id].first).sort().at(-1)
    const end = weights.map(w => dates[w.fund_id].last).sort()[0]
    if (start > end) return '所选基金没有公共净值区间。'
    if (effectiveDate < start || effectiveDate > end) return `起始日期应在公共区间 ${start} 至 ${end} 内。`
  }
  return ''
}

export function buildPortfolioCalculatePayload({ method, portfolioName, weights, effectiveDate, dates = {} }) {
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
      effective_date: dates[w.fund_id]?.first > effectiveDate ? dates[w.fund_id].first : effectiveDate,
    }
  })
  return {
    portfolio_name: portfolioName,
    build_method,
    constituents,
  }
}
