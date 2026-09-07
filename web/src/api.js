const API = ''
let csrfToken = ''
export async function apiRequest(path, options = {}) {
  const headers = { ...options.headers }
  if (csrfToken && options.method && !['GET', 'HEAD'].includes(options.method)) headers['X-CSRF-Token'] = csrfToken
  return fetch(path, { ...options, headers, credentials: 'same-origin' })
}
export async function authSession() {
  const data = await apiFetch('/api/auth/session')
  csrfToken = data.csrf_token || ''
  return data
}
export async function authLogin(username, password) {
  const response = await apiRequest('/api/auth/login', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ username, password }) })
  const data = await response.json()
  if (!response.ok) throw new Error(data.detail || '登录失败')
  csrfToken = data.csrf_token || ''
  return data
}
export async function authLogout() {
  const response = await apiRequest('/api/auth/logout', { method: 'POST' })
  if (!response.ok) throw new Error('退出失败，请重试')
  csrfToken = ''
  return authSession()
}
export async function fetchPortfolios(signal) { return apiFetch('/api/portfolios', signal) }
export async function fetchSyncHistory(signal) { return apiFetch('/api/sync/history', signal) }
export async function retryFailure(id) {
  const response = await apiRequest('/api/failures/' + id + '/retry', { method: 'POST' })
  const data = await response.json()
  if (!response.ok) throw new Error(data.detail || '重试失败')
  return data
}
export async function downloadEmailExport() {
  const response = await apiRequest('/api/export/email.xlsx')
  if (!response.ok) { const body = await response.json().catch(() => ({})); throw new Error(body.detail || '导出失败，请重试') }
  const url = URL.createObjectURL(await response.blob())
  const link = document.createElement('a')
  link.href = url; link.download = 'fund_email_nav.xlsx'; document.body.appendChild(link); link.click(); link.remove()
  setTimeout(() => URL.revokeObjectURL(url), 1000)
}

async function apiFetch(path, signal) {
  const res = await apiRequest(`${API}${path}`, signal ? { signal } : {})
  if (!res.ok) {
    const body = await res.json().catch(() => ({}))
    throw new Error(body.detail || `HTTP ${res.status}`)
  }
  return res.json()
}

export async function fetchStats(signal) {
  return apiFetch('/api/stats', signal)
}

export async function fetchFunds(signal, opts = {}) {
  const params = new URLSearchParams()
  if (opts.source) params.set('source', opts.source)
  if (opts.strategy_l1) params.set('strategy_l1', opts.strategy_l1)
  if (opts.strategy_l2) params.set('strategy_l2', opts.strategy_l2)
  const qs = params.toString()
  const data = await apiFetch(`/api/funds${qs ? '?' + qs : ''}`, signal)
  return data.items
}

export async function fetchFund(id, signal) {
  return apiFetch(`/api/funds/${id}`, signal)
}

export async function fetchFundNav(id, opts = {}, signal) {
  const params = new URLSearchParams()
  if (opts.limit != null) params.set('limit', opts.limit)
  if (opts.apply_filter != null) params.set('apply_filter', opts.apply_filter)
  if (opts.date_from) params.set('date_from', opts.date_from)
  if (opts.date_to) params.set('date_to', opts.date_to)
  const qs = params.toString()
  const data = await apiFetch(`/api/funds/${id}/nav${qs ? '?' + qs : ''}`, signal)
  return data.items
}

export function subtractDays(dateStr, days) {
  if (!dateStr) return null
  const d = new Date(dateStr)
  if (isNaN(d.getTime())) return null
  d.setDate(d.getDate() - days)
  return d.toISOString().slice(0, 10)
}

export async function fetchSyncStatus(signal) {
  return apiFetch('/api/sync/status', signal)
}

export async function triggerSync() {
  const res = await apiRequest('/api/sync/trigger', { method: 'POST' })
  if (!res.ok) throw new Error(`HTTP ${res.status}`)
  return res.json()
}

export async function fetchAllIssues(signal) {
  return apiFetch('/api/funds/issues', signal)
}

export async function fetchFundIssues(id, signal) {
  return apiFetch(`/api/funds/${id}/issues`, signal)
}

export async function fetchFailures(signal) {
  return apiFetch('/api/failures?limit=100', signal)
}

export async function createNav(data) {
  const res = await apiRequest('/api/nav', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(data),
  })
  if (!res.ok) {
    const body = await res.json().catch(() => ({}))
    throw new Error(body.detail || `HTTP ${res.status}`)
  }
  return res.json()
}

export async function deleteNav(navId) {
  const res = await apiRequest(`/api/nav/${navId}`, { method: 'DELETE' })
  if (!res.ok && res.status !== 204) throw new Error(`HTTP ${res.status}`)
}
export async function updateNav(navId, data) {
  const response = await apiRequest(`/api/nav/${navId}`, { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(data) })
  const body = await response.json().catch(() => ({}))
  if (!response.ok) throw new Error(body.detail || '净值保存失败')
  return body
}

export async function fetchMarketIndices(signal) {
  return apiFetch('/api/market/indices', signal)
}

export async function fetchIndexDaily(tsCode, opts = {}, signal) {
  const params = new URLSearchParams()
  if (opts.date_from) params.set('date_from', opts.date_from)
  if (opts.date_to) params.set('date_to', opts.date_to)
  if (opts.limit != null) params.set('limit', opts.limit)
  const qs = params.toString()
  return apiFetch(
    `/api/market/indices/${encodeURIComponent(tsCode)}/daily${qs ? '?' + qs : ''}`,
    signal,
  )
}

export async function fetchMarketFutures(signal) {
  return apiFetch('/api/market/futures', signal)
}

export async function fetchFuturesDaily(tsCode, opts = {}, signal) {
  const params = new URLSearchParams()
  if (opts.date_from) params.set('date_from', opts.date_from)
  if (opts.date_to) params.set('date_to', opts.date_to)
  if (opts.limit != null) params.set('limit', opts.limit)
  const qs = params.toString()
  return apiFetch(
    `/api/market/futures/${encodeURIComponent(tsCode)}/daily${qs ? '?' + qs : ''}`,
    signal,
  )
}

export async function fetchMarketSyncStatus(signal) {
  return apiFetch('/api/market/sync/status', signal)
}

export async function triggerMarketSync() {
  const res = await apiRequest('/api/market/sync/trigger', { method: 'POST' })
  if (!res.ok) {
    const body = await res.json().catch(() => ({}))
    throw new Error(body.detail || `HTTP ${res.status}`)
  }
  return res.json()
}

export async function fetchRealtimeIndices(signal) {
  return apiFetch('/api/market/realtime/indices', signal)
}

export async function fetchRealtimeFutures(signal) {
  return apiFetch('/api/market/realtime/futures', signal)
}

export async function triggerRealtimeSync() {
  const res = await apiRequest('/api/market/realtime/trigger', { method: 'POST' })
  if (!res.ok) {
    const body = await res.json().catch(() => ({}))
    throw new Error(body.detail || `HTTP ${res.status}`)
  }
  return res.json()
}

export async function fetchBasisDaily(symbol, opts = {}, signal) {
  const params = new URLSearchParams()
  if (opts.date_from) params.set('date_from', opts.date_from)
  if (opts.date_to)   params.set('date_to', opts.date_to)
  if (opts.limit != null) params.set('limit', opts.limit)
  const qs = params.toString()
  return apiFetch(`/api/market/basis/${symbol}/daily${qs ? '?' + qs : ''}`, signal)
}

export async function fetchQuarterlyBasis(symbol, opts = {}, signal) {
  const params = new URLSearchParams()
  if (opts.date_from) params.set('date_from', opts.date_from)
  if (opts.date_to)   params.set('date_to', opts.date_to)
  const qs = params.toString()
  return apiFetch(`/api/market/basis/${symbol}/quarterly${qs ? '?' + qs : ''}`, signal)
}

export async function fetchBasisToday(symbol, signal) {
  return apiFetch(`/api/market/basis/${symbol}/today`, signal)
}

export async function setFundStrategy(fundId, strategyL1, strategyL2) {
  const res = await apiRequest(`/api/funds/${fundId}/strategy`, {
    method: 'PATCH',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ strategy_l1: strategyL1 || null, strategy_l2: strategyL2 || null }),
  })
  if (!res.ok) {
    const body = await res.json().catch(() => ({}))
    throw new Error(body.detail || `HTTP ${res.status}`)
  }
  return res.json()
}

export async function fetchCompare(fundIds, opts = {}, signal) {
  const params = new URLSearchParams()
  fundIds.forEach(id => params.append('fund_ids', id))
  if (opts.date_from) params.set('date_from', opts.date_from)
  if (opts.date_to)   params.set('date_to', opts.date_to)
  params.set('apply_filter', opts.apply_filter ?? true)
  return apiFetch(`/api/compare?${params.toString()}`, signal)
}

export async function fetchFundReturns(opts = {}, signal) {
  const params = new URLSearchParams()
  if (opts.periods) params.set('periods', opts.periods)
  const qs = params.toString()
  return apiFetch(`/api/funds/returns${qs ? '?' + qs : ''}`, signal)
}

export async function fetchFundMetrics(opts = {}, signal) {
  const params = new URLSearchParams()
  if (opts.period) params.set('period', opts.period)
  const qs = params.toString()
  return apiFetch(`/api/funds/metrics/summary${qs ? '?' + qs : ''}`, signal)
}

export async function createPortfolio(payload) {
  const res = await apiRequest('/api/portfolios', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  })
  if (!res.ok) {
    const body = await res.json().catch(() => ({}))
    throw new Error(body.detail || `HTTP ${res.status}`)
  }
  return res.json()
}

export async function calculatePortfolio(id) {
  const res = await apiRequest(`/api/portfolios/${id}/calculate`, { method: 'POST' })
  if (!res.ok) {
    const body = await res.json().catch(() => ({}))
    throw new Error(body.detail || `HTTP ${res.status}`)
  }
  return res.json()
}

export async function fetchPortfolio(id, signal) {
  return apiFetch(`/api/portfolios/${id}`, signal)
}

export async function fetchPortfolioNav(id, signal) {
  return apiFetch(`/api/portfolios/${id}/nav`, signal)
}

export async function fetchPortfolioMetrics(id, signal) {
  return apiFetch(`/api/portfolios/${id}/metrics`, signal)
}

export async function setFundBenchmark(fundId, benchmarkIndex) {
  const res = await apiRequest(`/api/funds/${fundId}/benchmark`, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ benchmark_index: benchmarkIndex }),
  })
  if (!res.ok) {
    const body = await res.json().catch(() => ({}))
    throw new Error(body.detail || `HTTP ${res.status}`)
  }
  return res.json()
}
