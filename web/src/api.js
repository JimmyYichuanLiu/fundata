const API = ''

async function apiFetch(path, signal) {
  const res = await fetch(`${API}${path}`, signal ? { signal } : {})
  if (!res.ok) {
    const body = await res.json().catch(() => ({}))
    throw new Error(body.detail || `HTTP ${res.status}`)
  }
  return res.json()
}

export async function fetchStats(signal) {
  return apiFetch('/api/stats', signal)
}

export async function fetchFunds(signal) {
  const data = await apiFetch('/api/funds', signal)
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
  const res = await fetch('/api/sync/trigger', { method: 'POST' })
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
  const res = await fetch('/api/nav', {
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
  const res = await fetch(`/api/nav/${navId}`, { method: 'DELETE' })
  if (!res.ok && res.status !== 204) throw new Error(`HTTP ${res.status}`)
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
  const res = await fetch('/api/market/sync/trigger', { method: 'POST' })
  if (!res.ok) {
    const body = await res.json().catch(() => ({}))
    throw new Error(body.detail || `HTTP ${res.status}`)
  }
  return res.json()
}
