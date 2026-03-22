/**
 * 原油价格对比 API 封装
 * 对应后端 crude_api.py 中的端点
 */

const API = ''

async function apiFetch(path, signal) {
  const res = await fetch(`${API}${path}`, signal ? { signal } : {})
  if (!res.ok) {
    const body = await res.json().catch(() => ({}))
    throw new Error(body.detail || `HTTP ${res.status}`)
  }
  return res.json()
}

/**
 * 获取三品种对比数据（用于折线图）
 * @param {Object} opts - { date_from, date_to, limit }
 * @param {AbortSignal} signal
 * @returns {{ items: Array, latest_date: string, symbols: Object }}
 */
export async function fetchCrudeDaily(opts = {}, signal) {
  const params = new URLSearchParams()
  if (opts.date_from) params.set('date_from', opts.date_from)
  if (opts.date_to)   params.set('date_to', opts.date_to)
  if (opts.limit != null) params.set('limit', opts.limit)
  const qs = params.toString()
  return apiFetch(`/api/crude/daily${qs ? '?' + qs : ''}`, signal)
}

/**
 * 获取单品种历史数据
 * @param {string} tsCode - 'WTI' | 'BRENT' | 'SC'
 * @param {Object} opts - { date_from, date_to, limit }
 * @param {AbortSignal} signal
 */
export async function fetchCrudeSymbolDaily(tsCode, opts = {}, signal) {
  const params = new URLSearchParams()
  if (opts.date_from) params.set('date_from', opts.date_from)
  if (opts.date_to)   params.set('date_to', opts.date_to)
  if (opts.limit != null) params.set('limit', opts.limit)
  const qs = params.toString()
  return apiFetch(
    `/api/crude/${encodeURIComponent(tsCode)}/daily${qs ? '?' + qs : ''}`,
    signal,
  )
}

/**
 * 获取原油同步状态
 */
export async function fetchCrudeSyncStatus(signal) {
  return apiFetch('/api/crude/sync/status', signal)
}

/**
 * 手动触发原油数据同步
 */
export async function triggerCrudeSync() {
  const res = await fetch('/api/crude/sync/trigger', { method: 'POST' })
  if (!res.ok) {
    const body = await res.json().catch(() => ({}))
    throw new Error(body.detail || `HTTP ${res.status}`)
  }
  return res.json()
}

/**
 * 工具函数：将 YYYYMMDD 转换为 Date 对象
 */
export function parseCrudeDate(yyyymmdd) {
  if (!yyyymmdd || yyyymmdd.length !== 8) return null
  return new Date(
    parseInt(yyyymmdd.slice(0, 4)),
    parseInt(yyyymmdd.slice(4, 6)) - 1,
    parseInt(yyyymmdd.slice(6, 8)),
  )
}

/**
 * 工具函数：计算 N 天前的日期（YYYYMMDD 格式）
 */
export function daysAgoYYYYMMDD(n) {
  const d = new Date()
  d.setDate(d.getDate() - n)
  return d.toISOString().slice(0, 10).replace(/-/g, '')
}

// ---------------------------------------------------------------------------
// 新闻 API
// ---------------------------------------------------------------------------

/**
 * 获取新闻列表
 * @param {Object} opts - { category: 'conflict'|'crude', limit, offset }
 * @param {AbortSignal} signal
 */
export async function fetchCrudeNews(opts = {}, signal) {
  const params = new URLSearchParams()
  if (opts.category) params.set('category', opts.category)
  if (opts.limit  != null) params.set('limit',  opts.limit)
  if (opts.offset != null) params.set('offset', opts.offset)
  const qs = params.toString()
  return apiFetch(`/api/news${qs ? '?' + qs : ''}`, signal)
}

/**
 * 获取新闻同步状态
 */
export async function fetchNewsSyncStatus(signal) {
  return apiFetch('/api/news/sync/status', signal)
}

/**
 * 手动触发新闻同步
 */
export async function triggerNewsSync() {
  const res = await fetch('/api/news/sync/trigger', { method: 'POST' })
  if (!res.ok) {
    const body = await res.json().catch(() => ({}))
    throw new Error(body.detail || `HTTP ${res.status}`)
  }
  return res.json()
}
