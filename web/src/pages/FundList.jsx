import { useState, useEffect, useCallback, useRef } from 'react'
import { useNavigate } from 'react-router-dom'
import { fetchStats, fetchFunds, fetchFundNav, subtractDays, fetchAllIssues, fetchSyncStatus, triggerSync, fetchFailures } from '../api.js'

const BATCH = 4

function SkeletonRow() {
  return (
    <tr>
      {[40, 200, 80, 80, 100, 80, 60].map((w, i) => (
        <td key={i} className="px-4 py-3">
          <div className="shimmer rounded h-4" style={{ width: w }} />
        </td>
      ))}
    </tr>
  )
}

function PctCell({ pct }) {
  if (pct == null) return <span className="text-gray-400">—</span>
  const color = pct > 0 ? 'text-red-500' : pct < 0 ? 'text-emerald-600' : 'text-gray-500'
  const sign = pct > 0 ? '+' : ''
  return <span className={color}>{sign}{pct.toFixed(2)}%</span>
}

function buildTooltip(issue) {
  const lines = []
  if (issue.anomalous.length > 0) {
    const parts = issue.anomalous.map(a => `${a.nav_date}(NAV=${a.unit_nav.toFixed(2)})`)
    lines.push(`异常净值: ${parts.join(', ')}`)
  }
  if (issue.gaps.length > 0) {
    const parts = issue.gaps.map(g => `${g.from_date} ~ ${g.to_date} (${g.gap_days}天)`)
    lines.push(`日期断层: ${parts.join('; ')}`)
  }
  return lines.join('\n')
}

export default function FundList() {
  const navigate = useNavigate()

  // ── All state declarations at the top (hooks must be called unconditionally) ──
  const [stats, setStats] = useState(null)
  const [funds, setFunds] = useState([])
  const [pcts, setPcts] = useState({})
  const [issues, setIssues] = useState({})
  const [syncStatus, setSyncStatus] = useState(null)
  const [syncing, setSyncing] = useState(false)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [search, setSearch] = useState('')
  const [debouncedSearch, setDebouncedSearch] = useState('')
  const [retryCount, setRetryCount] = useState(0)
  const [failures, setFailures] = useState({ total: 0, items: [] })
  const [showFailures, setShowFailures] = useState(false)
  const genRef = useRef(0)
  const syncPollRef = useRef(null)

  // ── Load funds, stats, issues, and sync status ──
  useEffect(() => {
    const controller = new AbortController()
    const { signal } = controller

    setError(null)
    setLoading(true)

    Promise.all([fetchStats(signal), fetchFunds(signal)])
      .then(([s, items]) => {
        setStats(s)
        setFunds(items)
        setLoading(false)
      })
      .catch(err => {
        if (err.name === 'AbortError') return
        setError(err.message)
        setLoading(false)
      })

    fetchAllIssues(signal)
      .then(data => {
        // JSON keys are always strings; convert to number-keyed map to match fund.fund_id
        const map = {}
        for (const [k, v] of Object.entries(data.issues || {})) {
          map[Number(k)] = v
        }
        setIssues(map)
      })
      .catch(err => { if (err.name !== 'AbortError') console.warn('issues load failed', err) })

    fetchSyncStatus(signal)
      .then(data => setSyncStatus(data))
      .catch(err => { if (err.name !== 'AbortError') console.warn('sync status load failed', err) })

    fetchFailures(signal)
      .then(data => setFailures(data))
      .catch(err => { if (err.name !== 'AbortError') console.warn('failures load failed', err) })

    return () => controller.abort()
  }, [retryCount])

  // ── Batch-load weekly returns ──
  useEffect(() => {
    if (funds.length === 0) return
    const gen = ++genRef.current
    const myGen = gen
    const controller = new AbortController()
    const { signal } = controller

    async function loadBatch(batch) {
      await Promise.all(
        batch.map(async (fund) => {
          try {
            const latestDate = fund.latest_date
            if (!latestDate) {
              setPcts(prev => ({ ...prev, [fund.fund_id]: null }))
              return
            }
            const from = subtractDays(latestDate, 14)
            const items = await fetchFundNav(fund.fund_id, {
              date_from: from,
              date_to: latestDate,
              limit: 30,
              apply_filter: false,
            }, signal)
            if (genRef.current !== myGen) return
            let pct = null
            if (items.length >= 2) {
              const first = items[0].unit_nav
              const last = items[items.length - 1].unit_nav
              pct = first > 0 ? (last - first) / first * 100 : null
            }
            setPcts(prev => ({ ...prev, [fund.fund_id]: pct }))
          } catch (err) {
            if (err.name === 'AbortError') return
            if (genRef.current === myGen) {
              setPcts(prev => ({ ...prev, [fund.fund_id]: null }))
            }
          }
        })
      )
    }

    async function loadAll() {
      for (let i = 0; i < funds.length; i += BATCH) {
        if (signal.aborted) return
        await loadBatch(funds.slice(i, i + BATCH))
      }
    }

    loadAll()
    return () => controller.abort()
  }, [funds])

  // ── Debounce search ──
  useEffect(() => {
    const t = setTimeout(() => setDebouncedSearch(search), 200)
    return () => clearTimeout(t)
  }, [search])

  // ── Cleanup sync poll on unmount ──
  useEffect(() => {
    return () => {
      if (syncPollRef.current) clearTimeout(syncPollRef.current)
    }
  }, [])

  const handleSearchChange = useCallback((e) => setSearch(e.target.value), [])

  const handleRetry = useCallback(() => {
    setFunds([])
    setPcts({})
    setRetryCount(c => c + 1)
  }, [])

  const handleSync = useCallback(async () => {
    if (syncing) return
    setSyncing(true)
    try {
      await triggerSync()
      // Poll until status is no longer 'running'
      function poll() {
        syncPollRef.current = setTimeout(async () => {
          try {
            const data = await fetchSyncStatus()
            setSyncStatus(data)
            if (data.sync_last_status === 'running') {
              poll()
            } else {
              setSyncing(false)
              // Reload funds and issues after sync
              setRetryCount(c => c + 1)
            }
          } catch {
            setSyncing(false)
          }
        }, 2000)
      }
      poll()
    } catch (err) {
      console.warn('trigger sync failed', err)
      setSyncing(false)
    }
  }, [syncing])

  const filtered = debouncedSearch
    ? funds.filter(f =>
        (f.product_name || '').includes(debouncedSearch) ||
        (f.product_code || '').includes(debouncedSearch)
      )
    : funds

  return (
    <div className="min-h-screen bg-gray-50">
      {/* Failures modal */}
      {showFailures && (
        <div
          className="fixed inset-0 bg-black/40 z-50 flex items-center justify-center p-4"
          onClick={() => setShowFailures(false)}
        >
          <div
            className="bg-white rounded-xl shadow-lg max-w-3xl w-full max-h-[80vh] flex flex-col"
            onClick={e => e.stopPropagation()}
          >
            <div className="flex items-center justify-between px-6 py-4 border-b border-gray-200">
              <h2 className="text-base font-semibold text-gray-800">
                附件提取失败记录（{failures.total} 条）
              </h2>
              <button
                onClick={() => setShowFailures(false)}
                className="text-gray-400 hover:text-gray-600 text-xl leading-none"
              >
                ×
              </button>
            </div>
            <div className="overflow-auto flex-1">
              <table className="w-full text-xs">
                <thead className="bg-gray-50 sticky top-0">
                  <tr className="text-left text-gray-600 border-b border-gray-200">
                    <th className="px-4 py-2 font-medium">失败时间</th>
                    <th className="px-4 py-2 font-medium">邮件主题</th>
                    <th className="px-4 py-2 font-medium">附件文件名</th>
                    <th className="px-4 py-2 font-medium">失败原因</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-100">
                  {failures.items.map(item => (
                    <tr key={item.id} className="hover:bg-gray-50">
                      <td className="px-4 py-2 whitespace-nowrap text-gray-500">
                        {item['失败时间']?.slice(0, 16) || '—'}
                      </td>
                      <td className="px-4 py-2 max-w-[200px] truncate text-gray-700" title={item['邮件主题']}>
                        {item['邮件主题'] || '—'}
                      </td>
                      <td className="px-4 py-2 max-w-[180px] truncate text-gray-700" title={item['附件文件名']}>
                        {item['附件文件名'] || '—'}
                      </td>
                      <td className="px-4 py-2 max-w-[220px] truncate text-red-500" title={item['失败原因']}>
                        {item['失败原因'] || '—'}
                      </td>
                    </tr>
                  ))}
                  {failures.items.length === 0 && (
                    <tr>
                      <td colSpan={4} className="px-4 py-8 text-center text-gray-400">暂无记录</td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      )}

      {/* Header */}
      <header className="bg-white border-b border-gray-200 sticky top-0 z-10">
        <div className="max-w-6xl mx-auto px-4 py-4 flex flex-wrap items-center gap-4">
          <h1 className="text-xl font-bold text-gray-900 flex items-center gap-2">
            <span className="inline-flex items-center justify-center w-8 h-8 bg-blue-600 text-white text-sm font-bold rounded">净</span>
            基金净值看板
          </h1>
          {stats && (
            <div className="flex gap-4 text-sm text-gray-500">
              <span>基金数量 <strong className="text-gray-800">{stats.total_funds}</strong></span>
              <span>净值记录 <strong className="text-gray-800">{stats.total_records.toLocaleString()}</strong></span>
            </div>
          )}
          <div className="ml-auto flex items-center gap-3">
            {failures.total > 0 && (
              <button
                onClick={() => setShowFailures(true)}
                className="px-2 py-1 text-xs bg-orange-100 text-orange-700 border border-orange-200 rounded-full hover:bg-orange-200 transition-colors"
              >
                ⚠ 提取失败 {failures.total}
              </button>
            )}
            <a
              href="/market"
              className="px-3 py-1.5 text-sm text-gray-600 border border-gray-200 rounded-lg hover:bg-gray-100 transition-colors"
            >
              行情
            </a>
            {syncStatus && (
              <span className="text-xs text-gray-400">
                上次同步: {syncStatus.sync_last_time ? syncStatus.sync_last_time.slice(0, 16).replace('T', ' ') : '—'}
                {syncStatus.sync_last_status === 'error' && (
                  <span className="ml-1 text-red-500">⚠ 失败</span>
                )}
              </span>
            )}
            <button
              onClick={handleSync}
              disabled={syncing}
              className="px-3 py-1.5 text-sm bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
            >
              {syncing ? '同步中…' : '立即同步'}
            </button>
          </div>
        </div>
      </header>

      <main className="max-w-6xl mx-auto px-4 py-6">
        {/* Error banner — non-blocking, shows retry button */}
        {error && (
          <div className="mb-4 flex items-center gap-3 bg-red-50 border border-red-200 rounded-lg px-4 py-3 text-sm">
            <span className="text-red-500 font-medium">连接后端失败</span>
            <span className="text-red-400 text-xs flex-1 truncate">{error}</span>
            <button
              onClick={handleRetry}
              className="shrink-0 px-3 py-1 bg-red-500 text-white rounded text-xs hover:bg-red-600"
            >
              重试
            </button>
          </div>
        )}

        {/* Search */}
        <div className="mb-4">
          <input
            type="text"
            value={search}
            onChange={handleSearchChange}
            placeholder="搜索基金名称或代码…"
            className="w-full max-w-sm px-4 py-2 border border-gray-300 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
          />
          {debouncedSearch && (
            <span className="ml-3 text-sm text-gray-500">
              共 {filtered.length} 条结果
            </span>
          )}
        </div>

        {/* Table */}
        <div className="bg-white rounded-xl shadow overflow-hidden">
          <table className="w-full text-sm">
            <thead>
              <tr className="bg-gray-50 border-b border-gray-200 text-left text-gray-600 font-medium">
                <th className="px-4 py-3 w-10">#</th>
                <th className="px-4 py-3">基金名称</th>
                <th className="px-4 py-3">代码</th>
                <th className="px-4 py-3 text-right">最新净值</th>
                <th className="px-4 py-3 text-right">净值日期</th>
                <th className="px-4 py-3 text-right">近一周收益</th>
                <th className="px-4 py-3 text-center">数据状态</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-100">
              {loading
                ? Array.from({ length: 8 }, (_, i) => <SkeletonRow key={i} />)
                : filtered.map((fund, idx) => {
                    const fundIssue = issues[fund.fund_id]
                    const issueCount = fundIssue
                      ? fundIssue.anomalous.length + fundIssue.gaps.length
                      : 0
                    const hasIssue = issueCount > 0

                    return (
                      <tr
                        key={fund.fund_id}
                        className="hover:bg-blue-50 cursor-pointer transition-colors"
                        onClick={() => navigate(`/fund/${fund.fund_id}`)}
                      >
                        <td className="px-4 py-3 text-gray-400">{idx + 1}</td>
                        <td className="px-4 py-3">
                          <span className="font-medium text-gray-900 hover:text-blue-600">
                            {fund.product_name || '—'}
                          </span>
                        </td>
                        <td className="px-4 py-3">
                          <code className="text-xs text-gray-500 font-mono">{fund.product_code}</code>
                        </td>
                        <td className="px-4 py-3 text-right font-mono text-gray-800">
                          {fund.latest_nav != null ? fund.latest_nav.toFixed(4) : '—'}
                        </td>
                        <td className="px-4 py-3 text-right text-gray-500">
                          {fund.latest_date || '—'}
                        </td>
                        <td className="px-4 py-3 text-right">
                          {fund.fund_id in pcts
                            ? <PctCell pct={pcts[fund.fund_id]} />
                            : <span className="text-gray-300 text-xs">加载中…</span>
                          }
                        </td>
                        <td className="px-4 py-3 text-center">
                          {hasIssue ? (
                            <span
                              title={buildTooltip(fundIssue)}
                              className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-medium bg-orange-100 text-orange-700 cursor-help"
                            >
                              ⚠ {issueCount}
                            </span>
                          ) : (
                            fundIssue
                              ? <span className="text-xs text-gray-300">—</span>
                              : null
                          )}
                        </td>
                      </tr>
                    )
                  })
              }
              {!loading && filtered.length === 0 && (
                <tr>
                  <td colSpan={7} className="px-4 py-12 text-center text-gray-400">
                    {error ? '请点击上方"重试"按钮重新加载' : '没有匹配的基金'}
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </main>
    </div>
  )
}
