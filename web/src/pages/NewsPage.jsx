import { useState, useEffect, useCallback, useRef } from 'react'
import { Link } from 'react-router-dom'
import { fetchCrudeNews, triggerNewsSync, fetchNewsSyncStatus } from '../api/crudeApi.js'

// ---------------------------------------------------------------------------
// 常量
// ---------------------------------------------------------------------------

const NEWS_CATEGORIES = [
  { key: '',               label: '全部'   },
  { key: 'conflict',      label: '冲突'   },
  { key: 'shipping',      label: '运输'   },
  { key: 'crude',         label: '原油'   },
  { key: 'official_us',   label: '美国官方' },
  { key: 'official_iran', label: '伊朗官方' },
  { key: 'official_china',label: '中国官方' },
]

const CATEGORY_BADGE = {
  conflict:       'bg-rose-100 text-rose-600 dark:bg-rose-900/30 dark:text-rose-400',
  shipping:       'bg-sky-100 text-sky-700 dark:bg-sky-900/30 dark:text-sky-400',
  crude:          'bg-amber-100 text-amber-700 dark:bg-amber-900/30 dark:text-amber-400',
  official_us:    'bg-indigo-100 text-indigo-700 dark:bg-indigo-900/30 dark:text-indigo-400',
  official_iran:  'bg-purple-100 text-purple-700 dark:bg-purple-900/30 dark:text-purple-400',
  official_china: 'bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-400',
}

const CATEGORY_LABEL = {
  conflict:       '冲突',
  shipping:       '运输',
  crude:          '原油',
  official_us:    '美国官方',
  official_iran:  '伊朗官方',
  official_china: '中国官方',
}

function priorityDot(priority) {
  if (priority <= 2) return 'bg-red-500'
  if (priority <= 4) return 'bg-orange-400'
  return 'bg-yellow-400'
}

const PAGE_SIZE = 50

// ---------------------------------------------------------------------------
// 主组件
// ---------------------------------------------------------------------------

export default function NewsPage() {
  const [items,    setItems]    = useState([])
  const [total,    setTotal]    = useState(0)
  const [offset,   setOffset]   = useState(0)
  const [loading,  setLoading]  = useState(false)
  const [loadingMore, setLoadingMore] = useState(false)

  const [category, setCategory] = useState('')
  const [sort,     setSort]     = useState('time')
  const [query,    setQuery]    = useState('')
  const [inputVal, setInputVal] = useState('')

  const [syncing,    setSyncing]    = useState(false)
  const [syncStatus, setSyncStatus] = useState(null)
  const [syncMsg,    setSyncMsg]    = useState('')

  // 防抖 timer
  const debounceRef = useRef(null)

  // 输入框变化时防抖500ms后更新query
  function handleInput(e) {
    const val = e.target.value
    setInputVal(val)
    clearTimeout(debounceRef.current)
    debounceRef.current = setTimeout(() => {
      setQuery(val)
      setOffset(0)
    }, 500)
  }

  function clearSearch() {
    setInputVal('')
    setQuery('')
    setOffset(0)
  }

  // 加载第一页
  const loadFirst = useCallback(async (signal) => {
    setLoading(true)
    try {
      const res = await fetchCrudeNews(
        { category: category || undefined, sort, q: query || undefined, limit: PAGE_SIZE, offset: 0 },
        signal,
      )
      setItems(res.items || [])
      setTotal(res.total || 0)
      setOffset(0)
    } catch (e) {
      if (e.name !== 'AbortError') console.error('news load error', e)
    } finally {
      setLoading(false)
    }
  }, [category, sort, query])

  useEffect(() => {
    const ac = new AbortController()
    loadFirst(ac.signal)
    return () => ac.abort()
  }, [loadFirst])

  // 加载更多
  async function loadMore() {
    const newOffset = offset + PAGE_SIZE
    setLoadingMore(true)
    try {
      const res = await fetchCrudeNews(
        { category: category || undefined, sort, q: query || undefined, limit: PAGE_SIZE, offset: newOffset },
      )
      setItems(prev => [...prev, ...(res.items || [])])
      setOffset(newOffset)
    } catch (e) {
      console.error('load more error', e)
    } finally {
      setLoadingMore(false)
    }
  }

  // 同步状态
  useEffect(() => {
    fetchNewsSyncStatus().then(setSyncStatus).catch(() => {})
  }, [])

  async function handleSync() {
    setSyncing(true)
    setSyncMsg('')
    try {
      const { triggerNewsSync } = await import('../api/crudeApi.js')
      await triggerNewsSync()
      setSyncMsg('同步已启动，约30秒后刷新页面查看')
      setTimeout(async () => {
        const s = await fetchNewsSyncStatus().catch(() => null)
        if (s) setSyncStatus(s)
      }, 35000)
    } catch (e) {
      setSyncMsg(`失败: ${e.message}`)
    } finally {
      setSyncing(false)
    }
  }

  const hasMore = items.length < total

  return (
    <div className="px-4 py-4 md:p-6 max-w-5xl mx-auto space-y-4">

      {/* 页头 */}
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div className="flex items-center gap-3">
          <Link
            to="/crude"
            className="flex items-center gap-1 text-sm text-slate-500 hover:text-primary transition-colors"
          >
            <span className="material-symbols-outlined text-base">arrow_back</span>
            原油
          </Link>
          <h1 className="text-xl font-bold">全部新闻</h1>
          {total > 0 && <span className="text-sm text-slate-400">共 {total} 条</span>}
        </div>

        <div className="flex items-center gap-2 flex-wrap">
          {syncStatus && (
            <span className={`text-xs px-2 py-1 rounded-full font-medium ${
              syncStatus.last_status === 'success' ? 'bg-emerald-100 text-emerald-700'
              : syncStatus.last_status === 'running' ? 'bg-blue-100 text-blue-700'
              : 'bg-slate-100 text-slate-500'
            }`}>
              {syncStatus.last_status === 'success' ? '新闻正常'
               : syncStatus.last_status === 'running' ? '同步中'
               : syncStatus.last_status === 'never' ? '未同步' : '同步异常'}
            </span>
          )}
          <button
            onClick={handleSync}
            disabled={syncing}
            className="px-3 py-1.5 bg-slate-700 text-white rounded-lg text-xs font-medium hover:bg-slate-600 disabled:opacity-50 transition-colors"
          >
            {syncing ? '同步中…' : '抓取新闻'}
          </button>
        </div>
      </div>

      {syncMsg && (
        <div className="text-xs text-blue-600 dark:text-blue-400 bg-blue-50 dark:bg-blue-900/20 border border-blue-200 dark:border-blue-800 rounded px-3 py-2">
          {syncMsg}
        </div>
      )}

      {/* 搜索框 */}
      <div className="relative">
        <span className="absolute left-3 top-1/2 -translate-y-1/2 material-symbols-outlined text-slate-400 text-lg">search</span>
        <input
          type="text"
          value={inputVal}
          onChange={handleInput}
          placeholder="搜索新闻标题（中英文均可）…"
          className="w-full pl-9 pr-9 py-2.5 border border-slate-300 dark:border-slate-600 rounded-xl text-sm bg-white dark:bg-slate-800 focus:outline-none focus:ring-2 focus:ring-primary/30"
        />
        {inputVal && (
          <button
            onClick={clearSearch}
            className="absolute right-3 top-1/2 -translate-y-1/2 text-slate-400 hover:text-slate-600"
          >
            <span className="material-symbols-outlined text-lg">close</span>
          </button>
        )}
      </div>

      {/* 筛选 + 排序 */}
      <div className="flex flex-wrap items-center justify-between gap-2">
        <div className="flex flex-wrap items-center gap-2">
          {NEWS_CATEGORIES.map(({ key, label }) => (
            <button
              key={key}
              onClick={() => { setCategory(key); setOffset(0) }}
              className={`px-3 py-1 rounded-full text-xs font-medium transition-colors ${
                category === key
                  ? 'bg-primary text-white'
                  : 'bg-slate-100 dark:bg-slate-800 text-slate-600 dark:text-slate-300 hover:bg-slate-200 dark:hover:bg-slate-700'
              }`}
            >
              {label}
            </button>
          ))}
        </div>
        <div className="flex items-center gap-1">
          <span className="text-xs text-slate-400 mr-1">排序：</span>
          {[['time', '最新时间'], ['relevance', '相关度']].map(([k, lbl]) => (
            <button
              key={k}
              onClick={() => { setSort(k); setOffset(0) }}
              className={`px-2.5 py-1 rounded text-xs font-medium transition-colors ${
                sort === k
                  ? 'bg-slate-700 text-white'
                  : 'bg-slate-100 dark:bg-slate-800 text-slate-500 dark:text-slate-400 hover:bg-slate-200 dark:hover:bg-slate-700'
              }`}
            >
              {lbl}
            </button>
          ))}
        </div>
      </div>

      {/* 搜索提示 */}
      {query && (
        <div className="text-xs text-slate-500">
          搜索「{query}」— 找到 {total} 条（同时匹配中文标题和英文原标题）
        </div>
      )}

      {/* 新闻列表 */}
      <div className="bg-white dark:bg-slate-900 rounded-xl border border-slate-200 dark:border-slate-700 shadow-sm divide-y divide-slate-100 dark:divide-slate-800">
        {loading && (
          <div className="flex items-center justify-center h-32 text-slate-400 text-sm gap-2">
            <span className="material-symbols-outlined animate-spin text-lg">progress_activity</span>
            加载中…
          </div>
        )}

        {!loading && items.length === 0 && (
          <div className="flex flex-col items-center justify-center h-32 text-slate-400 text-sm gap-2">
            <span className="material-symbols-outlined text-3xl">newspaper</span>
            <span>{query ? `未找到包含「${query}」的新闻` : '暂无新闻，请点击「抓取新闻」'}</span>
          </div>
        )}

        {!loading && items.map(item => (
          <div key={item.id} className="px-4 py-3 hover:bg-slate-50 dark:hover:bg-slate-800/50 transition-colors">
            <div className="flex items-start justify-between gap-3">
              <div className="flex items-start gap-2 flex-1 min-w-0">
                <span className={`mt-1.5 shrink-0 w-2 h-2 rounded-full ${priorityDot(item.priority ?? 5)}`} />
                <div className="min-w-0">
                  <a
                    href={item.url}
                    target="_blank"
                    rel="noopener noreferrer"
                    title={item.title_zh ? item.title : undefined}
                    className="text-sm font-medium text-slate-800 dark:text-slate-100 hover:text-primary leading-snug"
                  >
                    {item.title_zh || item.title}
                  </a>
                  {/* 搜索时同时展示英文原文（若已翻译） */}
                  {query && item.title_zh && item.title_zh !== item.title && (
                    <p className="text-[11px] text-slate-400 mt-0.5 line-clamp-1">{item.title}</p>
                  )}
                </div>
              </div>
              <span className={`shrink-0 text-[10px] px-1.5 py-0.5 rounded font-medium ${
                CATEGORY_BADGE[item.category] || 'bg-slate-100 text-slate-500'
              }`}>
                {CATEGORY_LABEL[item.category] || item.category}
              </span>
            </div>
            <div className="flex items-center gap-3 mt-1.5 ml-4 text-xs text-slate-400">
              <span>{item.source_name}</span>
              {item.published_at && (
                <span>{item.published_at.slice(0, 16).replace('T', ' ')}</span>
              )}
            </div>
          </div>
        ))}

        {/* 加载更多 */}
        {!loading && hasMore && (
          <div className="flex justify-center py-4">
            <button
              onClick={loadMore}
              disabled={loadingMore}
              className="px-6 py-2 bg-slate-100 dark:bg-slate-800 hover:bg-slate-200 dark:hover:bg-slate-700 text-slate-600 dark:text-slate-300 rounded-lg text-sm font-medium transition-colors disabled:opacity-50"
            >
              {loadingMore ? '加载中…' : `加载更多（还有 ${total - items.length} 条）`}
            </button>
          </div>
        )}
      </div>

    </div>
  )
}
