import { useCallback, useRef } from 'react'

/**
 * Dual-handle range scrubber — Style A
 *
 * Props:
 *   dates      – string[]   full array of date labels (YYYY-MM-DD or YYYYMMDD)
 *   startIdx   – number     currently visible start index
 *   endIdx     – number     currently visible end index (inclusive)
 *   onChange   – (startIdx: number, endIdx: number) => void
 */
export default function RangeScrubber({ dates, startIdx, endIdx, onChange }) {
  const len = dates?.length ?? 0
  if (len < 2) return null

  const clamp = (v, lo, hi) => Math.max(lo, Math.min(hi, v))
  const safeStart = clamp(startIdx ?? 0, 0, len - 1)
  const safeEnd   = clamp(endIdx ?? len - 1, 0, len - 1)

  const trackRef   = useRef(null)
  const dragging   = useRef(null)           // 'start' | 'end' | null
  // Keep a ref to latest props so pointer handlers never go stale
  const latest     = useRef({})
  latest.current   = { safeStart, safeEnd, len, onChange }

  const startPct = (safeStart / (len - 1)) * 100
  const endPct   = (safeEnd   / (len - 1)) * 100

  function getIdx(clientX) {
    const rect = trackRef.current?.getBoundingClientRect()
    if (!rect || rect.width === 0) return 0
    const pct = Math.max(0, Math.min(1, (clientX - rect.left) / rect.width))
    return clamp(Math.round(pct * (latest.current.len - 1)), 0, latest.current.len - 1)
  }

  function fmtDate(idx) {
    const d = dates[idx] || ''
    if (d.length === 8) return `${d.slice(0,4)}-${d.slice(4,6)}-${d.slice(6,8)}`
    return d
  }

  const onStartDown = useCallback((e) => {
    e.preventDefault()
    dragging.current = 'start'
    e.currentTarget.setPointerCapture(e.pointerId)
  }, [])

  const onEndDown = useCallback((e) => {
    e.preventDefault()
    dragging.current = 'end'
    e.currentTarget.setPointerCapture(e.pointerId)
  }, [])

  const onStartMove = useCallback((e) => {
    if (dragging.current !== 'start') return
    const { safeEnd: se, onChange: cb } = latest.current
    cb(Math.min(getIdx(e.clientX), se - 1), se)
  }, [])

  const onEndMove = useCallback((e) => {
    if (dragging.current !== 'end') return
    const { safeStart: ss, onChange: cb } = latest.current
    cb(ss, Math.max(getIdx(e.clientX), ss + 1))
  }, [])

  const onPointerUp = useCallback(() => { dragging.current = null }, [])

  const handleCls =
    'absolute w-4 h-4 md:w-3.5 md:h-3.5 rounded-full bg-white border-2 border-blue-500 shadow ' +
    'cursor-grab active:cursor-grabbing hover:border-blue-600 hover:scale-110 ' +
    'transition-[border-color,transform] duration-100'

  return (
    <div className="mt-4 px-1 select-none">
      {/* Track row */}
      <div ref={trackRef} className="relative h-5 flex items-center">
        {/* Background track */}
        <div className="absolute inset-x-0 h-1 bg-gray-200 rounded-full" />

        {/* Selected range — gradient fill */}
        <div
          className="absolute h-1 rounded-full pointer-events-none"
          style={{
            left: `${startPct}%`,
            right: `${100 - endPct}%`,
            background: 'linear-gradient(90deg, #60a5fa 0%, #3b82f6 50%, #60a5fa 100%)',
          }}
        />

        {/* Start handle */}
        <div
          className={handleCls}
          style={{ left: `${startPct}%`, transform: 'translateX(-50%)', zIndex: 5 }}
          onPointerDown={onStartDown}
          onPointerMove={onStartMove}
          onPointerUp={onPointerUp}
        />

        {/* End handle */}
        <div
          className={handleCls}
          style={{ left: `${endPct}%`, transform: 'translateX(-50%)', zIndex: 5 }}
          onPointerDown={onEndDown}
          onPointerMove={onEndMove}
          onPointerUp={onPointerUp}
        />
      </div>

      {/* Date labels — float below each handle */}
      <div className="relative mt-1 h-4 pointer-events-none overflow-visible">
        <span
          className="absolute text-[11px] text-slate-400 whitespace-nowrap"
          style={{ left: `${startPct}%`, transform: 'translateX(-50%)' }}
        >
          {fmtDate(safeStart)}
        </span>
        <span
          className="absolute text-[11px] text-slate-400 whitespace-nowrap"
          style={{ left: `${endPct}%`, transform: 'translateX(-50%)' }}
        >
          {fmtDate(safeEnd)}
        </span>
      </div>
    </div>
  )
}
