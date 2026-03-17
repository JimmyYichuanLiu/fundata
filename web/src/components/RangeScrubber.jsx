import { useCallback, useRef } from 'react'

/**
 * Dual-handle range scrubber for timeline navigation.
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

  const startPct = (safeStart / (len - 1)) * 100
  const endPct   = (safeEnd   / (len - 1)) * 100

  const handleStart = useCallback((e) => {
    const idx = Number(e.target.value)
    const newStart = Math.min(idx, safeEnd - 1)
    onChange(newStart, safeEnd)
  }, [safeEnd, onChange])

  const handleEnd = useCallback((e) => {
    const idx = Number(e.target.value)
    const newEnd = Math.max(idx, safeStart + 1)
    onChange(safeStart, newEnd)
  }, [safeStart, onChange])

  function fmtDate(idx) {
    const d = dates[idx] || ''
    // Accept both YYYY-MM-DD and YYYYMMDD
    if (d.length === 8) return `${d.slice(0,4)}-${d.slice(4,6)}-${d.slice(6,8)}`
    return d
  }

  return (
    <div className="mt-2 px-1 select-none">
      {/* Track + highlight */}
      <div className="relative h-5 flex items-center">
        {/* Full track */}
        <div className="absolute inset-x-0 h-1 bg-slate-200 dark:bg-slate-700 rounded-full" />
        {/* Selected range highlight */}
        <div
          className="absolute h-1 bg-blue-400 rounded-full"
          style={{ left: `${startPct}%`, right: `${100 - endPct}%` }}
        />
        {/* Start range input */}
        <input
          type="range"
          min={0}
          max={len - 1}
          step={1}
          value={safeStart}
          onChange={handleStart}
          className="range-scrubber-thumb absolute inset-x-0 w-full h-1 appearance-none bg-transparent cursor-pointer"
          style={{ zIndex: safeStart > len - 5 ? 5 : 3 }}
        />
        {/* End range input */}
        <input
          type="range"
          min={0}
          max={len - 1}
          step={1}
          value={safeEnd}
          onChange={handleEnd}
          className="range-scrubber-thumb absolute inset-x-0 w-full h-1 appearance-none bg-transparent cursor-pointer"
          style={{ zIndex: 4 }}
        />
      </div>
      {/* Date labels */}
      <div className="flex justify-between mt-0.5 text-[10px] text-slate-400 dark:text-slate-500 pointer-events-none">
        <span>{fmtDate(safeStart)}</span>
        <span>{fmtDate(safeEnd)}</span>
      </div>
    </div>
  )
}
