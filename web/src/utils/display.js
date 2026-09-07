export function formatTimestamp(value) {
  if (!value) return '—'
  const text = String(value)
  if (!/(Z|[+-]\d\d:\d\d)$/.test(text)) return text.replace('T', ' ').slice(0, 19)
  const date = new Date(text)
  if (!Number.isFinite(date.getTime())) return '—'
  return new Intl.DateTimeFormat('sv-SE', { timeZone: 'Asia/Shanghai', year: 'numeric', month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit', second: '2-digit' }).format(date)
}
