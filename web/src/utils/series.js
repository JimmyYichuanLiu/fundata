/** Align reporting calendars without inventing explicit missing NAV values. */
export function alignComparisonSeries(items, dates, absolute = false) {
  if (!items.length) return dates.map(() => null)
  const firstValid = items.find(item => Number.isFinite(item.unit_nav) && item.unit_nav > 0)
  if (!firstValid) return dates.map(() => null)
  const base = absolute ? 1 : firstValid.unit_nav
  const byDate = new Map(items.map(item => [item.nav_date, item.unit_nav]))
  const end = items.at(-1).nav_date
  let latest = null
  return dates.map(date => {
    if (byDate.has(date)) latest = Number.isFinite(byDate.get(date)) && byDate.get(date) > 0 ? byDate.get(date) / base : null
    return date > end ? null : latest
  })
}
