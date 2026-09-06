export const MAX_COMPARE_FUNDS = 8
export function parseFundIds(search) {
  const params = new URLSearchParams(search)
  return [...new Set(params.getAll('fund_ids').flatMap(value => value.split(',')).filter(value => /^\d+$/.test(value)).map(Number).filter(value => Number.isSafeInteger(value) && value > 0))].slice(0, MAX_COMPARE_FUNDS)
}
export function fundSelectionSearch(funds) {
  return new URLSearchParams(funds.slice(0, MAX_COMPARE_FUNDS).map(fund => ['fund_ids', String(fund.fund_id)])).toString()
}
