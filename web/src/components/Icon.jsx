const paths = {
  analytics: 'M4 19V10m5 9V5m5 14v-7m5 7V3', dashboard: 'M3 3h7v7H3z M14 3h7v7h-7z M3 14h7v7H3z M14 14h7v7h-7z',
  trending_up: 'M3 17l6-6 4 4 8-10 M15 5h6v6', compare_arrows: 'M3 7h17m-4-4 4 4-4 4 M21 17H4m4-4-4 4 4 4',
  account_balance_wallet: 'M3 5h17v15H3z M3 5V3h14v2 M15 11h6v5h-6z', show_chart: 'M3 18l6-8 4 4 8-11',
  search: 'M21 21l-5-5 M18 10a8 8 0 1 1-16 0 8 8 0 0 1 16 0', close: 'M5 5l14 14 M19 5L5 19',
  menu: 'M4 6h16 M4 12h16 M4 18h16', light_mode: 'M12 2v2m0 16v2M2 12h2m16 0h2 M5 5l1 1m12 12 1 1M5 19l1-1M18 6l1-1 M17 12a5 5 0 1 1-10 0 5 5 0 0 1 10 0',
  dark_mode: 'M20 15A9 9 0 0 1 9 4a9 9 0 1 0 11 11', refresh: 'M20 7v5h-5 M4 17v-5h5 M5 7a8 8 0 0 1 14 0 M19 17a8 8 0 0 1-14 0',
  sync: 'M20 7v5h-5 M4 17v-5h5 M5 7a8 8 0 0 1 14 0 M19 17a8 8 0 0 1-14 0', add: 'M12 5v14 M5 12h14',
  arrow_back: 'M20 12H4m7-7-7 7 7 7', chevron_left: 'M15 5l-7 7 7 7', chevron_right: 'M9 5l7 7-7 7',
  check: 'M4 12l5 5L20 6', info: 'M12 11v6m0-10v1 M22 12a10 10 0 1 1-20 0 10 10 0 0 1 20 0',
  warning: 'M12 3l10 18H2z M12 9v5m0 3v1', error: 'M12 7v7m0 3v1 M22 12a10 10 0 1 1-20 0 10 10 0 0 1 20 0',
  settings: 'M4 6h16 M4 12h16 M4 18h16 M8 3v6 M16 9v6 M10 15v6', tune: 'M4 6h16 M4 12h16 M4 18h16 M8 3v6 M16 9v6 M10 15v6',
  download: 'M12 3v12m-5-5 5 5 5-5 M4 16v5h16v-5', delete: 'M3 6h18 M9 6V3h6v3 M5 6l1 15h12l1-15 M10 10v7m4-7v7',
  edit: 'M4 16L16 4l4 4L8 20H4z M14 6l4 4', lock: 'M5 10h14v11H5z M8 10V6a4 4 0 0 1 8 0v4',
}
export default function Icon({ name, children, className = '', ...props }) {
  const key = name || (typeof children === 'string' ? children.trim() : 'info')
  return <svg aria-hidden="true" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.7" strokeLinecap="round" strokeLinejoin="round" className={`ft-icon ${className}`} {...props}><path d={paths[key] || paths.info} /></svg>
}
