import { useState } from 'react'
import Icon from './Icon.jsx'
export default function FundPicker({ funds, selectedIds, onToggle, max = 8 }) {
  const [query, setQuery] = useState('')
  const rows = funds.filter(fund => `${fund.product_name} ${fund.product_code}`.toLocaleLowerCase().includes(query.trim().toLocaleLowerCase()))
  return <section className="panel"><div className="toolbar"><label className="search-field"><Icon name="search" /><input value={query} onChange={event => setQuery(event.target.value)} placeholder="搜索并添加基金…" aria-label="添加基金" /></label><span className="text-xs text-slate-500">已选 {selectedIds.length} / {max} 只</span></div><div className="picker-grid">{rows.map(fund => { const selected = selectedIds.includes(fund.fund_id); return <button type="button" key={fund.fund_id} aria-pressed={selected} className={`picker-item ${selected ? 'selected' : ''}`} disabled={!selected && selectedIds.length >= max} onClick={() => onToggle(fund)}>{selected ? '✓ ' : '+ '}{fund.product_name}<small>{fund.product_code} · {fund.strategy_l2 || fund.strategy_l1 || '未分类'}</small></button> })}{!rows.length && <p className="text-xs text-slate-500">未找到匹配基金。</p>}</div></section>
}
