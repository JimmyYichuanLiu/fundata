import { useMemo, useState } from 'react'
import { useLocation, useNavigate } from 'react-router-dom'
import { createPortfolio, calculatePortfolio } from '../api.js'
import {
  buildEqualWeights,
  updateWeights,
  sumWeights,
  buildPortfolioCalculatePayload,
} from '../utils/portfolio.js'

function useSelectedFundIds() {
  const { search } = useLocation()
  const params = new URLSearchParams(search)
  return params.getAll('fund_ids').map((v) => Number(v)).filter((v) => Number.isFinite(v))
}

export default function PortfolioBuilderPage() {
  const navigate = useNavigate()
  const selectedFundIds = useSelectedFundIds()
  const [portfolioName, setPortfolioName] = useState('我的基金组合')
  const [method, setMethod] = useState('UNIFIED_START')
  const [effectiveDate, setEffectiveDate] = useState('2024-01-05')
  const [weights, setWeights] = useState(() => buildEqualWeights(selectedFundIds))
  const [saving, setSaving] = useState(false)

  const totalWeight = useMemo(() => sumWeights(weights), [weights])

  const onResetEqual = () => {
    setWeights(buildEqualWeights(selectedFundIds))
  }

  const onGenerate = async () => {
    setSaving(true)
    try {
      const payload = buildPortfolioCalculatePayload({ method, portfolioName, weights, effectiveDate })
      const created = await createPortfolio(payload)
      await calculatePortfolio(created.id)
      navigate(`/portfolios/${created.id}`)
    } finally {
      setSaving(false)
    }
  }

  return (
    <div className="p-4 md:p-8">
      <h1 className="text-xl font-bold mb-4">基金组合配置</h1>
      <div className="mb-4">
        <label className="text-sm">组合名称</label>
        <input className="block border rounded px-3 py-2" value={portfolioName} onChange={(e) => setPortfolioName(e.target.value)} />
      </div>

      <div className="mb-4">
        <p className="text-sm font-medium mb-2">构建方式</p>
        <div className="flex gap-2">
          <button className={`px-3 py-1 rounded ${method === 'BATCH_INCLUDE' ? 'bg-primary text-white' : 'border'}`} onClick={() => setMethod('BATCH_INCLUDE')}>分批纳入法</button>
          <button className={`px-3 py-1 rounded ${method === 'UNIFIED_START' ? 'bg-primary text-white' : 'border'}`} onClick={() => setMethod('UNIFIED_START')}>统一起始日法</button>
        </div>
      </div>

      <div className="mb-4">
        <p className="text-sm font-medium">权重配置</p>
        {weights.map((w) => (
          <div key={w.fund_id} className="flex items-center gap-2 py-1">
            <span className="w-24 text-sm">基金 {w.fund_id}</span>
            <input type="range" min="0" max="1" step="0.01" value={w.weight} onChange={(e) => setWeights((prev) => updateWeights(prev, w.fund_id, Number(e.target.value)))} />
            <input type="number" min="0" max="1" step="0.01" className="w-24 border rounded px-2 py-1" value={w.weight} onChange={(e) => setWeights((prev) => updateWeights(prev, w.fund_id, Number(e.target.value)))} />
          </div>
        ))}
        <button className="mt-2 px-3 py-1 border rounded" onClick={onResetEqual}>重置等权</button>
        <p className={`mt-2 text-sm ${Math.abs(totalWeight - 1) > 1e-9 ? 'text-rose-500' : 'text-emerald-600'}`}>
          权重合计: {(totalWeight * 100).toFixed(2)}%
        </p>
      </div>

      <button disabled={saving} onClick={onGenerate} className="px-4 py-2 bg-primary text-white rounded disabled:opacity-50">{saving ? '生成中…' : '生成组合'}</button>
    </div>
  )
}
