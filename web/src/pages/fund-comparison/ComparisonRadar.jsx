import { useMemo } from 'react'
import {
  Chart as ChartJS, RadialLinearScale, PointElement,
  LineElement, Filler, Tooltip, Legend,
} from 'chart.js'
import { Radar } from 'react-chartjs-2'
import { computeMetrics } from '../../utils/metrics.js'
import { FUND_COLORS } from '../../utils/metricDefs.js'

ChartJS.register(RadialLinearScale, PointElement, LineElement, Filler, Tooltip, Legend)

const LABELS = ['年化收益', '夏普比率', '抗回撤', '低波动', '月胜率', '卡玛比率']

function normalize(val, min, max) {
  if (val == null || !Number.isFinite(val)) return null
  return Math.max(0, Math.min(100, ((Math.max(min, Math.min(max, val)) - min) / (max - min)) * 100))
}

function getRadarValues(m) {
  if (!m) return [null, null, null, null, null, null]
  return [
    normalize(m.annualizedReturn,       -50,  100),
    normalize(m.sharpe,                  -2,    5),
    normalize(m.maxDrawdown == null ? null : -Math.abs(m.maxDrawdown), -50, 0),
    normalize(m.annualizedVol == null ? null : -m.annualizedVol, -80, 0),
    normalize(m.monthlyWinRate,           0,  100),
    normalize(m.calmar,                  -2,   10),
  ]
}

export default function ComparisonRadar({ compareList, navDataMap, commonStart }) {
  const radarData = useMemo(() => {
    const datasets = compareList.map((f, idx) => {
      const allItems = navDataMap[f.fund_id] || []
      // Use common-start filtered items so all funds compared on same period
      const items = commonStart
        ? allItems.filter(i => i.nav_date >= commonStart)
        : allItems
      const m = computeMetrics(items, 'unit')
      const color = FUND_COLORS[idx % FUND_COLORS.length]
      return {
        label: f.product_name.length > 12 ? f.product_name.slice(0, 12) + '…' : f.product_name,
        data: getRadarValues(m),
        borderColor: color,
        backgroundColor: color + '22',
        borderWidth: 2,
        pointRadius: 3,
        pointBackgroundColor: color,
      }
    })
    return { labels: LABELS, datasets }
  }, [compareList, navDataMap, commonStart])

  const options = useMemo(() => ({
    responsive: true,
    maintainAspectRatio: false,
    animation: { duration: 0 },
    plugins: {
      legend: { position: 'bottom', labels: { font: { size: 11 }, boxWidth: 12 } },
      tooltip: {
        callbacks: {
          label: item => `${item.dataset.label}: ${Number(item.raw).toFixed(1)}`,
        },
      },
    },
    scales: {
      r: {
        beginAtZero: true,
        max: 100,
        ticks: { display: false },
        pointLabels: { font: { size: 11 }, color: '#6b7280' },
        grid: { color: '#e5e7eb' },
      },
    },
  }), [])

  return (
    <div className="bg-white rounded-xl shadow p-5">
      <div className="flex items-center justify-between mb-4">
        <h3 className="text-sm font-semibold text-gray-700">风险收益画像</h3>
        <p className="text-xs text-gray-400">基于共同起始区间 · 数值已归一化 0–100</p>
      </div>
      <div className="h-80 max-w-lg mx-auto">
        <Radar data={radarData} options={options} />
      </div>
    </div>
  )
}
