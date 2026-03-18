import { useMemo } from 'react'
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Filler,
  Tooltip,
} from 'chart.js'
import { Line } from 'react-chartjs-2'

ChartJS.register(CategoryScale, LinearScale, PointElement, LineElement, Filler, Tooltip)

export default function MetricsTab({
  filteredItems,
  navType,
  benchmarkCode,
  normalizedData,
}) {
  const drawdownSeries = useMemo(() => {
    if (filteredItems.length < 2) return null
    const getVal = item => navType === 'unit' ? item.unit_nav : (item.accumulated_nav ?? item.unit_nav)
    let peak = getVal(filteredItems[0])
    const dd = filteredItems.map(item => {
      const v = getVal(item)
      if (v > peak) peak = v
      return peak > 0 ? ((v - peak) / peak) * 100 : 0
    })
    if (Math.min(...dd) >= -0.01) return null
    return dd
  }, [filteredItems, navType])

  const ddChartData = useMemo(() => {
    if (!drawdownSeries) return null
    return {
      labels: filteredItems.map(i => i.nav_date),
      datasets: [{
        label: '动态回撤',
        data: drawdownSeries,
        borderColor: 'rgba(239,68,68,0.7)',
        backgroundColor: 'rgba(239,68,68,0.1)',
        fill: true,
        tension: 0.3,
        pointRadius: 0,
        pointHoverRadius: 3,
        borderWidth: 1.5,
      }],
    }
  }, [drawdownSeries, filteredItems])

  const ddOptions = useMemo(() => ({
    responsive: true,
    maintainAspectRatio: false,
    animation: { duration: 0 },
    interaction: { mode: 'index', intersect: false },
    plugins: {
      legend: { display: false },
      tooltip: {
        callbacks: {
          label: (item) => `回撤: ${Number(item.raw).toFixed(2)}%`,
        },
      },
    },
    scales: {
      x: {
        grid: { display: false },
        ticks: { maxTicksLimit: 8, maxRotation: 0, font: { size: 10 }, color: '#9ca3af' },
      },
      y: {
        position: 'right',
        grid: { color: '#f3f4f6' },
        ticks: {
          callback: v => `${Number(v).toFixed(1)}%`,
          font: { size: 10 },
          color: '#9ca3af',
        },
      },
    },
  }), [])

  if (!ddChartData) {
    return <div className="text-center text-gray-400 py-12">暂无回撤数据</div>
  }

  return (
    <div className="bg-white rounded-xl shadow p-4">
      <h3 className="text-sm font-semibold text-gray-700 mb-3">动态回撤</h3>
      <div className="h-48">
        <Line data={ddChartData} options={ddOptions} />
      </div>
    </div>
  )
}
