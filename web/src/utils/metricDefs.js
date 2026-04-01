// Shared metric definitions used by MetricsTab and ComparisonMetrics

export const METRIC_GROUPS = [
  {
    key: 'return',
    label: '收益能力',
    metrics: [
      { key: 'periodReturn',     label: '区间收益',   format: 'pct'   },
      { key: 'annualizedReturn', label: '年化收益',   format: 'pct'   },
      { key: 'monthlyWinRate',   label: '月胜率',     format: 'pct'   },
    ],
  },
  {
    key: 'risk',
    label: '抗风险能力',
    metrics: [
      { key: 'maxDrawdown',       label: '最大回撤',   format: 'pct',  invertGood: true },
      { key: 'maxDDRecoveryDays', label: '回撤回补期', format: 'days'  },
      { key: 'annualizedVol',     label: '年化波动率', format: 'pct',  invertGood: true },
      { key: 'downsideRisk',      label: '下行风险',   format: 'pct',  invertGood: true },
    ],
  },
  {
    key: 'ratio',
    label: '投资性价比',
    metrics: [
      { key: 'sharpe',   label: '夏普比率', format: 'ratio' },
      { key: 'sortino',  label: '索提诺',   format: 'ratio' },
      { key: 'calmar',   label: '卡玛比率', format: 'ratio' },
    ],
  },
]

export const EXCESS_METRIC_GROUPS = [
  {
    key: 'excess_return',
    label: '超额收益指标',
    metrics: [
      { key: 'periodExcess',     label: '超额区间收益', format: 'pct'   },
      { key: 'annualizedExcess', label: '超额年化收益', format: 'pct'   },
      { key: 'excessSharpe',     label: '超额夏普',     format: 'ratio' },
      { key: 'alpha',            label: 'Alpha',        format: 'pct'   },
    ],
  },
  {
    key: 'excess_risk',
    label: '超额风险指标',
    metrics: [
      { key: 'excessMaxDD',  label: '超额最大回撤', format: 'pct',  invertGood: true },
      { key: 'excessVol',    label: '超额波动率',   format: 'pct',  invertGood: true },
    ],
  },
]

export const BENCHMARK_OPTIONS = [
  { label: '无', code: null },
  { label: '中证1000', code: '000852.SH' },
  { label: '中证500',  code: '000905.SH' },
  { label: '沪深300',  code: '000300.SH' },
  { label: '上证50',   code: '000016.SH' },
  { label: '上证指数', code: '000001.SH' },
  { label: '深证成指', code: '399001.SZ' },
  { label: '创业板指', code: '399006.SZ' },
  { label: '科创50',   code: '000688.SH' },
]

export function fmtMetric(val, format) {
  if (val == null) return '—'
  if (format === 'pct')   return `${val >= 0 ? '+' : ''}${val.toFixed(2)}%`
  if (format === 'ratio') return val.toFixed(3)
  if (format === 'days')  return `${Math.round(val)}天`
  return String(val)
}

export function metricColor(val, format, invertGood) {
  if (val == null) return 'text-gray-400'
  if (format === 'days') return 'text-gray-700'
  const isGood = invertGood ? val < 0 : val > 0
  const isBad  = invertGood ? val > 0 : val < 0
  if (isGood) return 'text-red-500'
  if (isBad)  return 'text-emerald-600'
  return 'text-gray-700'
}

// Chart colors for up to 8 funds
export const FUND_COLORS = [
  '#3b82f6', '#ef4444', '#10b981', '#f59e0b',
  '#8b5cf6', '#06b6d4', '#f97316', '#ec4899',
]
