# Session 004 — 全站 Bug 修复

## 概述

本次 session 修复了多个导致页面白屏、API 报错、邮件同步失效的 bug，涵盖后端 Python 和前端 React 两侧。

---

## 1. 修复：组合计算 400 Bad Request（`/api/portfolios/11/calculate`）

**文件**: `api.py` — `_calculate_unified_start`

**根因**: `t0` 直接取 `effective_date` 字段值（如 `2024-01-05`），但该日期早于所有基金的实际首条 NAV 日期，导致找不到基准净值而返回 400。

**修复**: 对每只基金，找到 `>= effective_date` 的第一条实际 NAV 日期，再取所有基金中最晚的那个作为 `t0`。

```python
def _calculate_unified_start(constituents, nav_maps):
    fund_starts = []
    for c in constituents:
        fid = c["fund_id"]
        eff = c["effective_date"]
        first = next((d for d in sorted(nav_maps[fid].keys()) if d >= eff), None)
        if first is None:
            raise NavAPIError(400, f"Fund {fid} has no nav on or after effective_date={eff}", "BAD_REQUEST")
        fund_starts.append(first)
    t0 = max(fund_starts)
```

---

## 2. 修复：组合详情页显示原始 JSON（`PortfolioDetailPage.jsx`）

**文件**: `web/src/pages/PortfolioDetailPage.jsx`

**根因**: 页面为骨架实现，直接 `JSON.stringify` 渲染 API 响应。

**修复**: 完整重写为正式 UI，包含：
- 汇总指标卡片（区间收益、年化收益、最大回撤、夏普）
- Chart.js 折线图（支持基准指数叠加）
- RangeScrubber 日期范围控件
- 指标对比表（含 Beta/Alpha/相关系数）
- 成分基金权重表
- 重新计算按钮

---

## 3. 修复：邮件同步失效（`get_163_email.py`）

**文件**: `get_163_email.py`

**根因**: `db_schema_migrate.py` 已将数据库列名从中文改为英文，但 `get_163_email.py` 中所有 SQL 仍使用旧中文列名，导致 `no such column: funds.产品代码` 等错误。

**修复**: 全文替换所有 SQL 中的中文列名为英文列名：

| 旧列名 | 新列名 |
|--------|--------|
| `产品代码` | `fund_code` |
| `产品名称` | `fund_name` |
| `首次录入时间` | `created_at` |
| `净值日期` | `nav_date` |
| `单位净值` | `unit_nav` |
| `累计单位净值` | `accum_nav` |
| `插入时间` | `inserted_at` |

同时在 INSERT 语句中补充 `data_source='email'` 字段。

---

## 4. 修复：单基金详情页白屏（`MetricsTab.jsx`）

**文件**: `web/src/pages/fund-detail/MetricsTab.jsx`

**根因**: `nav_date` 字段存储为 `YYYYMMDD` 格式（如 `20260427`），`new Date('20260427')` 在部分浏览器返回 `Invalid Date`，后续 `.toISOString()` 抛出 `RangeError: Invalid time value`，导致 React 渲染崩溃白屏。

**修复**: 在 `periodItems` useMemo 中先对所有 `nav_date` 做格式归一化，再进行日期比较：

```js
function normalizeDate(dateStr) {
  if (/^\d{8}$/.test(dateStr))
    return `${dateStr.slice(0,4)}-${dateStr.slice(4,6)}-${dateStr.slice(6,8)}`
  if (/^\d{4}-\d{2}-\d{2}$/.test(dateStr)) return dateStr
  return null
}
```

---

## 5. 修复：`/api/funds/issues` 500 错误

**文件**: `api.py` — `_compute_issues`

**根因**: 数据库中 `unit_nav` 存储了带千分位逗号的字符串（如 `"10,997,307.40"`），`float(r[1])` 直接抛出 `ValueError`。

**修复**:

```python
try:
    nav_val = float(str(r[1]).replace(",", ""))
except (ValueError, TypeError):
    continue
```

---

## 6. 修复：`metrics.js` 全局日期格式兼容

**文件**: `web/src/utils/metrics.js`

**根因**: `computeMetrics`、`computeTopDrawdowns`、`computePeriodicReturns` 等函数内部直接 `new Date(dateStr)`，对 `YYYYMMDD` 格式不安全。

**修复**: 在文件顶部添加 `normalizeDate` + `parseDateSafe` 两个工具函数，替换所有裸 `new Date(dateStr)` 调用。

---

## 7. 修复：`/compare/v2` 对比页白屏

**文件**:
- `web/src/pages/fund-comparison/ComparisonChart.jsx`
- `web/src/pages/fund-comparison/ComparisonMetrics.jsx`

**根因**: 同 #4，`nav_date` 为 `YYYYMMDD` 格式时 `new Date(last)` 崩溃。

**ComparisonChart.jsx** (`fromDate` 计算):
```js
const normalized = /^\d{8}$/.test(last)
  ? `${last.slice(0,4)}-${last.slice(4,6)}-${last.slice(6,8)}`
  : last
const d = new Date(`${normalized}T00:00:00`)
if (Number.isNaN(d.getTime())) return ''
```

**ComparisonMetrics.jsx** (`filterByDays`):
```js
const normalized = /^\d{8}$/.test(last)
  ? `${last.slice(0,4)}-${last.slice(4,6)}-${last.slice(6,8)}`
  : last
const d = new Date(`${normalized}T00:00:00`)
if (Number.isNaN(d.getTime())) return items
```

---

## 变更文件汇总

| 文件 | 类型 |
|------|------|
| `api.py` | fix: `_calculate_unified_start` t0 计算；`_compute_issues` 千分位逗号 |
| `get_163_email.py` | fix: 全部 SQL 列名中文→英文 |
| `web/src/pages/PortfolioDetailPage.jsx` | feat: 完整重写组合详情页 UI |
| `web/src/pages/fund-detail/MetricsTab.jsx` | fix: nav_date YYYYMMDD 格式兼容 |
| `web/src/utils/metrics.js` | fix: 全局日期格式兼容 |
| `web/src/pages/fund-comparison/ComparisonChart.jsx` | fix: nav_date YYYYMMDD 格式兼容 |
| `web/src/pages/fund-comparison/ComparisonMetrics.jsx` | fix: nav_date YYYYMMDD 格式兼容 |
