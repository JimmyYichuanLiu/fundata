# 基金组合功能设计文档

## 1. 功能概述

基金组合功能用于将多只私募基金按预设权重合成为一条可回测、可展示、可计算指标的组合净值曲线，并在组合详情页提供与单基金详情页一致的指标分析与成分相关性分析。

本期支持两种构建方式：
1. **分批纳入法**（来自 `demo/combination.md`）：模拟真实逐步纳入与事件触发再平衡；
2. **统一起始日法**：取最晚生效日后统一起跑，全程固定权重，不再平衡。

---

## 2. 前端触发入口与交互流程

### 2.1 触发入口

与"基金对比"功能共用同一个多选触发机制：

- 在基金列表页，当用户勾选 **≥2 只基金** 时，底部操作栏同时出现两个按钮：
  - `基金对比`（现有功能）
  - `基金组合`（新功能）
- 点击"基金组合"进入组合配置页，所选基金自动填入成分列表。

### 2.2 组合配置页交互流程

```
[勾选≥2只基金] → 点击"基金组合"
    ↓
[权重配置区]
  - 默认等权（1/N）
  - 每只基金显示：名称 / 权重滑块 / 数字输入框
  - 实时显示权重合计，不等于100%时高亮提示
  - 支持"重置等权"快捷按钮
    ↓
[构建方式切换]（Tab 或 Toggle）
  - 分批纳入法 | 统一起始日法
  - 切换时说明两种方式的区别（一句话提示）
    ↓
[可选：自定义生效日]
  - 每只基金可单独设置生效日（默认用基金原生起始日）
    ↓
[生成组合] → 计算净值 → 进入组合详情页
```

### 2.3 组合详情页结构

与现有 FundDetail 页面结构对齐，复用已有组件：

| 区块 | 内容 | 复用组件 |
|---|---|---|
| 顶部摘要卡 | 累计收益、年化、最大回撤、波动率、夏普 | 现有 MetricCard |
| 净值曲线 | 组合净值 + 可选基准对比 | 现有 Chart.js 图表 |
| 构建方式切换 | 分批纳入法 / 统一起始日法 Tab | 新增 |
| 指标详情 | 全套收益/风险/超额指标 | 现有 MetricsTab |
| 成分明细 | 各基金权重、生效日、纳入顺序 | 新增表格 |
| 相关性分析 | 成分基金两两相关系数矩阵 | 现有 ComparisonCorrelation 或新增热力图 |

---

## 3. 数据库设计

### 2.1 新增表：`portfolio_master`（组合主表）

| 字段名 | 类型 | 约束 | 说明 |
|---|---|---|---|
| id | INTEGER | PK AUTOINCREMENT | 主键 |
| portfolio_code | TEXT | UNIQUE NOT NULL | 组合编码 |
| portfolio_name | TEXT | NOT NULL | 组合名称 |
| description | TEXT | NULL | 组合描述 |
| build_method | TEXT | NOT NULL CHECK IN ('BATCH_INCLUDE','UNIFIED_START') | 构建方式 |
| benchmark_index | TEXT | NULL | 基准指数代码 |
| rebalance_freq | TEXT | NOT NULL DEFAULT 'W' | 频率（当前周频） |
| status | TEXT | NOT NULL DEFAULT 'ACTIVE' | ACTIVE/ARCHIVED |
| created_at | TEXT | NOT NULL | 创建时间（ISO） |
| updated_at | TEXT | NOT NULL | 更新时间（ISO） |

索引建议：
- `idx_portfolio_master_method(build_method)`
- `idx_portfolio_master_status(status)`

### 2.2 新增表：`portfolio_constituents`（组合成分）

| 字段名 | 类型 | 约束 | 说明 |
|---|---|---|---|
| id | INTEGER | PK AUTOINCREMENT | 主键 |
| portfolio_id | INTEGER | NOT NULL FK -> portfolio_master(id) | 组合ID |
| fund_id | INTEGER | NOT NULL FK -> funds(fund_id) | 基金ID |
| fund_code | TEXT | NOT NULL | 冗余代码 |
| target_amount | REAL | NULL CHECK(target_amount > 0) | 终态金额（分批纳入法） |
| target_weight | REAL | NULL CHECK(target_weight >= 0) | 终态权重（统一起始日法） |
| custom_effective_date | TEXT | NULL | 自定义生效日 |
| effective_date | TEXT | NOT NULL | 实际生效日 |
| include_order | INTEGER | NOT NULL | 纳入顺序 |
| created_at | TEXT | NOT NULL | 创建时间 |
| updated_at | TEXT | NOT NULL | 更新时间 |

约束与索引建议：
- `UNIQUE(portfolio_id, fund_id)`
- `idx_constituents_portfolio(portfolio_id)`
- `idx_constituents_effective(portfolio_id, effective_date)`

### 2.3 新增表：`portfolio_nav_cache`（组合净值缓存）

| 字段名 | 类型 | 约束 | 说明 |
|---|---|---|---|
| id | INTEGER | PK AUTOINCREMENT | 主键 |
| portfolio_id | INTEGER | NOT NULL FK -> portfolio_master(id) | 组合ID |
| nav_date | TEXT | NOT NULL | 净值日期 |
| portfolio_nav | REAL | NOT NULL | 组合单位净值 |
| total_asset | REAL | NOT NULL | 组合总资产 |
| is_rebalance_day | INTEGER | NOT NULL DEFAULT 0 | 是否再平衡日（0/1） |
| included_fund_count | INTEGER | NOT NULL | 当期纳入基金数 |
| calc_version | INTEGER | NOT NULL | 计算版本 |
| created_at | TEXT | NOT NULL | 写入时间 |

约束与索引建议：
- `UNIQUE(portfolio_id, nav_date, calc_version)`
- `idx_nav_cache_portfolio_date(portfolio_id, nav_date)`

### 2.4 （可选）新增表：`portfolio_calc_jobs`（刷新任务日志）

用于记录刷新过程（RUNNING/SUCCESS/FAILED）与错误信息，便于排障。

---

## 3. API 设计

前缀：`/api/portfolios`

### 3.1 组合 CRUD

| Method | Path | 说明 |
|---|---|---|
| POST | `/api/portfolios` | 创建组合（含成分） |
| GET | `/api/portfolios` | 列表查询 |
| GET | `/api/portfolios/{id}` | 组合详情 |
| PUT | `/api/portfolios/{id}` | 更新组合 |
| DELETE | `/api/portfolios/{id}` | 删除/归档组合 |

### 3.2 计算与缓存

| Method | Path | 说明 |
|---|---|---|
| POST | `/api/portfolios/{id}/calculate` | 触发净值重算 |
| GET | `/api/portfolios/{id}/calc-jobs/latest` | 最新计算状态（异步时） |

### 3.3 净值与指标查询

| Method | Path | 说明 |
|---|---|---|
| GET | `/api/portfolios/{id}/nav` | 组合净值序列（图表） |
| GET | `/api/portfolios/{id}/nav-with-benchmark` | 组合+基准对比序列 |
| GET | `/api/portfolios/{id}/metrics` | 组合收益/风险指标 |

响应口径建议与现有 `/api/funds/*` 保持一致（`success/data/error` 风格可后续统一）。

---

## 5. 前端页面设计

### 4.1 路由与导航

- 导航新增：`组合管理`
- 路由建议：
  - `/portfolios`（列表）
  - `/portfolios/new`（创建）
  - `/portfolios/:id/edit`（编辑）
  - `/portfolios/:id`（详情）

### 4.2 列表页

展示：名称、构建方式、成分数、最近净值、更新时间、操作（查看/编辑/刷新/归档）。

### 4.3 创建/编辑页

1. 基本信息：名称、描述、方式、基准；
2. 成分配置：选基金、输入终态金额/权重、生效日；
3. 校验：重复基金、权重和=1（统一起始日法）、金额>0（分批纳入法）；
4. 提交后可选立即刷新。

### 4.4 详情页

- 复用现有图表与指标能力：
  - 净值曲线（组合 vs 基准）
  - 回撤曲线
  - MetricsTab 指标
- 显示成分明细：基金、终态金额/权重、生效日、纳入顺序。

---

## 6. 计算逻辑说明

> 统一使用复权净值 `adj_nav`（缺失时可回退 `unit_nav`，待确认）

### 5.1 方式一：分批纳入法（BATCH_INCLUDE）

1. 计算每只基金 `effective_date`（自定义优先）；
2. 按 `effective_date` 升序得出 `include_order`；
3. `t0 = min(effective_date)`；
4. 按周频日历遍历：
   - 若当周有新基金纳入：触发再平衡；
   - 当期目标权重：`w_i = target_amount_i / sum(target_amount_active)`；
   - 分配金额：`alloc_i = total_asset_prev * w_i`；
   - 持仓份额：`shares_i = alloc_i / nav_i(t)`；
   - 非再平衡周：`shares_i` 不变；
5. 当期总资产：`total_asset_t = Σ(shares_i * nav_i(t))`；
6. 标准化净值：`portfolio_nav_t = total_asset_t / total_asset_t0`。

### 5.2 方式二：统一起始日法（UNIFIED_START）

1. `t0 = max(effective_date)`（最晚生效日）；
2. `t0` 当日按终态权重建仓；
3. 后续不再平衡，持仓份额固定；
4. 组合净值可等价表达为：
   - **份额法**：先算初始份额，再逐期求和；
   - **归一化法**：`NAV_t = Σ(w_i * nav_i(t) / nav_i(t0))`。

---

## 7. 边界情况与待讨论问题

1. **净值缺失处理**：
   - A: 前值填充（LOCF）
   - B: 该基金该周不计入
   - C: 整周跳过
2. **再平衡日缺失净值**（新基金纳入当周无净值）：
   - 是否顺延到下一有净值周；
3. **初始总资产定义**（分批纳入法）：
   - 用首期已纳入基金终态金额总和，还是固定初始本金；
4. **刷新机制**：
   - 手动刷新为主，是否加定时自动刷新；
5. **删除策略**：
   - 软删（推荐）还是硬删；
6. **指标口径**：
   - 与现有基金指标口径完全一致（RF、年化系数等）还是组合单独配置。

---

## 8. 本期范围（建议）

MVP 先做：
1. 两种构建方式；
2. 组合 CRUD；
3. 手动刷新并落库缓存；
4. 组合净值曲线 + 基础指标展示；
5. 不做异步任务队列、不做自动调度（后续迭代）。
