# Portfolio MVP 实现进度

## 1) 测试清单（先写测试，RED）

### 后端（pytest）
- [ ] 组合 CRUD：创建/查询/更新/删除
- [ ] 分批纳入法计算正确性（小样本手算）
- [ ] 统一起始日法计算正确性（小样本手算）
- [ ] 指标接口结构正确且关键字段非空

### 前端（vitest）
- [ ] 勾选 >=2 只基金后出现“基金组合”入口
- [ ] 默认等权与权重调整联动正确
- [ ] 方法切换生效并触发不同计算
- [ ] 组合详情页展示净值与指标区域

### RED 结果
- 待新增测试后执行。

## 2) 实现清单（GREEN）
- 未开始。

## 3) 测试结果（GREEN）
- 未开始。

## 4) 变更文件列表
- /d/coding/fundata/sessions/portfolio_impl_progress.md

## 5) 未完成项/风险
- 需先确认现有前端测试环境（jsdom/testing-library）是否可用，若缺失需补充最小依赖。
- 组合指标将复用现有指标口径，需对齐后端已有计算逻辑字段命名。

## 6) Bug 修复记录

### KeyError: 21 in _calculate_batch_include (2026-04-28)

**Bug 描述**: `POST /api/portfolios/5/calculate` 返回 500，traceback 指向 `shares[fid]` KeyError。

**根因**: `_calculate_batch_include` 中 `rebalance` 条件仅在 `effective_date == d` 时触发。当某基金的 `effective_date` 早于当前日期 `d`，但该基金在 `effective_date` 当天无 NAV 数据时，它首次出现在 `active` 列表中时不会触发再平衡，导致 `shares` 字典中从未初始化该基金的份额。

**修复方案** (最小改动):
1. 将 `rebalance` 触发条件改为 `any(c["fund_id"] not in shares for c in active)` — 只要有新基金首次进入 active 就触发再平衡
2. 在非再平衡日遍历 active 时增加 `fid not in shares` 防御性跳过
3. 增加 LOCF (Last Observation Carried Forward) 缓存 `last_nav`，`nav_maps[fid].get(d, last_nav.get(fid))` 处理某基金某周无净值的情况

**修改文件**: `D:/coding/fundata/api.py` (函数 `_calculate_batch_include`, ~line 2037-2075)

**新增测试** (`tests/test_portfolio_api.py`):
- `test_batch_include_late_effective_no_nav`: 验证 effective_date 当周无 NAV 的基金能顺延到下一有 NAV 周并触发再平衡
- `test_batch_include_nav_gap_locf`: 验证非再平衡日基金净值缺失时不崩溃（LOCF 填充）

**测试结果**: 6/6 passed (含 2 个新增)

**Playwright 验证**: E2E 脚本已创建 (`tests/test_portfolio_e2e.py`)，需手动启动后端 (`python api.py`) 和前端 (`npm run dev`) 后执行：
```bash
cd D:/coding/fundata && python -m pytest tests/test_portfolio_e2e.py -v --headed
```
截图保存至 `sessions/playwright_portfolio_screenshots/`
