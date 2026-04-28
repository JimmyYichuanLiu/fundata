# 指标公式统一重构

日期：2026-04-28

## 改动文件
- `web/src/utils/formulae.js`（新建）：所有指标公式的纯函数库
- `web/src/utils/metrics.js`（重构）：改为调用 formulae.js
- `web/src/utils/metricDefs.js`（更新）：补充 Beta进攻/防守、年化超额收益指标定义
- `web/src/pages/fund-detail/MetricsTab.jsx`（更新）：Beta进攻/防守显示修正
- `web/src/pages/fund-comparison/ComparisonMetrics.jsx`（更新）：rf 说明文字修正

## 核心变更
1. 无风险收益率：2.5% → 1.75%
2. 年化收益：保留几何（默认）和线性两种模式
3. 下行风险：改为 STD.S(负收益序列) × √250（样本标准差）
4. Beta：新增进攻/防守拆分
5. Alpha：rf 直接用年化值，不再日频折算
6. 所有公式集中在 formulae.js，便于审查和维护

## 公式约定
- 1年 = 250交易日 = 50周
- 无风险收益率 RF = 1.75%
- 净值类型：复权净值（adj_nav），fallback unit_nav
