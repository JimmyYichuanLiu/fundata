# metrics_formula_tests.md

## 背景

本次为 `formulae.js` 和 `metrics.js` 的指标公式重构补齐单元测试与集成测试。

## 新增文件

- `web/src/utils/formulae.test.js` — 72 个单元测试，覆盖 formulae.js 所有导出函数
- `web/src/utils/metrics.test.js` — 30 个集成测试，覆盖 metrics.js 三个主要函数

## 测试框架

安装 `vitest@4.1.5`，新增 npm scripts：
- `npm test` → `vitest --run`
- `npm run test:coverage` → `vitest --run --coverage`

## 覆盖的测试点

### formulae.js 单元测试

| 函数 | 测试数 | 覆盖要点 |
|------|--------|---------|
| RF 常量 | 1 | 值为 0.0175 |
| ANNUAL_DAYS / ANNUAL_WEEKS | 2 | 250 / 50 |
| periodReturn | 5 | 正收益、负收益、零变化、startNav=0、startNav=null |
| annualizedReturn | 6 | geometric/linear 两种模式、n=1 边界、默认模式 |
| sampleStd | 6 | 标准样本、两相同值、空数组、单元素、null、[0,1] |
| annualizedVol | 2 | 基本计算、全零收益 |
| annualizedDownsideRisk | 3 | 无负收益→null、仅一个负收益→null、两个负收益正常计算 |
| maxDrawdown | 6 | 单调上涨、单次下跌、峰谷、多次取最大、空数组、单元素 |
| beta | 5 | fund=bench→1、fund=2×bench→2、fund=-bench→-1、n<2→null、bench常数→null |
| betaOffensive | 2 | 仅上涨期计算、无上涨期→null |
| betaDefensive | 2 | 仅下跌期计算、无下跌期→null |
| alpha | 3 | CAPM公式验证、beta=0、fund=bench |
| sharpe | 4 | 基本计算、vol=0→null、vol=null→null、负超额收益 |
| sortino | 3 | 基本计算、downsideRisk=0→null、null→null |
| calmar | 4 | 基本计算、maxDD=0→null、null→null、正maxDD→null |
| arithmeticExcess | 2 | 正超额、负超额 |
| geometricExcess | 3 | 基本计算、分母=0→0、fund=bench→0 |
| excessAnnualized | 3 | geometric/linear、n=1→0 |
| excessSharpe | 3 | 基本计算、vol=0→null、null→null |
| monthlyWinRate | 7 | 全涨→1、全跌→0、2/3胜率、多条目取最后、单条目→null、null→null、缺日期跳过 |

### metrics.js 集成测试

| 函数 | 测试数 | 覆盖要点 |
|------|--------|---------|
| computeMetrics | 12 | null/空→null、periodReturn正负、annualizedReturn n<30→null、maxDrawdown、monthlyWinRate、annualizedVol非负、sharpe/calmar边界、navType三种 |
| computeBenchmarkMetrics | 7 | null→null、<3公共日期→null、fund=bench→beta=1、fund=2×bench→beta=2、betaOffensive/betaDefensive字段存在、无上涨期→null、无下跌期→null |
| computeExcessMetrics | 6 | null→null、fund=bench→periodExcess≈0、annualizedExcessReturn字段、正超额、arithmetic/geometric模式、excessSharpe n<30→null、excessVol非负 |

## 运行结果

```
Test Files  2 passed (2)
     Tests  102 passed (102)
  Duration  430ms
```

**全部 102 个测试通过，0 失败。**

## 修正记录

测试编写过程中发现一处预期值错误：
- `sampleStd([2, 4, 4, 4, 5, 5, 7, 9])` 的教科书例子给出总体标准差=2（÷n），但 `sampleStd` 实现为样本标准差（÷n-1），正确值为 `sqrt(32/7) ≈ 2.1381`。已修正测试期望值，函数实现本身正确。

## 风险点

无。所有公式实现与测试一致，边界条件（null、空数组、n<2、零方差）均有覆盖。
