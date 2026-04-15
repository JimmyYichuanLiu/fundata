# Session 001 — 臻选基金 Excel 导入模块

**日期**：2026-04-15  
**状态**：完成

---

## 本次目标

阅读 demo 文件夹中的历史系统文档（`zxdemo-workflow.docx`、`ZXdatabase.xlsx`、`臻选货架.xlsx`、`step3 整理生数据.py`），提取 Excel 读取逻辑，在 fundata 项目中以 TDD 方式复现，生成独立的 SQLite 数据库。

---

## 已完成的工作

### 1. 阅读与分析（只读，未写代码）
- 通读 `demo/zxdemo-workflow.docx`：系统架构、四张数据库表、复权净值公式、周频对齐逻辑
- 分析 `demo/ZXdatabase.xlsx`：280 个 Sheet，每 Sheet = 一只基金，3列（date / unit_value / accumulated_value）
- 分析 `demo/臻选货架.xlsx`：276 行，22 列，Code_Id 在第 19 列，Start_date 在第 21 列
- 阅读 `demo/step3 整理生数据.py`：6种 Excel 格式识别逻辑（招商/海通/中信建投/国金/横向表格/竖格式）+ COLUMN_MAP

### 2. 生成规范文档
- **`demo/ZX_EXCEL_SPEC.md`**：完整记录两个 Excel 的结构、Code_Id 生成规则、复权净值公式（含公式推导纠错）、数据流、与 fundata 现有架构的映射关系

### 3. 新建数据库 + 实现导入模块（TDD）
先写测试，再写实现：

**`tests/test_zx_importer.py`**（27 个测试，全部通过）

| 测试类 | 覆盖内容 |
|--------|----------|
| `TestComputeAdjNav`（6项） | 空输入、单行、无分红跟踪、有分红总回报、零除进位、多次分红复利 |
| `TestReadShelf`（8项） | 数量范围、字段完整性、start_date 格式与优先级、已知排除代码不在白名单 |
| `TestImportZxExcel`（13项） | stats 结构、导入数量、NAV 总量、非白名单排除、列 schema、adj_nav 初始值=1、adj_nav 全正、首/末基金各5行逐列比对 Excel 原值、无分红基金复权公式交叉验证、无重复行、日期升序 |

**`zx_importer.py`**（主模块）

| 函数 | 说明 |
|------|------|
| `init_zx_database(db_path)` | 建表：`zx_fund_product` + `zx_fund_nav` |
| `read_shelf(shelf_path)` | 读 `臻选货架.xlsx`，返回 `{code_id: {...}}` 白名单字典 |
| `compute_adj_nav(unit_navs, accum_navs)` | 复权净值计算，正确公式：`adj[i] = adj[i-1] * (unit[i-1] + accum[i] - accum[i-1]) / unit[i-1]` |
| `import_zx_excel(db_path, zxdb_path, shelf_path)` | 完整导入流程，返回 stats dict |
| CLI | `python zx_importer.py` 直接生成 `zx_fund.db` |

### 4. 生成正式数据库
- **`zx_fund.db`**：273 只基金 / 56,645 条净值记录（7只不在白名单，跳过）

---

## 新增文件清单

```
zx_importer.py                   ← 核心导入模块
zx_fund.db                       ← 生成的 SQLite 数据库（已 gitignore）
demo/ZX_EXCEL_SPEC.md             ← Excel 结构与业务逻辑规范文档
tests/__init__.py
tests/conftest.py                 ← 将项目根加入 sys.path
tests/test_zx_importer.py        ← 27 个测试
sessions/session_001_zx_excel_importer.md   ← 本文件
```

---

## 关键设计决策

1. **独立数据库**：使用 `zx_fund.db`，不修改现有 `fund_data.db`
2. **复权净值公式纠错**：`ZX_EXCEL_SPEC.md` 中的简化公式有笔误（写成了 `unit_nav[i]` 而非 `unit_nav[i-1]`），实现时用从步骤式公式推导的正确版本
3. **白名单过滤**：ZXdatabase 的 280 个 Sheet 中，只有在 `臻选货架.xlsx` 的 `Code_Id` 列中存在的才导入
4. **accum_nav 缺失兜底**：若累计净值缺失则回退到单位净值（等价于无分红）
5. **ON CONFLICT UPSERT**：重复导入是幂等的

---

## 下一步方向（待规划）

- 为 `zx_fund.db` 添加 API 端点（参考现有 `api.py` 结构）
- 前端页面：基金列表 + 净值图表（复权/不复权切换）
- 性能指标计算（区间收益、年化、最大回撤、Sharpe）并存入 DB 或按需计算
- 周频对齐逻辑（可选，参考 `ZX_EXCEL_SPEC.md` 第5节）
