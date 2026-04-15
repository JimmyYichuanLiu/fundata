# Session 002 — 数据库 Schema 统一

**日期**：2026-04-15  
**状态**：完成

---

## 本次目标

将 `fund_data.db` 与 `zx_fund.db` 的对应表格格式统一，使两边的列名、数据类型、日期格式完全一致，为后续数据迁移（合并）做好准备。数据迁移本身推迟到下一个 session。

---

## 决策汇总

### `funds` 表（fund_data.db）

| 变更 | 说明 |
|------|------|
| `产品代码` → `fund_code` | 改为英文列名 |
| `产品名称` → `fund_name` | 改为英文列名 |
| `首次录入时间` → `created_at` | 改为英文列名 |
| 新增 `strategy_l3` `manager` `custodian` `inception_date` `start_date` | 补列，现有行填 NULL |
| 新增 `display` | 现有行填 `'展示'` |
| `benchmark_index` | 原本已是英文，保持不变 |

### `fund_nav_data` 表（fund_data.db）

| 变更 | 说明 |
|------|------|
| `产品代码` → `fund_code` | 改为英文列名 |
| `产品名称` → `fund_name` | 改为英文列名 |
| `净值日期` → `nav_date` | 改为英文，**日期格式同时从 `YYYYMMDD` 转为 `YYYY-MM-DD`**（6,510 行全部转换） |
| `单位净值` → `unit_nav` | 改为英文列名 |
| `累计单位净值` → `accum_nav` | 改为英文列名 |
| `adjusted_nav` → `adj_nav` | 与 zx_fund_nav 统一 |
| `录入时间` | 保持中文列名不变 |
| 新增 `data_source` | `source_id IS NOT NULL` → `'email'`；`source_id IS NULL` → `'manual'` |

### `zx_fund_product` 表（zx_fund.db）

| 变更 | 说明 |
|------|------|
| `id` → `fund_id` | 主键改名与 `funds.fund_id` 一致 |
| `benchmark` → `benchmark_index` | 与 `funds.benchmark_index` 统一 |
| 其余列 | 原本已是英文，保持不变 |

### `zx_fund_nav` 表（zx_fund.db）

| 变更 | 说明 |
|------|------|
| `fund_code` TEXT FK → `fund_id` INTEGER FK | 改为整数外键，指向 `zx_fund_product.fund_id` |
| 新增 `fund_code` TEXT（冗余列） | 从 `zx_fund_product` join 填入 |
| 新增 `fund_name` TEXT（冗余列） | 从 `zx_fund_product` join 填入 |
| 新增 `source_id` | NULL（zx 数据无邮件溯源） |
| 新增 `录入时间` | NULL |
| 新增 `data_source` | `'zx_excel'` |
| UNIQUE 约束 | 改为 `UNIQUE(fund_id, nav_date)` |

---

## 已完成的工作

### 1. TDD：先写测试，再实现

**`tests/test_db_migration.py`**（37 个测试，全部通过）

| 测试类 | 覆盖内容 |
|--------|----------|
| `TestFundsMigration`（8项） | 列存在、旧中文列消失、行数保持、display='展示'、新列为 NULL、benchmark_index 保留、UNIQUE 约束、fund_code 非空 |
| `TestFundNavDataMigration`（10项） | 列存在、旧列消失、行数保持、YYYY-MM-DD 格式、无旧格式残留、email/manual data_source、nav_date 非空、UNIQUE 约束、unit_nav 非空 |
| `TestZxFundProductMigration`（8项） | fund_id 列存在、id 消失、benchmark_index 存在、benchmark 消失、所有列、行数保持、fund_code UNIQUE、fund_id 是主键 |
| `TestZxFundNavMigration`（11项） | fund_id INTEGER、fund_code/fund_name 冗余列、所有列、FK 完整性、fund_code 与产品表匹配、data_source='zx_excel'、source_id 全为 NULL、行数保持、无重复行、日期升序、日期格式不变 |

### 2. 实现迁移脚本

**`db_schema_migrate.py`**

| 函数 | 说明 |
|------|------|
| `migrate_fund_data_db(db_path)` | 迁移 `funds` 和 `fund_nav_data`，事务包裹，幂等（检测已迁移则跳过） |
| `migrate_zx_fund_db(db_path)` | 迁移 `zx_fund_product` 和 `zx_fund_nav`（顺序固定，nav 依赖 product 先迁移），幂等 |
| CLI | `python db_schema_migrate.py` 可直接运行 |

**幂等性**：每个内部函数在执行前检查目标列是否已存在（如 `fund_code`、`nav_date`、`fund_id`），若已迁移则直接跳过。

### 3. 在真实数据库上执行迁移

- `fund_data.db`：58 只基金 / 6,510 条净值记录，迁移成功
- `zx_fund.db`：273 只基金 / 56,645 条净值记录，迁移成功

### 4. 更新 `zx_importer.py`

- `init_zx_database`：建表改为新 schema（`fund_id` PK、`benchmark_index`、nav 表用 INTEGER `fund_id`）
- `import_zx_excel`：upsert 改为 `benchmark_index`；写 nav 前先 SELECT fund_id；写入 `fund_id`、`fund_code`（冗余）、`fund_name`（冗余）、`data_source='zx_excel'`

### 5. 更新 `tests/test_zx_importer.py`

- `test_fund_product_has_expected_columns`：`benchmark` 改为 `benchmark_index`，加入 `fund_id`
- `test_fund_nav_has_expected_columns`：加入 `fund_id`、`fund_name`、`data_source`

**最终测试结果：64 个测试全部通过**（37 migration + 27 zx_importer）

---

## 新增/修改文件清单

```
db_schema_migrate.py                    ← 新增：迁移脚本
tests/test_db_migration.py             ← 新增：37 个迁移测试
zx_importer.py                         ← 修改：适配新 schema
tests/test_zx_importer.py              ← 修改：schema 列名更新
sessions/session_002_db_schema_unification.md  ← 本文件
```

---

## 关键设计决策

1. **迁移方式**：SQLite 不支持直接重命名列（3.25 以下）或修改数据，全部采用"新建表→复制数据→删旧表→重命名"的 recreate-and-copy 模式。
2. **日期转换**：`YYYYMMDD` → `YYYY-MM-DD` 通过 SQL `substr` 函数在迁移 SQL 内完成，无需 Python 循环。
3. **data_source**：区分三种数据来源：`'email'`（邮件导入）/ `'manual'`（手动录入）/ `'zx_excel'`（Excel 导入），取代原来模糊的 `source_id IS NULL` 判断。
4. **幂等性**：迁移函数检测目标列是否已存在，已迁移则跳过。使测试套件在迁移后仍可重复运行。
5. **zx_fund_nav 外键**：从 TEXT `fund_code` 改为 INTEGER `fund_id`，与 `fund_nav_data` 使用整数 FK 的方式统一；`fund_code` / `fund_name` 作为冗余列保留用于 JOIN 和展示。

---

## 下一步方向

- **数据迁移**：将 `zx_fund.db` 的数据导入 `fund_data.db`（两张表都已格式统一，可开始合并）
- **API 更新**：`api.py` 中仍使用旧中文列名查询，需全面更新为新英文列名
- **前端影响**：API 返回字段改变后，前端字段引用（`产品代码`、`净值日期` 等）也需对应更新
