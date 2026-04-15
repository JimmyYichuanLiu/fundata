# Session 003 — 数据迁移：zx_fund.db → fund_data.db

**日期**：2026-04-16  
**状态**：完成

---

## 本次目标

将 `zx_fund.db` 中的两张表（`zx_fund_product`、`zx_fund_nav`）数据迁移到 `fund_data.db`，写成独立迁移程序。要求：
- 以 `fund_code` 识别两边的同名基金
- 冲突时以 zx 数据为准
- 追踪所有冲突并记录
- 迁移过程中 `zx_fund.db` 绝对不能被修改（只读）
- TDD：先写测试，再实现

---

## 决策汇总

| 问题 | 决策 |
|------|------|
| 被覆盖行的 source_id | **方案 A**：清为 NULL，`data_source='zx_excel'`（丢失邮件溯源） |
| adj_nav 重新计算范围 | **方案 B**：对所有受影响基金全系列重新计算 |

---

## 已完成的工作

### 1. TDD：先写测试

**`tests/test_db_merger.py`**（55 个测试）

测试使用完全合成的内存数据库，不依赖任何真实文件。三只测试基金：

| 代码 | 场景 |
|------|------|
| `T_OVERLAP1` | 两边都有，存在元数据和净值冲突 |
| `T_EMAIL_ONLY1` | 仅在 fund_data.db，迁移后完全不变 |
| `T_ZX_ONLY1` | 仅在 zx_fund.db，需新插入 |

| 测试类 | 测试数 | 覆盖内容 |
|--------|--------|----------|
| `TestInitConflictTable` | 2 | 表存在、有必要列 |
| `TestMergeFunds` | 13 | 计数、新插入、元数据更新、created_at保留、email专属不变、冲突记录 |
| `TestMergeNavData` | 18 | 总计数、早期日期插入、ZX专属插入、fund_id重新映射、data_source/source_id、冲突记录、同值不记冲突、email行不变、无重复 |
| `TestAdjNavRecalculation` | 8 | first=1.0、中间行、末行精确值、email专属不变、全部为正 |
| `TestZxDatabaseReadOnly` | 4 | zx表行数不变、数值不变 |
| `TestMigrationStats` | 6 | stats结构、new/updated/inserted/updated/conflicts计数 |
| `TestIdempotency` | 5 | 二次运行行数不变、冲突表清空 |

### 2. 实现迁移脚本

**`db_merger.py`**

| 函数 | 说明 |
|------|------|
| `init_conflict_table(fd_conn)` | DROP + CREATE `migration_conflicts`（每次运行清空，保证幂等） |
| `merge_funds(fd_conn, zx_conn)` | zx_fund_product → funds；新建或更新元数据（不覆盖 created_at）；记录字段级冲突 |
| `_recalculate_adj_nav(fd_conn, fund_code)` | 对单只基金全系列重算 adj_nav（与 zx_importer 公式相同） |
| `merge_nav_data(fd_conn, zx_conn)` | zx_fund_nav → fund_nav_data；fund_id 重新从 funds 表查找；INSERT新行/UPDATE已有行；记录 unit_nav 差异；对受影响基金调用重算 |
| `run_migration(fund_data_db, zx_fund_db)` | 主入口；zx 以 URI 只读打开；整体事务包裹；返回统计字典 |

**关键设计点**：
- `zx_fund.db` 以 `sqlite3.connect("file:path?mode=ro", uri=True)` 打开，操作系统级只读
- fund_id 不从 zx 复制，而是在 fund_data.db 的 `funds` 表中按 fund_code 重新查找（两库 AUTOINCREMENT 序列独立）
- 冲突表每次迁移开头 DROP + CREATE，确保二次运行后冲突表体现当前状态（全为空，因为 zx 已赢过一次）
- adj_nav 只对受影响基金重算，EMAIL_ONLY 基金不重算、不变

### 3. 测试结果

**55 个测试全部通过**，完整套件 119 个测试全部通过。

### 4. 在真实数据库上执行迁移

```
Funds : new=263, updated=10
NAV   : inserted=56210, updated=435, value_conflicts=25
```

| 指标 | 迁移后 |
|------|--------|
| funds 表行数 | 321（原58 + 新263） |
| fund_nav_data 行数 | 62,720 |
| email 保留净值行 | 6,075（435条被 zx 覆盖） |
| zx_excel 净值行 | 56,645（56,210新增 + 435覆盖） |
| 基金级别冲突 | 82 条（10只重叠基金，含 fund_name、strategy 等字段差异） |
| 净值数值冲突 | 25 条 |
| migration_conflicts 总计 | 107 条 |

---

## 新增/修改文件清单

```
db_merger.py                              ← 新增：迁移脚本
tests/test_db_merger.py                   ← 新增：55 个迁移测试
sessions/session_003_data_migration.md    ← 本文件
```

---

## 关键设计决策

1. **只读打开 zx_fund.db**：SQLite URI 模式 `mode=ro`，操作系统级保护，不可能意外写入。
2. **fund_id 重新映射**：两库 AUTOINCREMENT 序列完全独立，zx 的 fund_id 对 fund_data.db 毫无意义；必须按 fund_code JOIN 查找 fund_data.db 中真实的 fund_id。
3. **Option A（source_id 清空）**：被 zx 覆盖的净值行，source_id 设 NULL、data_source 改 'zx_excel'，与新插入行行为一致；email 邮件溯源信息不再可追溯，但简化了 data_source 语义。
4. **adj_nav 全系列重算**：只要基金有任一净值行被插入或更新，整个序列从头重算；保证序列内部一致性。
5. **冲突表幂等**：DROP + CREATE 而非 DELETE，二次运行时冲突表必为空（zx 已赢，值相同，无差异可记）。

---

## 下一步方向

- **API 更新**：`api.py` 中仍使用旧中文列名（`产品代码`、`净值日期` 等），需全面更新为新英文列名
- **前端影响**：API 返回字段变更后，前端字段引用也需对应更新
- **migration_conflicts 暴露**：可考虑在 API/前端提供冲突查看入口，方便人工核查 25 条净值数值冲突
