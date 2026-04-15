# 数据库合并设计讨论

> 目标：将 `zx_fund.db` 的数据导入 `fund_data.db`，统一两边的表格格式。  
> 本文档用于在动手之前对齐设计决策。

---

## 一、`funds` vs `zx_fund_product` 字段对比

| 字段含义 | `funds`（fund_data.db） | `zx_fund_product`（zx_fund.db） | 差异说明 |
|----------|------------------------|--------------------------------|----------|
| 自增主键 | `fund_id` INTEGER PK | `id` INTEGER PK | 名称不同，逻辑相同 |
| 基金代码（唯一键） | `产品代码` TEXT NOT NULL UNIQUE | `fund_code` TEXT NOT NULL UNIQUE | 中文 vs 英文列名 |
| 基金名称 | `产品名称` TEXT | `fund_name` TEXT | 中文 vs 英文列名 |
| 首次录入时间 | `首次录入时间` DATETIME | `created_at` DATETIME | 中文 vs 英文列名 |
| 一级策略标签 | `strategy_l1` TEXT | `strategy_l1` TEXT | ✅ 完全一致 |
| 二级策略标签 | `strategy_l2` TEXT | `strategy_l2` TEXT | ✅ 完全一致 |
| 三级策略标签 | ❌ 缺失 | `strategy_l3` TEXT | `funds` 需补列 |
| 管理人 | ❌ 缺失 | `manager` TEXT | `funds` 需补列 |
| 托管机构 | ❌ 缺失 | `custodian` TEXT | `funds` 需补列 |
| 成立日期 | ❌ 缺失 | `inception_date` TEXT | `funds` 需补列 |
| 运作起始日 | ❌ 缺失 | `start_date` TEXT | `funds` 需补列 |
| 对标指数 | `benchmark_index` TEXT | `benchmark` TEXT | **列名不同**，语义相同 |
| 对外展示 | ❌ 缺失 | `display` TEXT | `funds` 需补列 |

**结论**：`funds` 比 `zx_fund_product` 少 6 个字段；对标指数列名不一致；其余有对应关系的字段为中文 vs 英文命名。

---

### 【待决策 A】`funds` 表需要补充哪些字段？

> 请在下方填写你的意见，或直接修改这张表。

| 字段 | 是否加入 `funds` | 备注/你的意见 |
|------|----------------|--------------|
| `strategy_l3`（三级策略） | | |
| `manager`（管理人） | | |
| `custodian`（托管机构） | | |
| `inception_date`（成立日期） | | |
| `start_date`（运作起始日） | | |
| `display`（对外展示） | | |

---

### 【待决策 B】对标指数列名如何统一？

`funds` 现在叫 `benchmark_index`，`zx_fund_product` 叫 `benchmark`。

> 你的意见：

---

---

## 二、`fund_nav_data` vs `zx_fund_nav` 字段对比

| 字段含义 | `fund_nav_data`（fund_data.db） | `zx_fund_nav`（zx_fund.db） | 差异说明 |
|----------|--------------------------------|----------------------------|----------|
| 自增主键 | `id` INTEGER PK | `id` INTEGER PK | ✅ 一致 |
| 基金外键 | `fund_id` INTEGER → `funds.fund_id` | `fund_code` TEXT → `zx_fund_product.fund_code` | 一个用整数 FK，一个用字符串 code |
| 基金代码（冗余列） | `产品代码` TEXT（反规范化） | ❌ 无 | `fund_nav_data` 多一列冗余代码 |
| 基金名称（冗余列） | `产品名称` TEXT（反规范化） | ❌ 无 | `fund_nav_data` 多一列冗余名称 |
| 净值日期 | `净值日期` TEXT，格式 **`YYYYMMDD`** | `nav_date` TEXT，格式 **`YYYY-MM-DD`** | ⚠️ **日期格式不同** |
| 单位净值 | `单位净值` REAL NOT NULL | `unit_nav` REAL | 中文 vs 英文；NOT NULL 约束不同 |
| 累计净值 | `累计单位净值` REAL | `accum_nav` REAL | 中文 vs 英文列名 |
| 复权净值 | `adjusted_nav` REAL | `adj_nav` REAL | **列名不同** |
| 邮件来源 | `source_id` INTEGER → `email_sources.id` | ❌ 无 | `fund_nav_data` 专属，邮件溯源用 |
| 录入时间 | `录入时间` DATETIME | ❌ 无 | `fund_nav_data` 专属 |
| 唯一约束 | UNIQUE(`产品代码`, `净值日期`) | UNIQUE(`fund_code`, `nav_date`) | 逻辑相同，字段不同 |

**现有数据量**：`fund_nav_data` 6,510 行（来自邮件）；`zx_fund_nav` 56,645 行（来自 ZXdatabase.xlsx）

**关键差异汇总**：
1. **日期格式**：`YYYYMMDD` vs `YYYY-MM-DD`，导入时必须转换其中一边
2. **复权净值列名**：`adjusted_nav` vs `adj_nav`
3. zx 数据导入后 `source_id` / `录入时间` 两列填 `NULL`，以区别于邮件来源记录

---

### 【待决策 C】日期格式统一为哪种？

两个选项：
- **方案 C1**：保留 `YYYYMMDD`（现有 `fund_nav_data` 格式不变，zx 数据导入时转换）
- **方案 C2**：改为 `YYYY-MM-DD`（更标准，但需迁移现有 6,510 行数据）

> 你的意见：

---

### 【待决策 D】复权净值列名统一为哪个？

两个选项：
- **方案 D1**：保留 `adjusted_nav`（现有列名不变）
- **方案 D2**：改为 `adj_nav`（与 zx 一致，更简短）

> 你的意见：

---

### 【待决策 E】`fund_nav_data` 的冗余列（`产品代码`、`产品名称`）如何处理？

zx 数据没有这两列，导入时：
- **方案 E1**：`产品代码` 从 `funds` 表 join 填入，`产品名称` 同理（保持现有结构不变）
- **方案 E2**：这两列标记为历史遗留，新数据只写 `fund_id`，不再填冗余列

> 你的意见：

---

---

## 三、决策汇总（待填写）

> 决策如下：
`funds` 与 `zx_fund_product` 格式统一

| 字段含义 | 两个数据库一致列| 修改说明 |
|----------|------------------------|--------------------------------|----------|
| 自增主键 | `fund_id` INTEGER PK | 改成同一名字 |
| 基金代码（唯一键） | `fund_code` TEXT NOT NULL UNIQUE | 改成同一名字 |
| 基金名称 | `fund_name` TEXT | 改成同一名字 |
| 首次录入时间 | `created_at` DATETIME | 改成同一名字 |
| 一级策略标签 | `strategy_l1` TEXT | 维持不变 |
| 二级策略标签 | `strategy_l2` TEXT | 维持不变 |
| 三级策略标签 |  `strategy_l3` TEXT | `funds` 需补列 |
| 管理人 |  `manager` TEXT | `funds` 需补列 |
| 托管机构 |  `custodian` TEXT | `funds` 需补列 |
| 成立日期 |  `inception_date` TEXT | `funds` 需补列 |
| 运作起始日 | `start_date` TEXT | `funds` 需补列 |
| 对标指数 | `benchmark_index` TEXT | 改成同一名字|
| 对外展示 | `display` TEXT | `funds` 需补列 |

`fund_nav_data` vs `zx_fund_nav` 格式统一

| 字段含义 | 两个数据库一致列 | 修改说明 |
|----------|--------------------------------|----------------------------|----------|
| 自增主键 | `id` INTEGER PK | ✅ 一致 |
| 基金外键 | `fund_id` INTEGER → `funds.fund_id`| 都用整数 |
| 基金代码（冗余列） | `fund_code` TEXT（反规范化） |`zx_fund_nav`新增这一列，从`zx_fund_product`中获取 |
| 基金名称（冗余列） | `fund_name` TEXT（反规范化） |`zx_fund_nav`新增这一列，从`zx_fund_product`中获取 |
| 净值日期 | `nav_date` TEXT，格式 **`YYYY-MM-DD`** | `funds` 需修改 |
| 单位净值 | `unit_nav` REAL NOT NULL | 两边表格均修改 |
| 累计净值 | `accum_nav` REAL | `funds` 需修改 |
| 复权净值 | `adj_nav` REAL | `funds` 需修改 |
| 邮件来源 | `source_id` INTEGER → `email_sources.id` | ❌ 无 | `fund_nav_data` 专属，邮件溯源用 |
| 录入时间 | `录入时间` DATETIME |`zx_fund_nav`新增这一列 |
| 唯一约束 | UNIQUE(`fund_code`, `nav_date`) | 根据数据库名称修改字段 |



