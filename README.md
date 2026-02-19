# 基金净值数据采集系统

自动从 163 邮箱中拉取 Excel 附件，智能提取基金净值数据并存入 SQLite 数据库，支持数据质量检测和 Excel 导出。

---

## 目录结构

```
emailcontent/
├── get_163_email.py        # 主程序：连接邮箱、提取附件、写入数据库
├── smart_extractor.py      # 核心库：智能识别 Excel 格式并提取数据
├── data_quality_check.py   # 质检脚本：异常检测 + 生成对外展示库
├── organize_fund_data.py   # 工具脚本：将数据库导出为 Excel 文件
├── requirements.txt        # Python 依赖
├── .env                    # 本地配置（已 gitignore，含邮箱密码）
├── .env.example            # 配置模板
└── .gitignore
```

> `fund_data.db`、`fund_clean.db`、`email_attachments/` 均已 gitignore，不进入版本控制。

---

## 数据流

```
163 邮箱 (IMAP)
       │
       ▼
get_163_email.py          ← 增量拉取（基于 IMAP UID，不重复处理）
       │
       ├─ 提取 Excel 附件（BytesIO，不落磁盘）
       │
       ▼
smart_extractor.py        ← 自动识别表格格式，提取 5 个核心字段
       │
       ▼
fund_data.db              ← 原始库（4张表，见下方 Schema）
       │
       ▼
data_quality_check.py     ← 三类异常检测
       │
       ▼
fund_clean.db             ← 校准库（排除异常，来源信息内联，对外展示）
```

---

## 快速开始

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

### 2. 配置环境变量

复制模板并填入真实信息：

```bash
cp .env.example .env
```

编辑 `.env`：

```
DB_PATH=fund_data.db
CLEAN_DB_PATH=fund_clean.db
EMAIL_USER=your_email@163.com
EMAIL_PASSWORD=your_imap_auth_code
```

> `EMAIL_PASSWORD` 填写的是 **IMAP 授权码**，不是登录密码。
> 获取路径：163 邮箱 → 设置 → POP3/SMTP/IMAP → 开启 IMAP → 生成授权码。

### 3. 拉取邮件数据

```bash
python get_163_email.py
```

首次运行全量扫描所有邮件，此后每次只处理新邮件（增量模式）。

### 4. 数据质量检测

```bash
python data_quality_check.py
```

输出检测报告并生成 `fund_clean.db`。

### 5. 导出 Excel（可选）

```bash
python organize_fund_data.py
```

生成 `fund_data_organized.xlsx`，包含汇总 Sheet 和每个产品的独立 Sheet。

---

## 各文件详解

### `get_163_email.py` — 主程序

| 函数 | 说明 |
|------|------|
| `init_database(db_path)` | 初始化 SQLite，创建全部表，对已有库执行迁移 |
| `connect_and_fetch_email(...)` | 主流程：连接 → 登录 → 增量拉取 → 解析 → 写库 |
| `extract_excel_attachments(msg, ...)` | 从邮件中提取 Excel 附件（内存操作，支持多 Sheet） |
| `insert_email_source(conn, ...)` | 将邮件元数据写入 `email_sources`，返回 `source_id` |
| `insert_data_to_db(conn, df, ..., source_id)` | 批量插入净值数据，附带来源 ID |
| `get_sync_state / save_sync_state` | 读写 IMAP UID 同步位点，实现增量拉取 |
| `log_extraction_failure(...)` | 将提取/插入失败记录持久化到 `extraction_failures` |
| `query_and_display_data(conn)` | 打印数据库统计摘要 |

**增量机制**：每次成功处理后保存 `last_uid` 和 `uidvalidity` 到 `sync_state` 表。下次运行只拉取 UID 更大的新邮件。若检测到邮箱被重建（`uidvalidity` 变化），自动降级为全量扫描。

---

### `smart_extractor.py` — 智能提取引擎

对外只需调用一个函数：

```python
from smart_extractor import extract_and_normalize
import pandas as pd

df = pd.read_excel('attachment.xlsx', header=None)
records = extract_and_normalize(df)
# 返回 list of dict，每条包含 5 个核心字段
```

**支持的 Excel 格式**

| 格式 | 说明 | 示例产品 |
|------|------|---------|
| 标准表格 | 第 0 行为表头，第 1 行起为数据 | 大多数基金 |
| 多行标题表格 | 第 0 行为大标题，第 1 行为表头，第 2 行起为数据 | 东恺百会系列 |
| 键值对（纵向） | 左列为字段名，右列为值 | 东恺系列 |
| 表头含换行符 | 表头单元格内含 `\n`，清洗后匹配 | 利幄系列 |

**提取字段与关键字别名**

| 标准字段 | 识别的关键字变体 |
|---------|----------------|
| 产品名称 | 产品名称、基金名称、名称、FundName |
| 产品代码 | 产品代码、基金代码、协会备案编码、FundFillingCode 等 |
| 净值日期 | 净值日期、日期、估值基准日、NAVAsOfDate |
| 单位净值 | 单位净值、基金份额净值、实际净值、NAV/Share 等 |
| 累计单位净值 | 累计单位净值、基金份额累计净值、实际累计净值 等 |

**提取逻辑**：先尝试表格格式（扫描前 5 行寻找含 ≥2 个关键字的表头），失败则尝试键值对格式（全表扫描，含防误匹配逻辑，如"客户名称"不会错误匹配"名称"字段）。

---

### `data_quality_check.py` — 数据质检

每次运行会：
1. 对 `fund_data.db` 执行三类检测，打印报告
2. 重建 `fund_clean.db`（先删后建，确保幂等）

**检测项**

| 检测 | 规则 | 处理方式 |
|------|------|---------|
| 净值超范围 | 单位净值或累计单位净值 > 5 | 报告 + 排除出 clean DB |
| 同名多代码 | 相同产品名称对应不同产品代码 | 报告 + 追溯来源邮件 |
| 重复净值日期 | 同产品代码同日期多条记录 | 报告（正常不会触发，UNIQUE 约束兜底） |

**`fund_clean.db` 特点**

- 排除所有异常记录（净值 > 5）
- 对重复记录只保留最早插入的（`MIN(id)`）
- 来源信息**反范式化**：邮件主题、发件人、附件名等直接内联到每条记录，无需 JOIN

---

### `organize_fund_data.py` — Excel 导出工具

从 `fund_data.db` 读取所有数据，生成 `fund_data_organized.xlsx`：

- **汇总 Sheet**：每个产品的代码、名称、记录数、日期范围、最早/最新净值
- **各产品 Sheet**：每个产品代码单独一个 Sheet，按日期升序排列

---

## 数据库 Schema

### `fund_data.db`（原始库）

**`email_sources`** — 邮件来源

| 字段 | 类型 | 说明 |
|------|------|------|
| id | INTEGER PK | 自增主键 |
| 邮件主题 | TEXT | |
| 邮件发件人 | TEXT | |
| 邮件日期 | TEXT | |
| 附件文件名 | TEXT | |
| sheet名称 | TEXT | |
| 记录时间 | DATETIME | 写入时间 |

**`fund_nav_data`** — 基金净值

| 字段 | 类型 | 说明 |
|------|------|------|
| id | INTEGER PK | 自增主键 |
| 产品名称 | TEXT | |
| 产品代码 | TEXT NOT NULL | |
| 净值日期 | TEXT NOT NULL | YYYYMMDD 格式 |
| 单位净值 | REAL NOT NULL | |
| 累计单位净值 | REAL | 可为空 |
| 插入时间 | DATETIME | |
| source_id | INTEGER | 外键 → email_sources.id |
| — | UNIQUE | (产品代码, 净值日期) |

> 2025 年前的历史数据 `source_id` 为 NULL（迁移前已存在），属正常现象。

**`sync_state`** — IMAP 同步位点

| key | value |
|-----|-------|
| last_uid | 上次处理到的最大 IMAP UID |
| uidvalidity | 邮箱 UIDVALIDITY，变化则触发全量扫描 |

**`extraction_failures`** — 提取失败记录

记录所有无法识别格式或插入失败的附件，含失败原因，用于排查。

---

### `fund_clean.db`（校准库）

`fund_nav_data` 表，结构与原始库基本一致，额外内联来源字段：

| 额外字段 | 说明 |
|---------|------|
| 来源邮件主题 | |
| 来源发件人 | |
| 来源邮件日期 | |
| 来源附件文件名 | |
| 来源sheet名称 | |

---

## 依赖说明

```
pandas>=1.5.0       # DataFrame 处理
openpyxl>=3.0.0     # 读写 .xlsx
xlrd>=2.0.0         # 读取旧版 .xls
python-dotenv>=1.0.0 # 加载 .env 配置
```
