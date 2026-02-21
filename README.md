# 基金净值数据采集与可视化系统

自动从 163 邮箱中拉取 Excel 附件，智能提取基金净值数据并存入 SQLite 数据库，支持数据质量检测、Excel 导出，以及通过 **FastAPI 后端 API** 进行数据查询与手动编辑。同时支持通过 **tushare.pro** 接入 A 股主要指数和中金所金融期货日线行情。前端为 React 单页应用，提供基金列表、净值走势图、定时自动同步、手动补充净值和 A 股行情看板等功能。

---

## 目录结构

```
emailcontent/
├── get_163_email.py        # 主程序：连接邮箱、提取附件、写入数据库
├── smart_extractor.py      # 核心库：智能识别 Excel 格式并提取数据
├── get_market_data.py      # 行情拉取：tushare.pro 指数 + 金融期货日线
├── data_quality_check.py   # 质检脚本：异常检测 + 生成对外展示库
├── organize_fund_data.py   # 工具脚本：将数据库导出为 Excel 文件
├── api.py                  # FastAPI 后端：REST API + APScheduler 定时同步
├── requirements.txt        # Python 依赖
├── .env                    # 本地配置（已 gitignore，含邮箱密码）
├── .env.example            # 配置模板
├── web/                    # React 前端
│   ├── src/
│   │   ├── pages/
│   │   │   ├── FundList.jsx        # 基金列表页（同步按钮、问题徽章、失败推送）
│   │   │   ├── FundDetail.jsx      # 基金详情页（净值图表、手动录入）
│   │   │   └── MarketDashboard.jsx # A股行情页（指数卡片 + 期货表）
│   │   └── api.js             # API 调用封装
│   └── package.json
└── .gitignore
```

> `fund_data.db`、`fund_clean.db`、`email_attachments/` 均已 gitignore，不进入版本控制。

---

## 数据流

```
163 邮箱 (IMAP)                           tushare.pro
       │                                       │
       ▼                                       ▼
get_163_email.py                      get_market_data.py
       │   增量拉取（基于 IMAP UID）         │   指数日线 + 期货日线
       │   12:00/18:00 自动执行              │   17:00 自动执行
       │   POST /api/sync/trigger 触发       │   POST /api/market/sync/trigger 触发
       ▼                                       ▼
fund_data.db  ◄──────────────────────────────
  (funds, fund_nav_data, email_sources,
   extraction_failures, sync_state,
   index_daily, futures_daily)
       │
       ├──────────────────────────────────┐
       ▼                                  ▼
data_quality_check.py            api.py (FastAPI)
       │                                  │
       ▼                                  ▼
fund_clean.db                    REST API（端口 8000）
                                          │
                                          ▼
                                   React 前端（端口 5173）
                                   - 基金列表 + 失败推送
                                   - 净值走势图 + 手动录入
                                   - A股行情看板
```

---

## 快速开始

### 1. 安装后端依赖

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
API_HOST=0.0.0.0
API_PORT=8000
TUSHARE_TOKEN=your_tushare_token_here
```

> `EMAIL_PASSWORD` 填写的是 **IMAP 授权码**，不是登录密码。
> 获取路径：163 邮箱 → 设置 → POP3/SMTP/IMAP → 开启 IMAP → 生成授权码。
>
> `TUSHARE_TOKEN` 为可选项。注册 [tushare.pro](https://tushare.pro) 后在个人中心获取。
> 不配置时 A 股行情功能不可用，但其他功能正常运行。

### 3. 拉取邮件数据

```bash
python get_163_email.py
```

首次运行全量扫描所有邮件，此后每次只处理新邮件（增量模式）。

### 4. 拉取 A 股行情（可选）

```bash
python get_market_data.py
```

首次运行拉取最近 90 个交易日数据，此后增量更新。需要已配置 `TUSHARE_TOKEN`。

### 5. 数据质量检测

```bash
python data_quality_check.py
```

输出检测报告并生成 `fund_clean.db`。

### 6. 启动后端 API

```bash
python api.py
```

服务启动后访问：
- **API 接口**：`http://localhost:8000/api/...`
- **交互文档**：`http://localhost:8000/docs`（Swagger UI）

> 启动时会同时启动 APScheduler：
> - 邮件同步：每天 12:00 和 18:00（北京时间）自动执行
> - 行情同步：每天 17:00（北京时间）自动执行（需配置 TUSHARE_TOKEN）

### 7. 启动前端

```bash
cd web
npm install        # 首次需要安装依赖
npm run dev        # 开发服务器 http://localhost:5173
```

### 8. 导出 Excel（可选）

```bash
python organize_fund_data.py
```

生成 `fund_data_organized.xlsx`，包含汇总 Sheet 和每个产品的独立 Sheet。

---

## 前端功能说明

### 基金列表页 (`/`)

- **搜索**：按基金名称或代码模糊过滤
- **近一周收益**：前端实时计算，逐批异步加载
- **数据状态徽章**：异步加载 `GET /api/funds/issues`，对存在异常净值或日期断层的基金行显示橙色 `⚠ N` 徽章，鼠标悬停显示具体问题详情
- **提取失败推送**：Header 橙色徽章显示 `extraction_failures` 表中的失败记录数；点击弹出 Modal，显示失败时间、邮件主题、附件文件名、失败原因
- **行情**：Header 快速跳转到 A 股行情看板
- **立即同步**：点击调用 `POST /api/sync/trigger`，触发后台邮件同步，轮询 `GET /api/sync/status` 直到完成；Header 显示上次同步时间和状态

### 基金详情页 (`/fund/:id`)

- **净值走势图**：Chart.js 折线图，支持时间范围切换（近1周 / 近1月 / 近3月 / 近6月 / 近1年 / 全部）
- **单位净值 / 累计净值切换**（有累计数据时显示）
- **手动录入**：右上角「+ 手动录入」按钮，弹窗填写净值日期、单位净值（必填）、累计净值（可选），提交后自动刷新图表
- **手动录入标注**：`source_id = null` 的记录在图表上以橙色圆点标注，其余记录不显示点
- **手动录入记录列表**：图表下方列出所有手动录入记录，支持删除（带确认弹窗）
- **异常净值标注**：红色虚线垂直线标注净值 > 5 的异常日期
- **日期断层标注**：灰色半透明色带标注相邻记录间隔异常大的区间
- 图表上方显示问题摘要说明（异常数量 + 断层数量）

### A 股行情看板 (`/market`)

- **指数卡片**：9 只主要指数（上证指数、深证成指、创业板指、上证50、沪深300、中证500、中证1000、中证2000、科创50），显示最新收盘价和涨跌幅（红涨绿跌）
- **指数历史图**：点击指数卡片，下方显示近 250 个交易日收盘价折线图
- **金融期货表**：中金所主力合约（IF/IC/IH/IM/T/TF/TS），显示代码、收盘价、涨跌幅、成交量、持仓量
- **立即同步**：手动触发 tushare 行情同步

---

## 后端 API 接口文档

### 基础信息

| 项目 | 值 |
|------|-----|
| Base URL | `http://localhost:8000` |
| 数据格式 | JSON |
| 字符编码 | UTF-8 |
| 跨域 | 已开启（allow_origins=["*"]） |

---

### 系统接口

#### `GET /api/health` — 健康检查

检测服务与数据库连接是否正常。

**响应示例**

```json
{"status": "ok"}
```

**错误响应**（503）：数据库不可用时

```json
{"error": "DB_UNAVAILABLE", "detail": "Database unavailable"}
```

---

#### `GET /api/stats` — 全局统计摘要

**响应示例**

```json
{
  "total_records": 5736,
  "total_funds": 56,
  "manual_records": 12
}
```

> `manual_records`：`source_id = NULL` 的记录数，即通过 API 手动录入的条数。

---

#### `GET /api/failures` — 附件提取失败记录

返回邮件附件解析失败的记录，供前端展示未能导入的数据。

**查询参数**

| 参数 | 类型 | 默认 | 说明 |
|------|------|------|------|
| `limit` | int | 50 | 最大返回条数（1-200） |
| `offset` | int | 0 | 分页偏移量 |

**响应示例**

```json
{
  "total": 3,
  "items": [
    {
      "id": 1,
      "失败时间": "2026-01-15 12:34:56",
      "邮件主题": "【净值】某基金净值更新",
      "邮件发件人": "fund@example.com",
      "邮件日期": "2026-01-15",
      "附件文件名": "nav_20260115.xlsx",
      "sheet名称": "Sheet1",
      "失败原因": "未找到产品代码列"
    }
  ]
}
```

---

#### `GET /api/sync/status` — 查询同步状态

返回最近一次邮件同步的状态信息。

**响应示例**

```json
{
  "sync_last_time": "2026-02-21T12:00:03.142857",
  "sync_last_status": "success",
  "sync_last_added": "",
  "sync_last_error": ""
}
```

`sync_last_status` 取值：`null`（从未同步）/ `"running"` / `"success"` / `"error"`

---

#### `POST /api/sync/trigger` — 手动触发同步

立即在后台启动一次邮件同步（非阻塞，使用 FastAPI BackgroundTasks）。若同步已在运行则本次调用无操作。

**响应示例**

```json
{"message": "sync started"}
```

---

### 基金接口

#### `GET /api/funds` — 列出所有基金

返回所有基金的摘要统计信息，按 `fund_id` 升序排列。

**响应示例**

```json
{
  "total": 56,
  "items": [
    {
      "fund_id": 1,
      "product_code": "SG674B",
      "product_name": "某某基金B",
      "first_entry_time": "2025-12-02 08:34:21",
      "record_count": 115,
      "earliest_date": "2024-01-30",
      "latest_date": "2024-07-24",
      "latest_nav": 1.145,
      "anomalous_count": 0
    }
  ]
}
```

> `anomalous_count`：该基金净值 > 5 的记录数，用于前端快速判断是否存在异常。

---

#### `GET /api/funds/search` — 搜索基金

按产品名称或产品代码模糊搜索。

**查询参数**

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `q` | string | 是 | 搜索关键词（产品代码或名称，模糊匹配） |
| `limit` | int | 否 | 返回条数上限，默认 50，最大 200 |

**请求示例**

```
GET /api/funds/search?q=SG674B
GET /api/funds/search?q=东恺&limit=20
```

**响应示例**

```json
{
  "total": 1,
  "items": [
    {
      "fund_id": 1,
      "product_code": "SG674B",
      "product_name": "某某基金B",
      "first_entry_time": "2025-12-02 08:34:21"
    }
  ]
}
```

---

#### `GET /api/funds/issues` — 全部基金数据问题汇总

返回所有基金的异常净值记录和日期断层信息，前端一次性加载后本地使用。

**断层检测算法**：计算该基金所有相邻记录的日期间隔，取中位数；间隔 > `max(中位数 × 2.5, 30天)` 判定为断层。

**响应示例**

```json
{
  "issues": {
    "1": {
      "anomalous": [
        {"nav_date": "2024-03-15", "unit_nav": 12.34}
      ],
      "gaps": [
        {"from_date": "2024-01-01", "to_date": "2024-02-15", "gap_days": 45}
      ]
    },
    "2": {
      "anomalous": [],
      "gaps": []
    }
  }
}
```

---

#### `GET /api/funds/{fund_id}` — 单只基金详情

**路径参数**：`fund_id`（整数）

**响应示例**

```json
{
  "fund_id": 1,
  "product_code": "SG674B",
  "product_name": "某某基金B",
  "first_entry_time": "2025-12-02 08:34:21",
  "record_count": 115,
  "earliest_date": "2024-01-30",
  "latest_date": "2024-07-24",
  "latest_nav": 1.145,
  "anomalous_count": 0
}
```

**错误**：基金不存在返回 404。

---

#### `GET /api/funds/{fund_id}/issues` — 单只基金数据问题

返回指定基金的异常净值记录和日期断层，结构与 `/api/funds/issues` 中单条相同。

**响应示例**

```json
{
  "anomalous": [
    {"nav_date": "2024-03-15", "unit_nav": 12.34}
  ],
  "gaps": [
    {"from_date": "2024-01-01", "to_date": "2024-02-15", "gap_days": 45}
  ]
}
```

**错误**：基金不存在返回 404。

---

#### `GET /api/funds/{fund_id}/nav` — 净值时序（核心查询）

获取指定基金的净值时间序列，支持日期范围过滤与分页。

**路径参数**：`fund_id`（整数）

**查询参数**

| 参数 | 类型 | 必填 | 默认 | 说明 |
|------|------|------|------|------|
| `date_from` | string | 否 | — | 起始日期，格式 `YYYY-MM-DD` |
| `date_to` | string | 否 | — | 截止日期，格式 `YYYY-MM-DD` |
| `apply_filter` | bool | 否 | `true` | 开启数据质量过滤（排除净值 > 5 的异常记录） |
| `limit` | int | 否 | `1000` | 每页条数，最大 5000 |
| `offset` | int | 否 | `0` | 分页偏移量 |

**请求示例**

```
GET /api/funds/1/nav?date_from=2024-01-01&date_to=2024-12-31&limit=100
GET /api/funds/1/nav?apply_filter=false&offset=200&limit=50
```

**响应示例**

```json
{
  "total": 38,
  "fund_id": 1,
  "items": [
    {
      "id": 1,
      "fund_id": 1,
      "product_name": "某某基金B",
      "product_code": "SG674B",
      "nav_date": "2024-01-30",
      "unit_nav": 1.1244,
      "accumulated_nav": 1.181,
      "insert_time": "2025-12-02 08:34:21",
      "source_id": null
    }
  ]
}
```

> `source_id = null` 表示该记录为手动录入；有值时对应 `email_sources` 表的主键。

**错误**：`date_from > date_to` 返回 400；基金不存在返回 404。

---

#### `GET /api/compare` — 多基金净值对比

同时获取多只基金的全量净值序列，供图表库直接消费。

**查询参数**

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `fund_ids` | int（可重复） | 是 | 基金 ID，最多 20 个 |
| `date_from` | string | 否 | 起始日期 `YYYY-MM-DD` |
| `date_to` | string | 否 | 截止日期 `YYYY-MM-DD` |
| `apply_filter` | bool | 否 | 默认 `true` |

**请求示例**

```
GET /api/compare?fund_ids=1&fund_ids=2&fund_ids=3
GET /api/compare?fund_ids=1&fund_ids=2&date_from=2024-01-01&date_to=2024-12-31
```

**响应示例**

```json
{
  "funds": {
    "1": {
      "fund_id": 1,
      "product_code": "SG674B",
      "product_name": "某某基金B",
      "series": [
        {"date": "2024-01-30", "nav": 1.1244, "accumulated_nav": 1.181},
        {"date": "2024-01-31", "nav": 1.1161, "accumulated_nav": 1.1727}
      ]
    }
  }
}
```

**错误**：`fund_ids` 超过 20 个返回 400；任一 `fund_id` 不存在返回 404。

---

### 净值 CRUD 接口

#### `POST /api/nav` — 新增净值记录

手动补充缺失的净值数据。若产品代码不存在会自动在 `funds` 表创建新基金记录。

**请求体**

```json
{
  "product_code": "TEST001",
  "product_name": "测试基金",
  "nav_date": "2024-06-15",
  "unit_nav": 1.2345,
  "accumulated_nav": 1.3000
}
```

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `product_code` | string | 是 | 产品代码 |
| `product_name` | string | 否 | 产品名称 |
| `nav_date` | string | 是 | 净值日期，格式 `YYYY-MM-DD` |
| `unit_nav` | float | 是 | 单位净值，必须 > 0 |
| `accumulated_nav` | float | 否 | 累计单位净值 |

**响应**：201 Created，返回新建的 `NavRecord` 对象。

**错误**：
- 422：日期格式非法 / `unit_nav ≤ 0`
- 409：`(product_code, nav_date)` 已存在

---

#### `GET /api/nav/{nav_id}` — 查询单条净值记录

**路径参数**：`nav_id`（整数）

**错误**：记录不存在返回 404。

---

#### `PUT /api/nav/{nav_id}` — 修改净值记录

部分更新语义，只传需要修改的字段。

**路径参数**：`nav_id`（整数）

**请求体**（所有字段均可选）

```json
{
  "product_name": "新名称",
  "nav_date": "2024-06-20",
  "unit_nav": 1.3000,
  "accumulated_nav": 1.4000
}
```

**响应**：200 OK，返回更新后的 `NavRecord`。

**错误**：
- 404：记录不存在
- 409：修改后 `(product_code, nav_date)` 与其他记录冲突
- 422：日期格式非法 / `unit_nav ≤ 0`

---

#### `DELETE /api/nav/{nav_id}` — 删除净值记录

**路径参数**：`nav_id`（整数）

**响应**：204 No Content（不级联删除 `funds` 表记录）

**错误**：记录不存在返回 404。

---

### 行情接口

#### `GET /api/market/indices` — 指数最新行情

返回各指数最近一个交易日的数据。

**响应示例**

```json
{
  "items": [
    {
      "ts_code": "000001.SH",
      "name": "上证指数",
      "trade_date": "20260221",
      "close": 3388.52,
      "open": 3372.14,
      "high": 3392.10,
      "low": 3368.80,
      "pct_chg": 0.45,
      "vol": 234567890,
      "amount": 3456789000
    }
  ]
}
```

---

#### `GET /api/market/indices/{ts_code}/daily` — 指数历史行情

**路径参数**：`ts_code`（如 `000001.SH`）

**查询参数**

| 参数 | 类型 | 默认 | 说明 |
|------|------|------|------|
| `date_from` | string | — | 起始日期 YYYYMMDD |
| `date_to` | string | — | 截止日期 YYYYMMDD |
| `limit` | int | 250 | 最多返回条数（最大 2000） |

**响应**：按 `trade_date` 升序，不含日期区间时返回最近 N 条。

---

#### `GET /api/market/futures` — 期货最新行情

返回各期货品种主力合约最近一个交易日的数据。

**响应示例**

```json
{
  "items": [
    {
      "ts_code": "IF2504.CFX",
      "symbol": "IF",
      "trade_date": "20260221",
      "close": 3920.2,
      "open": 3901.0,
      "high": 3928.4,
      "low": 3896.6,
      "vol": 45321,
      "amount": 89456000000,
      "oi": 123456
    }
  ]
}
```

---

#### `GET /api/market/futures/{ts_code}/daily` — 期货历史行情

参数与 `/api/market/indices/{ts_code}/daily` 相同。

---

#### `GET /api/market/sync/status` — 行情同步状态

```json
{
  "market_last_status": "success",
  "market_last_error": "",
  "market_index_last_date": "20260221",
  "market_futures_last_date": "20260221"
}
```

---

#### `POST /api/market/sync/trigger` — 触发行情同步

非阻塞，在后台执行。需配置 TUSHARE_TOKEN，否则返回 400/503。

```json
{"message": "market sync started"}
```

---

### 错误码规范

所有错误响应格式统一为：

```json
{"error": "ERROR_CODE", "detail": "错误描述"}
```

| HTTP 状态码 | `error` 字段 | 触发场景 |
|------------|-------------|---------|
| 400 | `BAD_REQUEST` | `date_from > date_to`、`fund_ids` 超过 20 个、未配置 token |
| 404 | `NOT_FOUND` | 基金或净值记录不存在 |
| 409 | `DUPLICATE_RECORD` | `(产品代码, 净值日期)` 唯一约束冲突 |
| 422 | `VALIDATION_ERROR` | 日期格式错误、净值 ≤ 0、搜索词为空 |
| 503 | `DB_UNAVAILABLE` | 数据库连接失败（仅 `/api/health`）；行情模块未安装 |
| 500 | `INTERNAL_ERROR` | 未预期的服务器内部错误 |

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

### `get_market_data.py` — 行情拉取

| 函数 | 说明 |
|------|------|
| `init_market_schema(db_path)` | 创建 `index_daily`、`futures_daily` 表及索引 |
| `get_active_futures(ts_api, symbols)` | 从 `fut_basic` 查询各品种最近未到期合约 |
| `sync_indices(ts_api, conn, since_date)` | 批量调用 `index_daily` 写入各指数日线 |
| `sync_futures(ts_api, conn, since_date)` | 批量调用 `fut_daily` 写入各主力合约日线 |
| `connect_and_fetch_market(token, db_path)` | 主入口，初始化 → 查询增量起点 → 同步写库 |

**增量机制**：`market_index_last_date` / `market_futures_last_date` 存于 `sync_state`，每次从上次日期+1天开始拉取。首次运行默认拉取最近 90 天。

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

---

### `organize_fund_data.py` — Excel 导出工具

从 `fund_data.db` 读取所有数据，生成 `fund_data_organized.xlsx`：

- **汇总 Sheet**：每个产品的代码、名称、记录数、日期范围、最早/最新净值
- **各产品 Sheet**：每个产品代码单独一个 Sheet，按日期升序排列

---

## 数据库 Schema

### `fund_data.db`（原始库）

**`funds`** — 基金主表

| 字段 | 类型 | 说明 |
|------|------|------|
| fund_id | INTEGER PK | 自增主键，全局唯一基金 ID |
| 产品代码 | TEXT UNIQUE | 基金唯一标识 |
| 产品名称 | TEXT | |
| 首次录入时间 | DATETIME | |

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
| fund_id | INTEGER | 外键 → funds.fund_id |
| 产品名称 | TEXT | |
| 产品代码 | TEXT NOT NULL | |
| 净值日期 | TEXT NOT NULL | YYYYMMDD 格式 |
| 单位净值 | REAL NOT NULL | |
| 累计单位净值 | REAL | 可为空 |
| 插入时间 | DATETIME | |
| source_id | INTEGER | 外键 → email_sources.id，NULL 表示手动录入 |
| — | UNIQUE | (产品代码, 净值日期) |

> `source_id = NULL` 有两种情形：①迁移前的历史数据；②通过 API 手动录入的数据。

**`extraction_failures`** — 提取失败记录

| 字段 | 类型 | 说明 |
|------|------|------|
| id | INTEGER PK | 自增主键 |
| 失败时间 | DATETIME | |
| 邮件主题 | TEXT | |
| 邮件发件人 | TEXT | |
| 邮件日期 | TEXT | |
| 附件文件名 | TEXT | |
| sheet名称 | TEXT | |
| 失败原因 | TEXT | 错误描述 |

**`sync_state`** — 同步状态（KV 表）

| key | value |
|-----|-------|
| last_uid | 上次处理到的最大 IMAP UID |
| uidvalidity | 邮箱 UIDVALIDITY，变化则触发全量扫描 |
| sync_last_time | 最近一次邮件同步触发时间（ISO 格式） |
| sync_last_status | `running` / `success` / `error` |
| sync_last_added | 预留字段 |
| sync_last_error | 邮件同步失败时的错误信息 |
| market_index_last_date | 指数最近成功同步日期（YYYYMMDD） |
| market_futures_last_date | 期货最近成功同步日期（YYYYMMDD） |
| market_last_status | `running` / `success` / `error` |
| market_last_error | 行情同步失败时的错误信息 |

**`index_daily`** — A 股指数日线

| 字段 | 类型 | 说明 |
|------|------|------|
| id | INTEGER PK | |
| ts_code | TEXT | tushare 指数代码（如 `000300.SH`） |
| trade_date | TEXT | 交易日 YYYYMMDD |
| close/open/high/low | REAL | 收盘/开盘/最高/最低价 |
| vol | REAL | 成交量（手） |
| amount | REAL | 成交额（元） |
| pct_chg | REAL | 涨跌幅（%） |
| — | UNIQUE | (ts_code, trade_date) |

**`futures_daily`** — 金融期货日线

| 字段 | 类型 | 说明 |
|------|------|------|
| id | INTEGER PK | |
| ts_code | TEXT | 合约代码（如 `IF2504.CFX`） |
| symbol | TEXT | 品种代码（如 `IF`） |
| trade_date | TEXT | 交易日 YYYYMMDD |
| close/open/high/low | REAL | 价格 |
| vol | REAL | 成交量（手） |
| amount | REAL | 成交额 |
| oi | REAL | 持仓量（手） |
| — | UNIQUE | (ts_code, trade_date) |

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

### Python（`requirements.txt`）

```
pandas>=1.5.0        # DataFrame 处理
openpyxl>=3.0.0      # 读写 .xlsx
xlrd>=2.0.0          # 读取旧版 .xls
python-dotenv>=1.0.0 # 加载 .env 配置
fastapi>=0.100.0     # Web 框架
uvicorn[standard]>=0.23.0  # ASGI 服务器
pydantic>=2.0.0      # 数据验证
apscheduler>=3.10.0  # 定时任务调度（BackgroundScheduler）
tushare>=1.4.0       # A股行情数据（可选，不安装则行情功能不可用）
```

### Node.js（`web/package.json`）

```
react / react-dom                # UI 框架
react-router-dom                 # 客户端路由
chart.js / react-chartjs-2       # 图表库
chartjs-plugin-annotation ^3.1.0 # 图表标注插件（兼容 Chart.js v4）
```


---

## 目录结构

```
emailcontent/
├── get_163_email.py        # 主程序：连接邮箱、提取附件、写入数据库
├── smart_extractor.py      # 核心库：智能识别 Excel 格式并提取数据
├── data_quality_check.py   # 质检脚本：异常检测 + 生成对外展示库
├── organize_fund_data.py   # 工具脚本：将数据库导出为 Excel 文件
├── api.py                  # FastAPI 后端：REST API + APScheduler 定时同步
├── requirements.txt        # Python 依赖
├── .env                    # 本地配置（已 gitignore，含邮箱密码）
├── .env.example            # 配置模板
├── web/                    # React 前端
│   ├── src/
│   │   ├── pages/
│   │   │   ├── FundList.jsx    # 基金列表页（同步按钮、问题徽章）
│   │   │   └── FundDetail.jsx  # 基金详情页（净值图表、异常标注）
│   │   └── api.js             # API 调用封装
│   └── package.json
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
       │                     也可通过 POST /api/sync/trigger 触发
       │                     或由调度器每天 12:00、18:00 自动执行
       ├─ 提取 Excel 附件（BytesIO，不落磁盘）
       │
       ▼
smart_extractor.py        ← 自动识别表格格式，提取 5 个核心字段
       │
       ▼
fund_data.db              ← 原始库（4张表，见下方 Schema）
       │
       ├──────────────────────────────────┐
       ▼                                  ▼
data_quality_check.py            api.py (FastAPI)
       │                                  │
       ▼                                  ▼
fund_clean.db                    REST API（端口 8000）
                                          │
                                          ▼
                                   React 前端（端口 5173）
                                   - 基金列表 + 问题徽章
                                   - 净值走势图 + 异常标注
```

---

## 快速开始

### 1. 安装后端依赖

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
API_HOST=0.0.0.0
API_PORT=8000
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

### 5. 启动后端 API

```bash
python api.py
```

服务启动后访问：
- **API 接口**：`http://localhost:8000/api/...`
- **交互文档**：`http://localhost:8000/docs`（Swagger UI）

> 启动时会同时启动 APScheduler，每天 12:00 和 18:00（北京时间）自动同步一次邮件。

### 6. 启动前端

```bash
cd web
npm install        # 首次需要安装依赖
npm run dev        # 开发服务器 http://localhost:5173
```

### 7. 导出 Excel（可选）

```bash
python organize_fund_data.py
```

生成 `fund_data_organized.xlsx`，包含汇总 Sheet 和每个产品的独立 Sheet。

---

## 前端功能说明

### 基金列表页 (`/`)

- **搜索**：按基金名称或代码模糊过滤
- **近一周收益**：前端实时计算，逐批异步加载
- **数据状态徽章**：异步加载 `GET /api/funds/issues`，对存在异常净值或日期断层的基金行显示橙色 `⚠ N` 徽章，鼠标悬停显示具体问题详情
- **立即同步**：点击调用 `POST /api/sync/trigger`，触发后台邮件同步，轮询 `GET /api/sync/status` 直到完成；Header 显示上次同步时间和状态

### 基金详情页 (`/fund/:id`)

- **净值走势图**：Chart.js 折线图，支持时间范围切换（近1周 / 近1月 / 近3月 / 近6月 / 近1年 / 全部）
- **单位净值 / 累计净值切换**（有累计数据时显示）
- **异常净值标注**：红色虚线垂直线标注净值 > 5 的异常日期
- **日期断层标注**：灰色半透明色带标注相邻记录间隔异常大的区间
- 图表上方显示问题摘要说明（异常数量 + 断层数量）

---

## 后端 API 接口文档

### 基础信息

| 项目 | 值 |
|------|-----|
| Base URL | `http://localhost:8000` |
| 数据格式 | JSON |
| 字符编码 | UTF-8 |
| 跨域 | 已开启（allow_origins=["*"]） |

---

### 系统接口

#### `GET /api/health` — 健康检查

检测服务与数据库连接是否正常。

**响应示例**

```json
{"status": "ok"}
```

**错误响应**（503）：数据库不可用时

```json
{"error": "DB_UNAVAILABLE", "detail": "Database unavailable"}
```

---

#### `GET /api/stats` — 全局统计摘要

**响应示例**

```json
{
  "total_records": 5736,
  "total_funds": 56,
  "manual_records": 12
}
```

> `manual_records`：`source_id = NULL` 的记录数，即通过 API 手动录入的条数。

---

#### `GET /api/sync/status` — 查询同步状态

返回最近一次同步的状态信息。

**响应示例**

```json
{
  "sync_last_time": "2026-02-21T12:00:03.142857",
  "sync_last_status": "success",
  "sync_last_added": "",
  "sync_last_error": ""
}
```

`sync_last_status` 取值：`null`（从未同步）/ `"running"` / `"success"` / `"error"`

---

#### `POST /api/sync/trigger` — 手动触发同步

立即在后台启动一次邮件同步（非阻塞，使用 FastAPI BackgroundTasks）。若同步已在运行则本次调用无操作。

**响应示例**

```json
{"message": "sync started"}
```

---

### 基金接口

#### `GET /api/funds` — 列出所有基金

返回所有基金的摘要统计信息，按 `fund_id` 升序排列。

**响应示例**

```json
{
  "total": 56,
  "items": [
    {
      "fund_id": 1,
      "product_code": "SG674B",
      "product_name": "某某基金B",
      "first_entry_time": "2025-12-02 08:34:21",
      "record_count": 115,
      "earliest_date": "2024-01-30",
      "latest_date": "2024-07-24",
      "latest_nav": 1.145,
      "anomalous_count": 0
    }
  ]
}
```

> `anomalous_count`：该基金净值 > 5 的记录数，用于前端快速判断是否存在异常。

---

#### `GET /api/funds/search` — 搜索基金

按产品名称或产品代码模糊搜索。

**查询参数**

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `q` | string | 是 | 搜索关键词（产品代码或名称，模糊匹配） |
| `limit` | int | 否 | 返回条数上限，默认 50，最大 200 |

**请求示例**

```
GET /api/funds/search?q=SG674B
GET /api/funds/search?q=东恺&limit=20
```

**响应示例**

```json
{
  "total": 1,
  "items": [
    {
      "fund_id": 1,
      "product_code": "SG674B",
      "product_name": "某某基金B",
      "first_entry_time": "2025-12-02 08:34:21"
    }
  ]
}
```

---

#### `GET /api/funds/issues` — 全部基金数据问题汇总

返回所有基金的异常净值记录和日期断层信息，前端一次性加载后本地使用。

**断层检测算法**：计算该基金所有相邻记录的日期间隔，取中位数；间隔 > `max(中位数 × 2.5, 30天)` 判定为断层。

**响应示例**

```json
{
  "issues": {
    "1": {
      "anomalous": [
        {"nav_date": "2024-03-15", "unit_nav": 12.34}
      ],
      "gaps": [
        {"from_date": "2024-01-01", "to_date": "2024-02-15", "gap_days": 45}
      ]
    },
    "2": {
      "anomalous": [],
      "gaps": []
    }
  }
}
```

---

#### `GET /api/funds/{fund_id}` — 单只基金详情

**路径参数**：`fund_id`（整数）

**响应示例**

```json
{
  "fund_id": 1,
  "product_code": "SG674B",
  "product_name": "某某基金B",
  "first_entry_time": "2025-12-02 08:34:21",
  "record_count": 115,
  "earliest_date": "2024-01-30",
  "latest_date": "2024-07-24",
  "latest_nav": 1.145,
  "anomalous_count": 0
}
```

**错误**：基金不存在返回 404。

---

#### `GET /api/funds/{fund_id}/issues` — 单只基金数据问题

返回指定基金的异常净值记录和日期断层，结构与 `/api/funds/issues` 中单条相同。

**响应示例**

```json
{
  "anomalous": [
    {"nav_date": "2024-03-15", "unit_nav": 12.34}
  ],
  "gaps": [
    {"from_date": "2024-01-01", "to_date": "2024-02-15", "gap_days": 45}
  ]
}
```

**错误**：基金不存在返回 404。

---

#### `GET /api/funds/{fund_id}/nav` — 净值时序（核心查询）

获取指定基金的净值时间序列，支持日期范围过滤与分页。

**路径参数**：`fund_id`（整数）

**查询参数**

| 参数 | 类型 | 必填 | 默认 | 说明 |
|------|------|------|------|------|
| `date_from` | string | 否 | — | 起始日期，格式 `YYYY-MM-DD` |
| `date_to` | string | 否 | — | 截止日期，格式 `YYYY-MM-DD` |
| `apply_filter` | bool | 否 | `true` | 开启数据质量过滤（排除净值 > 5 的异常记录） |
| `limit` | int | 否 | `1000` | 每页条数，最大 5000 |
| `offset` | int | 否 | `0` | 分页偏移量 |

**请求示例**

```
GET /api/funds/1/nav?date_from=2024-01-01&date_to=2024-12-31&limit=100
GET /api/funds/1/nav?apply_filter=false&offset=200&limit=50
```

**响应示例**

```json
{
  "total": 38,
  "fund_id": 1,
  "items": [
    {
      "id": 1,
      "fund_id": 1,
      "product_name": "某某基金B",
      "product_code": "SG674B",
      "nav_date": "2024-01-30",
      "unit_nav": 1.1244,
      "accumulated_nav": 1.181,
      "insert_time": "2025-12-02 08:34:21",
      "source_id": null
    }
  ]
}
```

> `source_id = null` 表示该记录为手动录入；有值时对应 `email_sources` 表的主键。

**错误**：`date_from > date_to` 返回 400；基金不存在返回 404。

---

#### `GET /api/compare` — 多基金净值对比

同时获取多只基金的全量净值序列，供图表库直接消费。

**查询参数**

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `fund_ids` | int（可重复） | 是 | 基金 ID，最多 20 个 |
| `date_from` | string | 否 | 起始日期 `YYYY-MM-DD` |
| `date_to` | string | 否 | 截止日期 `YYYY-MM-DD` |
| `apply_filter` | bool | 否 | 默认 `true` |

**请求示例**

```
GET /api/compare?fund_ids=1&fund_ids=2&fund_ids=3
GET /api/compare?fund_ids=1&fund_ids=2&date_from=2024-01-01&date_to=2024-12-31
```

**响应示例**

```json
{
  "funds": {
    "1": {
      "fund_id": 1,
      "product_code": "SG674B",
      "product_name": "某某基金B",
      "series": [
        {"date": "2024-01-30", "nav": 1.1244, "accumulated_nav": 1.181},
        {"date": "2024-01-31", "nav": 1.1161, "accumulated_nav": 1.1727}
      ]
    }
  }
}
```

**错误**：`fund_ids` 超过 20 个返回 400；任一 `fund_id` 不存在返回 404。

---

### 净值 CRUD 接口

#### `POST /api/nav` — 新增净值记录

手动补充缺失的净值数据。若产品代码不存在会自动在 `funds` 表创建新基金记录。

**请求体**

```json
{
  "product_code": "TEST001",
  "product_name": "测试基金",
  "nav_date": "2024-06-15",
  "unit_nav": 1.2345,
  "accumulated_nav": 1.3000
}
```

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `product_code` | string | 是 | 产品代码 |
| `product_name` | string | 否 | 产品名称 |
| `nav_date` | string | 是 | 净值日期，格式 `YYYY-MM-DD` |
| `unit_nav` | float | 是 | 单位净值，必须 > 0 |
| `accumulated_nav` | float | 否 | 累计单位净值 |

**响应**：201 Created，返回新建的 `NavRecord` 对象。

**错误**：
- 422：日期格式非法 / `unit_nav ≤ 0`
- 409：`(product_code, nav_date)` 已存在

---

#### `GET /api/nav/{nav_id}` — 查询单条净值记录

**路径参数**：`nav_id`（整数）

**错误**：记录不存在返回 404。

---

#### `PUT /api/nav/{nav_id}` — 修改净值记录

部分更新语义，只传需要修改的字段。

**路径参数**：`nav_id`（整数）

**请求体**（所有字段均可选）

```json
{
  "product_name": "新名称",
  "nav_date": "2024-06-20",
  "unit_nav": 1.3000,
  "accumulated_nav": 1.4000
}
```

**响应**：200 OK，返回更新后的 `NavRecord`。

**错误**：
- 404：记录不存在
- 409：修改后 `(product_code, nav_date)` 与其他记录冲突
- 422：日期格式非法 / `unit_nav ≤ 0`

---

#### `DELETE /api/nav/{nav_id}` — 删除净值记录

**路径参数**：`nav_id`（整数）

**响应**：204 No Content（不级联删除 `funds` 表记录）

**错误**：记录不存在返回 404。

---

### 错误码规范

所有错误响应格式统一为：

```json
{"error": "ERROR_CODE", "detail": "错误描述"}
```

| HTTP 状态码 | `error` 字段 | 触发场景 |
|------------|-------------|---------|
| 400 | `BAD_REQUEST` | `date_from > date_to`、`fund_ids` 超过 20 个 |
| 404 | `NOT_FOUND` | 基金或净值记录不存在 |
| 409 | `DUPLICATE_RECORD` | `(产品代码, 净值日期)` 唯一约束冲突 |
| 422 | `VALIDATION_ERROR` | 日期格式错误、净值 ≤ 0、搜索词为空 |
| 503 | `DB_UNAVAILABLE` | 数据库连接失败（仅 `/api/health`） |
| 500 | `INTERNAL_ERROR` | 未预期的服务器内部错误 |

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

---

### `organize_fund_data.py` — Excel 导出工具

从 `fund_data.db` 读取所有数据，生成 `fund_data_organized.xlsx`：

- **汇总 Sheet**：每个产品的代码、名称、记录数、日期范围、最早/最新净值
- **各产品 Sheet**：每个产品代码单独一个 Sheet，按日期升序排列

---

## 数据库 Schema

### `fund_data.db`（原始库）

**`funds`** — 基金主表

| 字段 | 类型 | 说明 |
|------|------|------|
| fund_id | INTEGER PK | 自增主键，全局唯一基金 ID |
| 产品代码 | TEXT UNIQUE | 基金唯一标识 |
| 产品名称 | TEXT | |
| 首次录入时间 | DATETIME | |

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
| fund_id | INTEGER | 外键 → funds.fund_id |
| 产品名称 | TEXT | |
| 产品代码 | TEXT NOT NULL | |
| 净值日期 | TEXT NOT NULL | YYYYMMDD 格式 |
| 单位净值 | REAL NOT NULL | |
| 累计单位净值 | REAL | 可为空 |
| 插入时间 | DATETIME | |
| source_id | INTEGER | 外键 → email_sources.id，NULL 表示手动录入 |
| — | UNIQUE | (产品代码, 净值日期) |

> `source_id = NULL` 有两种情形：①迁移前的历史数据；②通过 API 手动录入的数据。

**`sync_state`** — 同步状态

| key | value |
|-----|-------|
| last_uid | 上次处理到的最大 IMAP UID |
| uidvalidity | 邮箱 UIDVALIDITY，变化则触发全量扫描 |
| sync_last_time | 最近一次同步触发时间（ISO 格式） |
| sync_last_status | `running` / `success` / `error` |
| sync_last_added | 预留字段 |
| sync_last_error | 失败时的错误信息 |

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

### Python（`requirements.txt`）

```
pandas>=1.5.0        # DataFrame 处理
openpyxl>=3.0.0      # 读写 .xlsx
xlrd>=2.0.0          # 读取旧版 .xls
python-dotenv>=1.0.0 # 加载 .env 配置
fastapi>=0.100.0     # Web 框架
uvicorn[standard]>=0.23.0  # ASGI 服务器
pydantic>=2.0.0      # 数据验证
apscheduler>=3.10.0  # 定时任务调度（BackgroundScheduler）
```

### Node.js（`web/package.json`）

```
react / react-dom                # UI 框架
react-router-dom                 # 客户端路由
chart.js / react-chartjs-2       # 图表库
chartjs-plugin-annotation ^3.1.0 # 图表标注插件（兼容 Chart.js v4）
```
