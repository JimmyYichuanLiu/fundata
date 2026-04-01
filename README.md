# 基金净值数据采集与可视化系统

自动从 163 邮箱中拉取 Excel 附件，智能提取基金净值数据并存入 SQLite 数据库，支持数据质量检测、Excel 导出，以及通过 **FastAPI 后端 API** 进行数据查询与手动编辑。同时支持通过 **akshare**（免费，无需 Token）接入 A 股主要指数、中金所金融期货、全球原油价格日线行情及股指期货基差分析。此外内置 **中东冲突与原油新闻** RSS 聚合功能，每 2 小时自动抓取 USNI News / OilPrice.com / Al Jazeera 三个来源，并通过 **yfinance 交叉验证** WTI/Brent 价格数据可靠性。前端为 React 单页应用，支持移动端（≥375px）。

---

## 目录结构

```
emailcontent/
├── get_163_email.py        # 主程序：连接邮箱、提取附件、写入数据库
├── smart_extractor.py      # 核心库：智能识别 Excel 格式并提取数据
├── get_market_data.py      # 行情拉取：akshare 指数 + 金融期货日线/5分钟K线
├── get_crude_data.py       # 原油数据拉取：WTI/Brent/SC 日频 + yfinance 交叉验证
├── crude_api.py            # 原油 API 路由（FastAPI APIRouter，挂载到 api.py）
├── get_news_data.py        # 新闻 RSS 抓取：USNI News / OilPrice.com / Al Jazeera
├── news_api.py             # 新闻 API 路由（FastAPI APIRouter，挂载到 api.py）
├── data_quality_check.py   # 质检脚本：异常检测 + 生成对外展示库
├── organize_fund_data.py   # 工具脚本：将数据库导出为 Excel 文件
├── api.py                  # FastAPI 后端：REST API + APScheduler 定时同步
├── requirements.txt        # Python 依赖
├── .env                    # 本地配置（已 gitignore，含邮箱密码）
├── .env.example            # 配置模板
├── web/                    # React 前端
│   ├── src/
│   │   ├── components/
│   │   │   ├── Layout.jsx          # 全局导航布局（含移动端悬浮顶栏 h-14）
│   │   │   └── RangeScrubber.jsx   # 双柄日期范围滑动条（图表页复用）
│   │   ├── context/
│   │   │   └── CompareContext.jsx  # 全局对比列表状态（React Context）
│   │   ├── pages/
│   │   │   ├── FundList.jsx        # 基金列表页（同步按钮、问题徽章、失败推送、对比勾选）
│   │   │   ├── FundDetail.jsx      # 基金详情页（Scrollspy 导航、净值图表、手动录入）
│   │   │   ├── MarketDashboard.jsx # A股行情页（指数卡片 + 期货表）
│   │   │   ├── ComparisonPage.jsx  # 多基金对比页（Scrollspy 四段式布局）
│   │   │   ├── FundComparison.jsx  # 旧对比页（保留）
│   │   │   ├── BasisAnalysis.jsx   # 股指基差分析页（年化基差走势）
│   │   │   ├── CrudeOilComparison.jsx # 原油价格对比 + 中东冲突新闻列表
│   │   │   └── fund-comparison/    # 对比页子组件
│   │   │       ├── ComparisonHero.jsx       # 基金卡片（名称 + 最新净值 + 日期）
│   │   │       ├── ComparisonChart.jsx      # 归一化走势图 + 动态回撤图
│   │   │       ├── ComparisonMetrics.jsx    # 业绩指标表（支持基金互为基准 / 超额指标）
│   │   │       ├── ComparisonCorrelation.jsx # 相关性热力矩阵（含基准指数）
│   │   │       └── ComparisonRadar.jsx      # 风险收益六维雷达图
│   │   ├── api/
│   │   │   └── crudeApi.js         # 原油 + 新闻 API 封装
│   │   ├── utils/
│   │   │   ├── metrics.js          # 绩效指标计算（夏普/最大回撤等）
│   │   │   └── metricDefs.js       # 指标分组定义、格式化、颜色函数、基准选项
│   │   └── api.js                  # 通用 API 调用封装
│   └── package.json
└── .gitignore
```

> `fund_data.db`、`fund_clean.db`、`email_attachments/` 均已 gitignore，不进入版本控制。

---

## 数据流

```
163 邮箱 (IMAP)                         akshare (免费，无需 Token)
       │                                       │
       ▼                                       ▼
get_163_email.py                  get_market_data.py  get_crude_data.py
       │   增量拉取（基于 IMAP UID）   指数+期货日线/5min  WTI/Brent/SC 日线
       │   12:00/18:00 自动执行       11:30/15:15 自动执行  15:20 自动同步
       │   POST /api/sync/trigger    POST /api/market/sync/trigger  + yfinance 交叉验证
       │                             POST /api/crude/sync/trigger
       │
       │                         RSS 新闻源（免费，无需 Token）
       │                         USNI News / OilPrice.com / Al Jazeera
       │                                       │
       │                               get_news_data.py
       │                               每 2 小时自动抓取
       │                               POST /api/news/sync/trigger
       │                                       │
       ▼                                       ▼
fund_data.db  ◄──────────────────────────────
  (funds, fund_nav_data, email_sources,
   extraction_failures, sync_state,
   index_daily, futures_daily,
   index_5min, futures_5min,
   crude_daily, crude_price_cross,
   crude_news)
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
                                   / 基金列表 + 失败推送
                                   /fund/:id 净值走势图 + 手动录入
                                   /market A股行情看板
                                   /compare/v2 多基金对比（新版）
                                   /basis 股指基差分析
                                   /crude 原油价格对比 + 中东冲突新闻
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
MARKET_INTRADAY_MODE=0
```

> `EMAIL_PASSWORD` 填写的是 **IMAP 授权码**，不是登录密码。
> 获取路径：163 邮箱 → 设置 → POP3/SMTP/IMAP → 开启 IMAP → 生成授权码。
>
> **行情数据无需任何 Token**，akshare 免费直连，数据源：腾讯QQ财经（指数）、CFFEX官网（期货）、新浪财经（5分钟K线 + 原油）。

### 3. 拉取邮件数据

```bash
python get_163_email.py
```

首次运行全量扫描所有邮件，此后每次只处理新邮件（增量模式）。

### 4. 拉取 A 股行情（可选）

```bash
python get_market_data.py
```

首次运行拉取约 8 年历史数据，此后增量更新。无需任何 Token。

> **代理注意**：若使用 Clash 等代理并开启了全局模式，国内数据源可能无法访问。
> 请切换为规则模式，或为以下域名添加直连规则：
> `proxy.finance.qq.com`, `www.cffex.com.cn`, `finance.sina.com.cn`

### 5. 拉取原油行情（可选）

```bash
python get_crude_data.py
```

拉取 WTI（NYMEX）、Brent（ICE）、上海 SC（INE）三品种日频收盘价，增量写入 `crude_daily` 表。同时自动执行 **yfinance 交叉验证**（WTI/Brent 与 Yahoo Finance 对比，差异 > 3% 标记为未验证），结果写入 `crude_price_cross` 表。

### 6. 抓取中东冲突与原油新闻（可选）

```bash
python get_news_data.py
```

从 USNI News、OilPrice.com、Al Jazeera 三个 RSS 源抓取新闻，写入 `crude_news` 表。服务启动后每 2 小时自动执行，也可在前端原油页面点击「抓取新闻」手动触发。

### 6. 数据质量检测

```bash
python data_quality_check.py
```

### 7. 启动后端 API

```bash
python api.py
```

服务启动后访问：
- **API 接口**：`http://localhost:8000/api/...`
- **交互文档**：`http://localhost:8000/docs`（Swagger UI）

> 启动时会同时启动 APScheduler：
> - 邮件同步：每天 12:00 和 18:00（北京时间）自动执行
> - 行情同步：每天 11:30 和 15:15（北京时间）自动执行
> - 原油同步：每天 15:20（北京时间）自动执行
> - 新闻抓取：每 2 小时自动执行（USNI / OilPrice / Al Jazeera）

### 8. 启动前端

```bash
cd web
npm install        # 首次需要安装依赖
npm run dev        # 开发服务器 http://localhost:5173
```

### 9. 导出 Excel（可选）

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
- **对比勾选**：每行第一列新增「对比」Checkbox；选中 1 只时右下角弹出浮动面板，选中 ≥ 2 只时「开始对比」按钮激活，点击跳转到 `/compare/v2`；面板支持单独移除、清空全部，最多同时选 8 只
- **立即同步**：点击调用 `POST /api/sync/trigger`，触发后台邮件同步，轮询 `GET /api/sync/status` 直到完成；Header 显示上次同步时间和状态

### 基金详情页 (`/fund/:id`)

- **Scrollspy 导航**：固定在页面顶部的导航条同时渲染「业绩走势 / 业绩指标 / 产品表现」三个区段；随用户滚动自动高亮当前区段，点击导航项平滑滚动定位
- **净值走势图**：Chart.js 折线图，收益率模式下曲线统一归一到 1（Y轴和 Tooltip 显示百分比涨跌），支持时间范围切换（近1周 / 近1月 / 近3月 / 近6月 / 近1年 / 全部）
- **单位净值 / 累计净值切换**（有累计数据时显示，同样归一到 1）
- **手动录入**：右上角「+ 手动录入」按钮，弹窗填写净值日期、单位净值（必填）、累计净值（可选），提交后自动刷新图表
- **手动录入标注**：`source_id = null` 的记录在图表上以橙色圆点标注，其余记录不显示点
- **手动录入记录列表**：图表下方列出所有手动录入记录，支持删除（带确认弹窗）
- **异常净值标注**：红色虚线垂直线标注净值 > 5 的异常日期
- **日期断层标注**：灰色半透明色带标注相邻记录间隔异常大的区间

### A 股行情看板 (`/market`)

- **指数卡片**：9 只主要指数（上证指数、深证成指、创业板指、上证50、沪深300、中证500、中证1000、中证2000、科创50），显示最新收盘价和涨跌幅（红涨绿跌）
- **指数历史图**：点击指数卡片，下方显示近 250 个交易日收盘价折线图
- **金融期货表**：中金所主力合约（IF/IC/IH/IM/T/TF/TS），显示代码、收盘价、涨跌幅、成交量、持仓量
- **基差分析跳转**：快速跳转到股指基差分析页
- **立即同步**：手动触发 akshare 行情同步

### 多基金对比页 (`/compare/v2`)

由 `CompareContext`（全局 React Context）管理对比列表，最多同时对比 8 只基金。页面采用 Scrollspy 四段式布局，固定导航条随滚动自动高亮。

- **基金卡片（Hero）**：每只基金显示名称、最新单位净值（4位小数）、净值日期，带彩色左边框区分；点击 × 单独移除
- **业绩走势**：多基金叠加折线图，净值统一归一到 1（起点 = 1），Y 轴和 Tooltip 显示百分比涨跌；默认显示**共同区间**（以所有基金中最晚的起始日期为起点，确保全部基金均有数据）；支持近1月 / 近3月 / 近6月 / 近1年 / 近3年 / 全部切换；同屏叠加**动态回撤对比**子图；右上角选择对标指数（中证2000 / 沪深300 / 中证500 等）
- **业绩指标**：近1年 / 近3年 / 成立以来三档统计区间；按「收益类 / 风险类 / 比率类」三组展示，每组自动高亮最优列；支持**超额指标模式**（勾选后显示超额收益、超额夏普等）；每列表头带「基准」Checkbox，勾选某只基金后该列显示绝对指标（标注「基准」），其余列自动切换为相对该基金的几何超额指标
- **相关性**：基于日度收益率的 Pearson 相关系数矩阵，含对标指数行/列；颜色编码：深红（≥ 0.9）→ 浅红（≥ 0.7）→ 白（接近 0）→ 浅绿（负相关）
- **风险收益画像**：六维雷达图（年化收益 / 夏普比率 / 抗回撤 / 低波动 / 月胜率 / 卡玛比率），所有基金同区间数据叠加展示，各维度已归一化到 0–100

### 多基金对比页（旧版，`/compare`）

- **基金选择**：搜索框模糊搜索，支持同时选择最多 10 只基金，已选基金以彩色 Chip 显示
- **时间范围**：近3月 / 近6月 / 近1年 / 近3年 / 全部 / 自定义日期区间
- **归一化切换**：「归一到100」模式将各基金净值归一到起始值100，方便横向比较；「绝对净值」模式显示原始净值
- **绩效指标表**：区间收益、年化收益、年化波动率、最大回撤、夏普比率（无风险利率 2.5%）、月胜率

### 股指基差分析 (`/basis`)

- **品种切换**：IF（沪深300）/ IC（中证500）/ IH（上证50）/ IM（中证1000）
- **今日合约快照**：当季 / 下季 / 隔季合约的到期日、剩余天数、现货价、期货价、基差、年化基差%、贴水/升水方向
- **年化基差走势图**：当季和下季年化基差率历史折线图（近1月 / 近3月 / 近6月 / 近1年 / 全部 / 自定义）
- **近30交易日数据表**：当季/下季并排显示

> 基差 = 现货 − 期货；年化基差% = 基差 / 期货价 / 剩余天数 × 365 × 100

### 原油价格对比 (`/crude`)

- **三品种对比图**：WTI（美元/桶，左Y轴）、Brent（美元/桶，左Y轴）、上海SC（人民币/桶，右Y轴）双Y轴折线图
- **最新价格卡片**：三品种最新收盘价与日期
- **最近30日数据表**：三品种并排
- **同步状态与手动同步**：显示同步时间、状态徽章，支持手动触发
- **中东冲突与原油新闻**：页面底部展示 RSS 聚合新闻列表，支持按「冲突动态 / 原油市场」分类筛选；每条显示标题（外链）、来源、发布时间；「抓取新闻」按钮手动触发同步

### 移动端适配

前端全面支持手机浏览器（≥375px，如 iPhone SE）：

- **移动端顶栏**：手机上侧边栏隐藏，顶部固定 56px 导航栏（含汉堡菜单和应用名），点击菜单图标展开侧边栏
- **粘性页头偏移**：各页面固定页头在手机上使用 `top-14`（顶栏下方）、桌面端使用 `top-0`，互不遮挡
- **指数卡片网格**：手机端 2 列 → 平板 3 列 → 桌面 5 列
- **数据表格**：保留横向滚动（`overflow-x-auto`），单元格内边距手机端缩减，桌面端恢复正常
- **内容区间距**：手机端 `p-4`，桌面端 `p-8`
- **按钮行自动换行**：所有控制栏按钮组均设置 `flex-wrap`，防止溢出
- **滑动条把手**：手机端稍大（16px），便于触摸操作

---

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

**响应示例**

```json
{"status": "ok"}
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

---

#### `GET /api/failures` — 附件提取失败记录

**查询参数**：`limit`（默认50，最大200）、`offset`

---

#### `GET /api/sync/status` — 邮件同步状态

```json
{
  "sync_last_time": "2026-02-21T12:00:03",
  "sync_last_status": "success",
  "sync_last_added": "",
  "sync_last_error": ""
}
```

#### `POST /api/sync/trigger` — 触发邮件同步

---

### 基金接口

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/funds` | 所有基金摘要统计 |
| GET | `/api/funds/search?q=` | 按名称/代码模糊搜索 |
| GET | `/api/funds/issues` | 所有基金数据问题汇总 |
| GET | `/api/funds/{fund_id}` | 单只基金详情 |
| GET | `/api/funds/{fund_id}/nav` | 净值时序（支持 `date_from`/`date_to`/`apply_filter`/`limit`/`offset`） |
| GET | `/api/funds/{fund_id}/issues` | 单只基金数据问题 |
| GET | `/api/compare?fund_ids=1&fund_ids=2` | 多基金净值对比（最多20只） |

---

### 净值 CRUD 接口

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/nav` | 新增手动净值记录（`source_id=NULL`） |
| GET | `/api/nav/{id}` | 查询单条净值 |
| PUT | `/api/nav/{id}` | 修改净值 |
| DELETE | `/api/nav/{id}` | 删除净值 |

**POST `/api/nav` 请求体**

```json
{
  "product_code": "TEST001",
  "product_name": "测试基金",
  "nav_date": "2024-06-15",
  "unit_nav": 1.2345,
  "accumulated_nav": 1.3000
}
```

---

### 行情接口

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/market/indices` | 各指数最新行情 |
| GET | `/api/market/indices/{ts_code}/daily` | 指数历史（`date_from`/`date_to`/`limit`，YYYYMMDD） |
| GET | `/api/market/futures` | 各期货主力合约最新行情 |
| GET | `/api/market/futures/{ts_code}/daily` | 期货历史行情 |
| GET | `/api/market/basis/today?symbol=IF` | 当日股指期货基差快照（当季/下季/隔季） |
| GET | `/api/market/basis/quarterly?symbol=IF` | 历史年化基差（支持日期范围） |
| GET | `/api/market/sync/status` | 行情同步状态 |
| POST | `/api/market/sync/trigger` | 触发行情同步 |

---

### 原油接口

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/crude/daily` | WTI/BRENT/SC 三品种联合对比数据 |
| GET | `/api/crude/{ts_code}/daily` | 单品种历史数据（`ts_code`: WTI/BRENT/SC，不区分大小写） |
| GET | `/api/crude/cross` | WTI/BRENT 价格交叉验证结果（akshare vs yfinance） |
| GET | `/api/crude/sync/status` | 原油同步状态 |
| POST | `/api/crude/sync/trigger` | 触发原油数据同步 |

### 新闻接口

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/news` | 新闻列表（支持 `category=conflict\|crude`、`limit`、`offset`） |
| GET | `/api/news/sync/status` | 新闻同步状态 |
| POST | `/api/news/sync/trigger` | 手动触发新闻 RSS 抓取 |

**`GET /api/crude/daily` 响应示例**

```json
{
  "items": [
    {"trade_date": "20250301", "WTI": 72.5, "BRENT": 75.1, "SC": 530.2}
  ],
  "latest_date": "20250308",
  "symbols": {"WTI": "USD", "BRENT": "USD", "SC": "CNY"}
}
```

> SC 单位为 CNY/桶，WTI/BRENT 单位为 USD/桶，前端使用双Y轴处理。

---

### 错误码规范

所有错误响应格式：`{"error": "ERROR_CODE", "detail": "错误描述"}`

| HTTP | `error` | 触发场景 |
|------|---------|---------|
| 400 | `BAD_REQUEST` | 日期顺序错误、超过限制数量 |
| 404 | `NOT_FOUND` | 基金/净值/品种不存在 |
| 409 | `DUPLICATE_RECORD` | 唯一约束冲突 |
| 422 | `VALIDATION_ERROR` | 格式错误、净值 ≤ 0 |
| 503 | `DB_UNAVAILABLE` | 数据库连接失败 |
| 500 | `INTERNAL_ERROR` | 未预期服务器错误 |

---

## 各文件详解

### `get_163_email.py` — 主程序

**增量机制**：每次成功处理后保存 `last_uid` 和 `uidvalidity` 到 `sync_state` 表。下次运行只拉取 UID 更大的新邮件。若检测到邮箱被重建（`uidvalidity` 变化），自动降级为全量扫描。

### `get_market_data.py` — A股行情拉取

覆盖 A 股指数日线（腾讯QQ财经 JSONP API）和中金所金融期货日线（akshare CFFEX官网）。`get_active_futures_ak()` 调用 `ak.match_main_contract(symbol="cffex")` 获取各品种主力合约。支持 5 分钟 K 线（`MARKET_INTRADAY_MODE=1`）。

**增量机制**：`market_index_last_date` / `market_futures_last_date` 存于 `sync_state`，每次从上次日期+1天开始拉取。

### `get_crude_data.py` — 原油数据拉取

| 品种 | 数据源 | 货币 |
|------|--------|------|
| WTI | `ak.futures_foreign_hist("CL")`（新浪财经国际期货） | USD |
| BRENT | `ak.futures_foreign_hist("OIL")` | USD |
| SC | `ak.futures_zh_daily_sina("SC0")`（主力合约） | CNY |

每个品种独立同步，单品种失败不影响其他。状态记录于 `sync_state`（`crude_last_*` 键）。

同步完成后自动执行 **yfinance 交叉验证**：拉取 Yahoo Finance 的 WTI（`CL=F`）和 Brent（`BZ=F`）近 90 天数据，与 akshare 数据逐日对比，差异超过 3% 则标记 `is_verified=0`，结果写入 `crude_price_cross` 表。yfinance 未安装时自动跳过，不影响主数据入库。

### `get_news_data.py` — 新闻 RSS 抓取

| 来源 | URL | 策略 |
|------|-----|------|
| USNI News | `news.usni.org/feed` | 全量收录（美国海军/霍尔木兹） |
| OilPrice.com | `oilprice.com/rss/main` | 全量收录（原油专属媒体） |
| Al Jazeera | `aljazeera.com/xml/rss/all.xml` | 关键词过滤（Iran/Israel/Hormuz/crude/Houthi 等） |

通过 `feedparser` 解析 RSS，新条目按 `url UNIQUE` 去重写入 `crude_news` 表。支持 `conflict` / `crude` 两种分类标签。

### `crude_api.py` — 原油 API 路由

`APIRouter(prefix="/api/crude")`，在 `api.py` 中通过 `app.include_router()` 挂载。import 失败（akshare 未安装）时 API 正常启动，原油端点不可用。

### `smart_extractor.py` — 智能提取引擎

对外只需调用 `extract_and_normalize(df)` 一个函数，支持 4+ 种 Excel 布局（标准表格、多行标题、键值对纵向、表头含换行符）。

---

## 数据库 Schema

### `fund_data.db`（原始库）

**`funds`** — 基金主表：`fund_id (PK)`, `产品代码 (UNIQUE)`, `产品名称`, `首次录入时间`

**`fund_nav_data`** — 基金净值：`id (PK)`, `fund_id (FK)`, `产品代码`, `净值日期 (YYYYMMDD)`, `单位净值`, `累计单位净值`, `source_id (NULL = 手动录入)`, UNIQUE(`产品代码`, `净值日期`)

**`extraction_failures`** — 提取失败记录：时间、邮件主题、发件人、附件文件名、sheet名称、失败原因

**`sync_state`** — KV 同步状态表

| key | purpose |
|-----|---------|
| `last_uid` | IMAP 增量同步位点 |
| `uidvalidity` | 邮箱 UIDVALIDITY，变化触发全量扫描 |
| `sync_last_time/status/error` | 邮件同步状态 |
| `market_index_last_date` | 指数最近成功同步日期（YYYYMMDD） |
| `market_futures_last_date` | 期货最近成功同步日期（YYYYMMDD） |
| `market_last_status/error` | 行情同步状态 |
| `crude_last_status/time/error/added` | 原油同步状态 |
| `news_last_status/time/error/added` | 新闻 RSS 同步状态 |

**`index_daily`** — A 股指数日线：`ts_code`, `trade_date`, OHLCV, `pct_chg`, UNIQUE(`ts_code`, `trade_date`)

**`futures_daily`** — 金融期货日线：`ts_code`, `symbol`, `trade_date`, OHLCV, `oi`, UNIQUE(`ts_code`, `trade_date`)

**`crude_daily`** — 原油日线：`ts_code`（WTI/BRENT/SC）, `trade_date`, OHLCV, `currency`, UNIQUE(`ts_code`, `trade_date`)

**`crude_price_cross`** — 原油价格交叉验证：`ts_code`, `trade_date`, `close_primary`（akshare）, `close_alt`（yfinance）, `diff_pct`, `is_verified`（差异<3%为1）, `verified_at`, UNIQUE(`ts_code`, `trade_date`)

**`crude_news`** — 原油/中东冲突新闻：`id`, `title`, `url (UNIQUE)`, `source_name`, `published_at`, `summary`, `category`（conflict/crude）, `fetched_at`

---

### `fund_clean.db`（校准库）

由 `data_quality_check.py` 重建。过滤净值 > 5，去重，并内联来源字段（邮件主题、发件人、附件文件名等）。

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
apscheduler>=3.10.0  # 定时任务调度
akshare>=1.10.0      # A股/期货/原油行情（免费，无需 Token）
pytz>=2023.3         # 时区处理
curl_cffi>=0.7.0     # akshare 部分接口依赖
feedparser>=6.0.0    # RSS 解析（新闻抓取）
yfinance>=0.2.50     # Yahoo Finance 数据（WTI/Brent 交叉验证）
```

### Node.js（`web/package.json`）

```
react / react-dom                # UI 框架
react-router-dom                 # 客户端路由
chart.js / react-chartjs-2       # 图表库
chartjs-plugin-annotation ^3.1.0 # 图表标注插件（兼容 Chart.js v4）
```
