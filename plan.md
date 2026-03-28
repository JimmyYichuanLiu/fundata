# 原油观察页功能扩展计划

> 生成日期：2026-03-26
> 状态：待讨论确认

---

## 任务一：时间对齐问题说明（无需改代码，仅澄清）

### 现状

| 品种 | 数据源 | 时间粒度 | 实际时区 |
|------|--------|---------|---------|
| WTI  | akshare `futures_foreign_hist("CL")` → 新浪财经 | 日频 YYYYMMDD | NYMEX 交易日（纽约 UTC-5/-4），但代码只存日期字符串，**不含时区** |
| BRENT | akshare `futures_foreign_hist("OIL")` → 新浪财经 | 日频 YYYYMMDD | ICE 交易日（伦敦 UTC+0），同上，**不含时区** |
| SC | akshare `futures_zh_daily_sina("SC0")` → 新浪财经 | 日频 YYYYMMDD | INE 交易日（上海 UTC+8），**不含时区** |

### 问题

三个品种**不是同一时间的价格**：

- WTI 的"2026-03-25"收盘价 = 纽约时间 3月25日下午2:30 NYMEX 结算价
- BRENT 的"2026-03-25"收盘价 = 伦敦时间 3月25日 ICE 结算价（比纽约早5小时）
- SC 的"2026-03-25"收盘价 = 上海时间 3月25日 INE 结算价（比纽约早13小时）

实际上，SC 的"3月25日"对应的是纽约时间 3月24日晚上的市场环境。

### 建议处理方式（供讨论）

**方案A（当前方案，维持不变）**：三者按日期字符串对齐，在前端图表上加一个注释说明"各品种以各自交易所交易日为准"。成本最低，对于趋势观察已足够。

**方案B（精确对齐）**：将 SC 的日期向后偏移1天再对比（因为上海收盘时纽约还没开盘）。但这会引入新的歧义，不推荐。

**结论**：维持方案A，在前端图表 tooltip 或说明文字中加一行注释即可。

---

## 任务二：新增 Murban Crude 和 DME Oman 价格

### 数据可行性分析

**Murban Crude（阿布扎比轻质原油，ADNOC/ICE）**
- 交易所：ICE Futures Abu Dhabi（IFAD），2021年3月上线
- akshare：**无此品种**（akshare 国际期货覆盖有限）
- Yahoo Finance：ticker `MCL=F` 或 `MBR=F`，**需验证是否可用**
- oilprice.com：有 Murban 现货价格页面，但**无公开 RSS/API**，需要爬取
- 备选：ICE 官网有数据但需注册；EIA 有部分参考价

**DME Oman（迪拜商品交易所阿曼原油）**
- 交易所：Dubai Mercantile Exchange（DME）
- akshare：**无此品种**
- Yahoo Finance：**无直接 ticker**
- oilprice.com：有 Oman Crude 页面，同样无公开 API
- 备选：DME 官网 `https://www.dubaimerc.com` 有每日结算价公告（HTML 页面，可爬取）；EIA 也有参考

### 可行方案（供讨论）

**方案A：爬取 oilprice.com 页面**
- 优点：数据权威，两个品种都有
- 缺点：需要 HTML 解析，oilprice.com 有反爬措施，维护成本高，可能随页面结构变化失效

**方案B：爬取 DME 官网结算价公告**
- DME Oman：`https://www.dubaimerc.com/settlement-prices` 每日更新
- Murban：ICE IFAD 官网有每日结算价 PDF/HTML
- 优点：官方数据，准确
- 缺点：两个来源不同，需要分别维护爬虫

**方案C：使用 EIA 参考价（最简单）**
- EIA 的 `https://www.eia.gov/dnav/pet/hist/LeafHandler.ashx?n=PET&s=RBRTE&f=D` 有 Brent 等参考价
- Oman/Murban 在 EIA 的 `spot_prices` 数据集中有月度数据，但**非日频**

**方案D：通过 worldmonitor.md 中的 EIA RSS**
- `https://news.google.com/rss/search?q=site:eia.gov energy oil gas when:14d`
- 这只是新闻，不是价格数据

**推荐方案**：方案B（DME 官网 + ICE IFAD 官网），作为独立的爬虫脚本 `get_crude_extra.py`，每日定时抓取，失败时静默跳过，不影响主流程。

### 需要确认的问题

1. 是否接受"可能有1-2天延迟"的数据（DME/ICE 官网通常次日更新）？
2. 如果 oilprice.com 爬取失败，是否有可接受的备用源？
3. Murban 和 DME Oman 是否需要交叉验证（类似 WTI/Brent 的 yfinance 验证）？

---

## 任务三：多方观点对比卡片

### 功能定位

不是新闻摘要，而是**结构化叙事抽取**：针对同一地缘事件，提取欧美/中国/伊朗三方的官方立场关键词和核心表述。

### 三方数据源

| 方 | 现有数据源 | 说明 |
|----|-----------|------|
| 欧美 | `official_west` 分类（白宫、国务院、IAEA） | 已有 |
| 伊朗 | `official_iran` 分类（Iran International） | 已有，但 Iran International 是**反伊朗政府**的英语媒体，并非伊朗官方立场 |
| 中国 | **暂无** | 需要新增 |

### 伊朗数据源问题

当前 `official_iran` 用的是 Iran International（总部在伦敦，立场反对伊朗政府），**不代表伊朗官方立场**。

真正的伊朗官方声音来源：
- **Press TV**：`https://www.presstv.ir/rss` — 伊朗国家英语电视台，代表官方立场
- **IRNA**（伊朗伊斯兰共和国通讯社）：`https://en.irna.ir/rss` — 官方通讯社
- **Tasnim News**：`https://www.tasnimnews.com/en/rss` — 伊朗半官方

### 中国数据源（来自 worldmonitor.md 参考）

worldmonitor.md 中没有直接列出中国官方媒体源，但可用以下可靠英语源：
- **Xinhua（新华社英文）**：`https://feeds.feedburner.com/xinhuanet/english` 或 `http://www.xinhuanet.com/english/rss/worldrss.xml`
- **China Daily**：`https://www.chinadaily.com.cn/rss/world_rss.xml`
- **Global Times**：`https://www.globaltimes.cn/rss/outboundfeeds/rss.xml` — 对外强硬立场，代表官方鹰派声音
- **CGTN**：`https://www.cgtn.com/subscribe/rss/section/world.xml`

推荐使用 **Xinhua + Global Times** 组合：前者是官方通讯社，后者在中东/伊朗议题上立场鲜明。

### 结构化抽取方案（供讨论）

**方案A：纯规则抽取（无 LLM）**
- 从各方最新 N 条新闻中，按关键词提取"立场词"（condemn/support/call for/reject 等）
- 优点：无 API 成本，实时
- 缺点：粗糙，容易误判

**方案B：LLM 结构化抽取（推荐）**
- 对每方最新 3-5 条新闻标题，调用 Claude API，提取：
  - 核心立场（1句话）
  - 关键词标签（3-5个）
  - 情绪倾向（强硬/中立/缓和）
- 结果缓存 6 小时，避免频繁调用
- 优点：质量高，结构化
- 缺点：需要 Anthropic API Key，有成本

**方案C：半规则半 LLM**
- 先用规则过滤出"同一事件"的相关新闻（关键词匹配）
- 再用 LLM 对过滤后的少量文本做结构化抽取
- 成本可控

### 前端展示设计（供讨论）

```
┌─────────────────────────────────────────────────────┐
│  多方观点对比  [事件：伊朗核谈判 · 更新于2小时前]      │
├──────────────┬──────────────┬───────────────────────┤
│   🇺🇸 欧美立场  │  🇨🇳 中国立场  │    🇮🇷 伊朗立场         │
├──────────────┼──────────────┼───────────────────────┤
│ 核心表述：    │ 核心表述：    │ 核心表述：              │
│ "伊朗必须停止 │ "各方应保持  │ "制裁是非法的，         │
│  铀浓缩活动"  │  克制与对话" │  我们不会妥协"          │
├──────────────┼──────────────┼───────────────────────┤
│ 标签：制裁    │ 标签：对话    │ 标签：抵制              │
│ 强硬 ↑       │ 中立 →       │ 强硬 ↑                 │
├──────────────┼──────────────┼───────────────────────┤
│ 来源：国务院  │ 来源：新华社  │ 来源：Press TV          │
│ 白宫声明      │ 环球时报      │ IRNA                   │
└──────────────┴──────────────┴───────────────────────┘
```

### 需要确认的问题

1. 是否接受使用 Claude API 做结构化抽取？还是纯规则方案？
2. "同一事件"的识别方式：手动选择事件？还是自动聚类？
3. 伊朗数据源：是否将 `official_iran` 改为真正的伊朗官方媒体（Press TV/IRNA）？
4. 中国数据源：Xinhua + Global Times 是否合适？

---

## 任务四：霍尔木兹海峡实时通船量监测

### 数据可行性分析（关键）

这是四个任务中**数据获取难度最高**的一个。

#### 需要的数据

- 过去24小时通过霍尔木兹海峡的船只数量
- 油轮 DWT（载重吨）
- 船只类型（VLCC/Suezmax/Aframax 等）

#### 数据源调研

**商业 AIS 数据（最准确，但收费）**
- MarineTraffic API：有霍尔木兹海峡区域船舶数据，但 API 需付费（$50-500/月）
- VesselFinder：类似，付费
- Spire Maritime：企业级，价格更高

**免费/半免费 AIS 数据**
- **AISHub**（`https://www.aishub.net`）：需要注册，有免费 API tier，但数据有延迟且覆盖不完整
- **OpenSeaMap**：开源，但霍尔木兹海峡覆盖不稳定
- **MarineTraffic 免费层**：只能查单船，无法批量查区域

**worldmonitor.md 中的相关源**
- **gCaptain**（`https://gcaptain.com/feed/`）：航运新闻，有霍尔木兹相关报道，但是**新闻**不是**实时数据**
- **Shipping & Freight RSS**：同样是新闻，非数据

**EIA 官方统计（最权威，但非实时）**
- EIA 每周发布"Petroleum Supply Weekly"，包含霍尔木兹海峡通过量
- 数据延迟约 1-2 周，且是周度数据，不是24小时实时
- URL：`https://www.eia.gov/petroleum/supply/weekly/`

**USNI News / gCaptain 新闻推断**
- 通过新闻中的"X tankers passed Hormuz"等表述间接获取
- 不准确，不系统

#### 结论

| 方案 | 数据质量 | 实时性 | 成本 | 可行性 |
|------|---------|--------|------|--------|
| MarineTraffic API | ⭐⭐⭐⭐⭐ | 实时 | 付费 | 需预算 |
| AISHub 免费层 | ⭐⭐⭐ | 15分钟延迟 | 免费（注册） | 可尝试 |
| EIA 周度数据 | ⭐⭐⭐⭐ | 1-2周延迟 | 免费 | 可行，但非"实时" |
| gCaptain 新闻 | ⭐⭐ | 不定期 | 免费 | 只能做新闻展示 |
| 自建 AIS 接收 | ⭐⭐⭐⭐ | 实时 | 硬件成本 | 不现实 |

**真实情况**：霍尔木兹海峡的**实时通船量**是商业数据，没有免费的精确来源。

### 建议的替代方案（供讨论）

**方案A：EIA 周度数据 + 新闻事件叠加**
- 展示 EIA 最新一周的霍尔木兹通过量（DWT、船次）
- 叠加 gCaptain/USNI 最新航运新闻
- 定位为"近期通过量趋势"而非"实时监测"

**方案B：AISHub 免费 API 尝试**
- 注册 AISHub，申请免费 API
- 定义霍尔木兹海峡地理围栏（经纬度范围：56.0°E-57.0°E, 25.5°N-26.5°N）
- 每小时查询该区域内的油轮数量
- 风险：免费层数据不完整，可能只有部分船只

**方案C：仅做新闻聚合展示**
- 不做实时数据，改为"霍尔木兹动态"：
  - 最新 gCaptain/USNI 航运新闻（已有 shipping 分类）
  - EIA 最新周度通过量（静态展示，每周更新）
  - 当前已知的封锁/威胁事件（来自 conflict 分类新闻）

### 需要确认的问题

1. 是否有预算购买 MarineTraffic API？（最低约 $50/月）
2. 如果没有预算，方案A（EIA 周度 + 新闻）是否满足需求？
3. "实时"的定义：是真正的分钟级实时，还是每日更新即可？

---

## 实施优先级建议

| 任务 | 难度 | 建议优先级 | 前置条件 |
|------|------|-----------|---------|
| 任务一（时间说明） | 低 | P1，先做 | 无 |
| 任务二（Murban/DME） | 中 | P2 | 确认数据源方案 |
| 任务三（多方观点） | 高 | P3 | 确认 LLM 方案 + 数据源 |
| 任务四（通船量） | 高 | P4 | 确认数据预算/方案 |

---

---

## 已确认方案与实施步骤

> 更新日期：2026-03-26

---

### 任务一：前端时区注释（最简单，先做）

**选定方案**：维持现有数据逻辑不变，仅在前端图表添加说明文字。

**涉及文件**：`web/src/pages/CrudeOilComparison.jsx`

**实施步骤**：

1. 在原油对比图表的图例下方（或右上角）添加一行小字说明：
   ```
   WTI：NYMEX收盘价（纽约时间，UTC-5/-4）
   Brent：ICE收盘价（伦敦时间，UTC+0）
   SC：INE收盘价（上海时间，UTC+8）
   各品种以各自交易所交易日为准，同一日期不代表同一时刻
   ```
2. 新增品种（Murban/DME Oman）的时间说明在任务二中一并加入。

**工作量**：小，约 10 行 JSX。

---

### 任务二：新增 Murban Crude 和 DME Oman 价格

**选定方案**：
- 主数据源（方案A）：爬取 oilprice.com（Murban 页面 ID 4464，DME Oman 页面待确认）
- 备用/补充数据源（方案D）：Google News RSS 搜索 EIA/价格相关新闻，用正则从标题提取价格，标注"仅供参考"
- 两源若都获取到数据，以 oilprice.com 数据为准
- 爬取失败则静默跳过，不影响主流程

**技术可行性确认**：
- oilprice.com 有 Murban 页面（`/oil-price-charts/4464`），参考 GitHub 上已有的爬虫案例，使用 `requests + BeautifulSoup` 加 Chrome UA 可绕过基本反爬
- DME Oman 页面 ID 需要在实施阶段先访问 oilprice.com 确认（很可能在同一 `/oil-price-charts/XXXX` 路径下）
- Google News RSS 中文/英文搜索均可用，价格提取用正则 `\$\s*(\d+\.?\d*)` 匹配标题中的美元数字

**需新增的数据库字段**：
- `crude_daily` 表新增两列：
  - `data_source TEXT`：值为 `"akshare"`（现有）/ `"scrape"`（oilprice爬取）/ `"news_estimate"`（新闻推断）
  - `is_reference INTEGER DEFAULT 0`：0=正式数据，1=仅供参考

**涉及文件**：
- `get_crude_data.py`：扩展 `CRUDE_SYMBOLS`，新增爬取逻辑
- `crude_api.py`：API 响应中暴露 `data_source` 和 `is_reference` 字段
- `api.py`：APScheduler 新增每日 16:00（上海时间）运行 extra crude 同步
- `web/src/pages/CrudeOilComparison.jsx`：新增两条折线，添加"仅供参考"标注和时区说明

**实施步骤**：

1. **后端 - 数据库迁移**：在 `init_crude_db()` 中用 `ALTER TABLE` 兼容性添加 `data_source` 和 `is_reference` 两列
2. **后端 - 爬虫实现**：在 `get_crude_data.py` 中新增 `fetch_oilprice_scrape(symbol, page_id)` 函数
   - 访问 `https://oilprice.com/oil-price-charts/{page_id}`
   - 用 BeautifulSoup 查找价格元素（实施时先调试确认选择器）
   - 提取当日价格和日期，写入 `crude_daily`，`data_source="scrape"`
3. **后端 - 新闻备用源**：新增 `fetch_eia_news_price(symbol)` 函数
   - 访问 Google News RSS：`https://news.google.com/rss/search?q=(Murban+crude+price)+when:2d`
   - 正则提取标题中的价格数字
   - 写入时标注 `data_source="news_estimate"`, `is_reference=1`
4. **后端 - 主入口**：扩展 `connect_and_fetch_crude()` 调用新增函数，先尝试 scrape，失败则尝试 news_estimate
5. **后端 - API**：`crude_api.py` 的响应中加入 `data_source` 和 `is_reference` 字段
6. **前端**：在图表中加入 MURBAN 和 DME_OMAN 折线；对 `is_reference=1` 的数据点用虚线/特殊颜色+tooltip 提示"数据来源：新闻推断，仅供参考"；图例下方加时区说明（含任务一内容）

---

### 任务三：多方视角对比卡片

**选定方案**：
- 不使用 LLM，纯数据展示
- 每方展示相关性最高的 3 条新闻标题（已翻译的中文标题 `title_zh`）+ 原文链接
- 相关性 = 现有 `priority` 排序（数值越小越相关）
- 无立场判断，仅提供信息
- 中国官方源直接出中文，不需翻译

**伊朗数据源调整**：
- 移除 Iran International（反伊朗政府，立场不中立）
- 改为：
  - **Press TV**：`https://www.presstv.ir/rss`（伊朗国家英语电视台）
  - **IRNA**（伊朗伊斯兰共和国通讯社）：`https://en.irna.ir/rss`
  - **Tasnim News**：`https://www.tasnimnews.com/en/rss`（半官方）
- category 仍为 `official_iran`，`title_zh` 照常翻译

**中国新数据源（新增 `official_china` 分类）**：

> **百度新闻 RSS 可行性**：经验证，百度新闻原生 RSS（`news.baidu.com/rss/`）在 2024-2025 年已基本废弃，实际为动态加载页面，不返回有效 XML。依赖第三方 RSSHub 不稳定且增加外部依赖。**结论：百度不可用，维持使用 Google News 中文 RSS**，稳定性已被 worldmonitor.md 项目验证。

| 来源 | RSS 方式 | 语言 | 说明 |
|------|---------|------|------|
| 新华社 | Google News RSS：`site:news.cn 伊朗 OR 霍尔木兹 OR 原油` | 中文 | 直接抓中文标题，不翻译 |
| 中国日报（中文版）| Google News RSS：`site:chinadaily.com.cn 伊朗 OR 霍尔木兹` | 中文 | 同上 |
| 环球网 | Google News RSS：`site:huanqiu.com 伊朗 OR 霍尔木兹 OR 原油` | 中文 | 同上 |
| CGTN | 直连 RSS：`https://www.cgtn.com/subscribe/rss/section/world.xml` | 英文 | 仍需翻译至中文 |

- Google News 中文 RSS 完整参数：`https://news.google.com/rss/search?q=site:news.cn+伊朗+OR+霍尔木兹+OR+原油&hl=zh-CN&gl=CN&ceid=CN:zh-Hans`
- 返回标题直接为中文，存入 `title` 字段，`title_zh` 直接复制 `title`（标记跳过翻译）
- `priority` 关键词权重沿用现有逻辑，中文关键词匹配：伊朗、霍尔木兹、原油、制裁、核

**新增 API 端点**：`GET /api/news/perspectives`

响应格式：
```json
{
  "west":  [{"id", "title_zh", "url", "source_name", "published_at", "priority"}, ...],
  "china": [{"id", "title_zh", "url", "source_name", "published_at", "priority"}, ...],
  "iran":  [{"id", "title_zh", "url", "source_name", "published_at", "priority"}, ...],
  "updated_at": "2026-03-26T10:00:00"
}
```
每方最多 3 条，按 `priority ASC, published_at DESC` 排序，取近 7 天内的数据。

**实施步骤**：

1. **后端 - `get_news_data.py`**：
   - 修改 `official_iran` 的 sources：替换为 Press TV / IRNA / Tasnim
   - 新增 `official_china` category，配置 4 个源（含 Google News 中文 RSS）
   - 中文源的 `title_zh` 处理：直接将 `title` 复制给 `title_zh`，跳过 deep-translator 翻译
2. **后端 - `news_api.py`**：
   - `VALID_CATEGORIES` 增加 `official_china`
   - `_CATEGORY_ZH` 增加 `"official_china": "中国官方"`
   - 新增 `GET /api/news/perspectives` 端点
3. **前端 - `CrudeOilComparison.jsx`**：
   - 新增"多方视角"卡片区块，三列布局
   - 每列：来源标签 + 3 条新闻标题（点击跳转原文）
   - 底部免责声明："以下内容均来自各方官方媒体，仅供信息参考，不代表本站立场"
4. **前端 - `web/src/api/crudeApi.js`**：新增 `fetchPerspectives()` 函数

---

### 任务四：霍尔木兹海峡船舶监测

**选定方案**：AISHub 实时数据（方案B）+ 航运新闻聚合（方案C）双层展示。

**AISHub API 技术确认**：
- 支持地理围栏 bounding box 查询：参数 `latmin/latmax/lonmin/lonmax`
- 霍尔木兹海峡围栏：`latmin=25.5&latmax=26.8&lonmin=56.0&lonmax=57.5`
- 返回字段包含：MMSI、船名、船型代码（`TYPE`）、经纬度、速度、航向等
- 油轮过滤：TYPE 80-89（AIS 标准油轮代码），在客户端过滤
- **重要限制**：AISHub 免费 API 需要贡献 AIS 接收站才能获取 API 访问权限，不是简单注册即可用

**注册方式选择（已确认）**：直接使用 **AISStream.io**（方式三），无需物理接收站，注册免费。

**AISStream.io 技术确认**：
- 免费，无需贡献接收站，注册后即获 API Key
- WebSocket 连接：`wss://stream.aisstream.io/v0/stream`
- 订阅时发送 JSON 消息，包含 `APIKey` + `BoundingBoxes`
- 霍尔木兹围栏：`[[25.5, 56.0], [26.8, 57.5]]`（SW角 + NE角）
- 返回消息类型：`PositionReport`，包含 MMSI、经纬度、速度、航向、船型（`ShipType`）等
- 油轮过滤：`ShipType` 80-89 范围（AIS 标准油轮代码）
- **注意**：AISStream 是流式 WebSocket，不是 HTTP 轮询；采集脚本需要在指定时间窗口订阅，收集一段时间（如 5 分钟）的数据后断开，将结果聚合写入数据库
- 平均全球流量约 300 条/秒，霍尔木兹小区域远低于此，系统压力小

**新增数据库表**：

```sql
-- 当前区域船舶列表（每次轮询覆盖写入）
CREATE TABLE hormuz_vessels (
  id         INTEGER PRIMARY KEY AUTOINCREMENT,
  mmsi       TEXT    NOT NULL,
  ship_name  TEXT,
  vessel_type INTEGER,          -- AIS TYPE code
  lat        REAL,
  lon        REAL,
  speed      REAL,
  course     REAL,
  nav_status INTEGER,
  fetched_at TEXT NOT NULL      -- ISO timestamp
);

-- 每小时快照（用于24小时趋势图）
CREATE TABLE hormuz_snapshot (
  id              INTEGER PRIMARY KEY AUTOINCREMENT,
  snapshot_at     TEXT    NOT NULL UNIQUE,
  vessel_count    INTEGER,       -- 区域内全部船只数
  tanker_count    INTEGER,       -- 其中油轮数（TYPE 80-89）
  data_quality    TEXT           -- "full"/"partial"/"unavailable"
);
```

**涉及新文件**：`get_ais_data.py`、`hormuz_api.py`

**涉及修改文件**：`api.py`（挂载新路由 + 调度任务）、`web/src/pages/CrudeOilComparison.jsx`、`web/src/api/crudeApi.js`

**实施步骤**：

1. **后端 - `get_ais_data.py`**：
   - 读取 env var `AISHUB_USERNAME`，若未设置则直接返回空
   - 调用 bounding box API，解析 JSON 响应
   - 过滤 TYPE 80-89 得到油轮列表
   - 写入 `hormuz_vessels`（先清空当轮数据再插入）
   - 写入 `hormuz_snapshot`（按小时 upsert）
   - 失败静默，记录错误到 `sync_state`（`hormuz_last_status`/`hormuz_last_error`）

2. **后端 - `hormuz_api.py`**：
   - `GET /api/hormuz/current`：返回最新一次快照（vessel_count、tanker_count）+ 完整船只列表
   - `GET /api/hormuz/history`：返回最近 24 条快照记录（用于趋势图）
   - `GET /api/hormuz/news`：直接复用 `/api/news/hormuz` 的逻辑

3. **后端 - `api.py`**：
   - `include_router(hormuz_router)`
   - APScheduler 每 30 分钟运行一次 AIS 查询

4. **前端 - CrudeOilComparison.jsx**：
   - 新增"霍尔木兹通道"监测区块
   - 顶部：当前船只数 / 油轮数 / 最后更新时间 三个数字卡片
   - 中部：24 小时船只数趋势折线图
   - 底部：最新航运新闻列表（复用现有 hormuz 新闻）
   - 若 AIS 数据不可用（未配置 API KEY），仅展示新闻部分，隐藏数字卡片和趋势图

5. **配置**：`.env.example` 新增 `AISSTREAM_API_KEY=` 说明（用户需在 aisstream.io 注册后填入）

---

## 并行工作流拆分

确认可以拆分为两个独立工作流并行实施，最后合并：

### 工作流 A：任务一 + 任务三

**改动范围**：
- `get_news_data.py`：修改 `official_iran` 数据源，新增 `official_china` category 和 4 个中文新闻源
- `news_api.py`：新增 `VALID_CATEGORIES` 中的 `official_china`，新增 `/api/news/perspectives` 端点
- `web/src/pages/CrudeOilComparison.jsx`：新增多方视角卡片 + 任务一时区注释
- `web/src/api/crudeApi.js`：新增 `fetchPerspectives()` 函数

**无数据库 schema 变更**（用现有 `crude_news` 表，只新增 category 值）

**无外部依赖**（Google News RSS 直接可用）

---

### 工作流 B：任务二 + 任务四

**改动范围**：
- `get_crude_data.py`：新增 `data_source`/`is_reference` 字段、oilprice.com 爬虫、Google News 价格备用源
- `crude_api.py`：新增字段透传，更新 `/api/crude/daily` 支持新品种
- 新文件 `get_ais_data.py`：AISStream.io WebSocket 采集，写入 `hormuz_vessels`/`hormuz_snapshot`
- 新文件 `hormuz_api.py`：`/api/hormuz/*` 端点
- `api.py`：挂载 hormuz_router，新增调度任务
- `web/src/pages/CrudeOilComparison.jsx`：新增 Murban/DME 折线 + 霍尔木兹监测区块
- `web/src/api/crudeApi.js`：新增相关 API 调用函数
- `.env.example`：新增 `AISSTREAM_API_KEY=`

**有数据库 schema 变更**（`crude_daily` 新增列，新增 `hormuz_*` 表）

**外部依赖**：用户需先在 [aisstream.io](https://aisstream.io) 注册获取免费 API Key

---

### 合并策略

两个工作流均在独立 git worktree 中进行，不修改同一函数（`CrudeOilComparison.jsx` 各自修改不同区块，最终手动合并该文件）。其余文件无重叠。

---

## 实施顺序与依赖关系

```
任务一  ──────────────────────────────────────────> 直接实施（无依赖）

任务三(后端新闻源)  ──> 任务三(API端点)  ──> 任务三(前端卡片)

任务二(后端爬虫)  ──> 任务二(API字段)  ──> 任务二(前端图表)
                                            ↑
                                      （任务一的注释一并加入）

任务四：需用户先确认 AISHub 账号可用
        ──> get_ais_data.py ──> hormuz_api.py ──> 前端监测区块
```

**建议执行顺序**：任务一 → 任务三 → 任务二 → 任务四（任务四等用户拿到 AISHub API key 后再做）

---

## 风险与注意事项

| 风险 | 任务 | 影响 | 应对 |
|------|------|------|------|
| oilprice.com 反爬导致爬取失败 | 二 | 无主数据 | 静默跳过，降级到 news_estimate；定期手动检查选择器是否失效 |
| Google News 中文 RSS 返回英文标题 | 三 | 需翻译 | 实施时测试验证；若返回英文则走现有 deep-translator 翻译链路 |
| Press TV / IRNA RSS 被封锁（伊朗媒体） | 三 | 无伊朗侧数据 | 设置 10 秒超时，失败静默；前端展示"暂无数据" |
| AISHub 无法获得免费 API（无接收站） | 四 | AIS 部分全部失败 | 前端自动降级为仅新闻模式；可改用 AISStream.io |
| AISHub 数据覆盖不完整（免费层） | 四 | 船只数低于实际 | 在 UI 加注"数据来源：AISHub，覆盖可能不完整" |
