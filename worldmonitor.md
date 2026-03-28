# WorldMonitor 新闻抓取架构完全参考

> 本文档为 worldmonitor 项目新闻抓取、汇总、分类、摘要全流程的详细技术参考，
> 用于在原油/美伊冲突监控等类似项目中复用同等架构。
> 生成日期：2026-03-26

---

## 目录

1. [项目整体架构](#1-项目整体架构)
2. [新闻源完整清单](#2-新闻源完整清单)
3. [RSS 抓取机制](#3-rss-抓取机制)
4. [新闻分类系统](#4-新闻分类系统)
5. [新闻聚合 Pipeline](#5-新闻聚合-pipeline)
6. [LLM 摘要生成](#6-llm-摘要生成)
7. [Breaking News 预警系统](#7-breaking-news-预警系统)
8. [缓存架构](#8-缓存架构)
9. [部署与基础设施](#9-部署与基础设施)
10. [原油/伊朗专项监控策略](#10-原油伊朗专项监控策略)
11. [关键文件索引](#11-关键文件索引)

---

## 1. 项目整体架构

### 技术栈

- **前端**：TypeScript SPA（React），Tauri 桌面版
- **后端**：Vercel Edge Functions（无状态，Node.js 运行时）
- **缓存**：Upstash Redis（跨实例共享）
- **中继代理**：Railway 部署的 RSS relay 服务（规避 Vercel IP 被封问题）
- **数据传输**：Protocol Buffers（protobuf）定义 API 接口
- **ML 推理**：@xenova/transformers（ONNX，纯浏览器端）

### 五种变体（Variant）

系统支持五种场景变体，每种有不同的新闻源集合：

| Variant | 说明 | 适用场景 |
|---------|------|----------|
| `full` | 地缘政治、冲突、军事、能源 | **原油/冲突监控首选** |
| `finance` | 金融市场、大宗商品、加密 | 油价金融面分析 |
| `commodity` | 矿业、金属、能源、大宗商品 | 原油供需基本面 |
| `tech` | 科技、AI、创业 | 技术类项目 |
| `happy` | 正面新闻 | 情绪对冲 |

**对于原油/美伊战争监控，核心使用 `full` + `finance` + `commodity` 三个变体。**

---

## 2. 新闻源完整清单

### 2.1 `full` 变体 — 地缘政治核心源

#### Politics（国际政治）
| 名称 | RSS URL | 说明 |
|------|---------|------|
| BBC World | `https://feeds.bbci.co.uk/news/world/rss.xml` | 直连 |
| Guardian World | `https://www.theguardian.com/world/rss` | 直连 |
| AP News | `https://news.google.com/rss/search?q=site:apnews.com` | GN代理 |
| Reuters World | `https://news.google.com/rss/search?q=site:reuters.com world` | GN代理 |
| CNN World | `https://news.google.com/rss/search?q=site:cnn.com world news when:1d` | GN代理 |

#### US（美国政治）
| 名称 | RSS URL |
|------|---------|
| Reuters US | `https://news.google.com/rss/search?q=site:reuters.com US` |
| NPR News | `https://feeds.npr.org/1001/rss.xml` |
| PBS NewsHour | `https://www.pbs.org/newshour/feeds/rss/headlines` |
| ABC News | `https://feeds.abcnews.com/abcnews/topstories` |
| CBS News | `https://www.cbsnews.com/latest/rss/main` |
| NBC News | `https://feeds.nbcnews.com/nbcnews/public/news` |
| Wall Street Journal | `https://feeds.content.dowjones.io/public/rss/RSSUSnews` |
| Politico | `https://rss.politico.com/politics-news.xml` |
| The Hill | `https://thehill.com/news/feed` |
| Axios | `https://api.axios.com/feed/` |

#### Middle East（中东 — 最关键区域）
| 名称 | RSS URL | 备注 |
|------|---------|------|
| BBC Middle East | `https://feeds.bbci.co.uk/news/world/middle_east/rss.xml` | 直连 |
| Al Jazeera | `https://www.aljazeera.com/xml/rss/all.xml` | 直连 |
| Guardian ME | `https://www.theguardian.com/world/middleeast/rss` | 直连 |
| Oman Observer | `https://www.omanobserver.om/rssFeed/1` | 霍尔木兹海峡地区 |
| BBC Persian | `https://feeds.bbci.co.uk/persian/rss.xml` | 波斯语，`lang: fa` |
| The National | `https://www.thenationalnews.com/arc/outboundfeeds/rss/?outputType=xml` | 阿联酋观点 |

#### Europe（欧洲）
| 名称 | RSS URL | 语言 |
|------|---------|------|
| France 24 | `https://www.france24.com/en/rss` | en |
| EuroNews | `https://www.euronews.com/rss?format=xml` | en |
| Le Monde | `https://www.lemonde.fr/en/rss/une.xml` | en |
| DW News | `https://rss.dw.com/xml/rss-en-all` | en |
| Tagesschau | `https://www.tagesschau.de/xml/rss2/` | de |
| ANSA | `https://www.ansa.it/sito/ansait_rss.xml` | it |
| NOS Nieuws | `https://feeds.nos.nl/nosnieuwsalgemeen` | nl |
| SVT Nyheter | `https://www.svt.se/nyheter/rss.xml` | sv |

#### Asia（亚洲）
| 名称 | RSS URL |
|------|---------|
| BBC Asia | `https://feeds.bbci.co.uk/news/world/asia/rss.xml` |
| The Diplomat | `https://thediplomat.com/feed/` |
| Nikkei Asia | `https://news.google.com/rss/search?q=site:asia.nikkei.com when:3d` |
| CNA | `https://www.channelnewsasia.com/api/v1/rss-outbound-feed?_format=xml` |
| NDTV | `https://feeds.feedburner.com/ndtvnews-top-stories` |
| South China Morning Post | `https://news.google.com/rss/search?q=site:scmp.com when:2d` |
| The Hindu | `https://www.thehindu.com/feeder/default.rss` |

#### Africa（非洲）
| 名称 | RSS URL |
|------|---------|
| BBC Africa | `https://feeds.bbci.co.uk/news/world/africa/rss.xml` |
| News24 | `https://feeds.news24.com/articles/news24/TopStories/rss` |
| Africanews | `https://www.africanews.com/feed/` |
| Jeune Afrique | `https://www.jeuneafrique.com/feed/` |
| Premium Times | `https://www.premiumtimesng.com/feed` |

#### Latin America（拉美）
| 名称 | RSS URL |
|------|---------|
| BBC Latin America | `https://feeds.bbci.co.uk/news/world/latin_america/rss.xml` |
| Guardian Americas | `https://www.theguardian.com/world/americas/rss` |
| InSight Crime | `https://insightcrime.org/feed/` |
| Infobae | `https://www.infobae.com/arc/outboundfeeds/rss/` |
| Clarín | `https://www.clarin.com/rss/lo-ultimo/` |

#### Energy（能源 — 原油核心）
| 名称 | RSS URL | 关键词覆盖 |
|------|---------|-----------|
| Oil & Gas | `https://news.google.com/rss/search?q=(oil price OR OPEC OR "natural gas" OR pipeline OR LNG) when:2d` | 油价、OPEC、管道、LNG |
| Reuters Energy | `https://news.google.com/rss/search?q=site:reuters.com energy when:2d` | 路透社能源 |
| Nuclear Energy | `https://news.google.com/rss/search?q=("nuclear energy" OR "nuclear power" OR "nuclear reactor") when:3d` | 核能 |

#### Think Tanks（智库）
| 名称 | RSS URL |
|------|---------|
| Foreign Policy | `https://foreignpolicy.com/feed/` |
| Atlantic Council | `https://www.atlanticcouncil.org/feed/` |
| Foreign Affairs | `https://www.foreignaffairs.com/rss.xml` |
| War on the Rocks | `https://warontherocks.com/feed/` |
| CSIS | `https://www.csis.org/rss.xml` |

#### Government（政府机构）
| 名称 | RSS URL |
|------|---------|
| White House | `https://news.google.com/rss/search?q=site:whitehouse.gov` |
| State Department | `https://news.google.com/rss/search?q=site:state.gov OR "State Department"` |
| Pentagon | `https://news.google.com/rss/search?q=site:defense.gov OR Pentagon` |
| Federal Reserve | `https://www.federalreserve.gov/feeds/press_all.xml` |
| UN News | `https://news.un.org/feed/subscribe/en/news/all/rss.xml` |
| CISA | `https://www.cisa.gov/cybersecurity-advisories/all.xml` |
| Treasury | `https://news.google.com/rss/search?q=site:treasury.gov` |

#### Crisis（危机）
| 名称 | RSS URL |
|------|---------|
| CrisisWatch | `https://www.crisisgroup.org/rss` |
| IAEA | `https://www.iaea.org/feeds/topnews` |
| WHO | `https://www.who.int/rss-feeds/news-english.xml` |

---

### 2.2 Intel Sources（军事/情报源 — `full` 变体专属）

这些是 `INTEL_SOURCES`，只在 `full` 变体下加载，归入 `intel` 分类：

| 名称 | RSS URL | 专长 |
|------|---------|------|
| **Defense One** | `https://www.defenseone.com/rss/all/` | 美国国防政策 |
| **The War Zone** | `https://www.twz.com/feed` | 军事冲突详情 |
| **Defense News** | `https://www.defensenews.com/arc/outboundfeeds/rss/?outputType=xml` | 防务采购/战略 |
| **Military Times** | `https://www.militarytimes.com/arc/outboundfeeds/rss/?outputType=xml` | 军事行动 |
| **Task & Purpose** | `https://taskandpurpose.com/feed/` | 美军视角 |
| **USNI News** | `https://news.usni.org/feed` | 海军/舰船动态 |
| **gCaptain** | `https://gcaptain.com/feed/` | 航运/海事（波斯湾关键） |
| **Oryx OSINT** | `https://www.oryxspioenkop.com/feeds/posts/default?alt=rss` | 装备损失追踪（OSINT） |
| **Foreign Policy** | `https://foreignpolicy.com/feed/` | 地缘战略 |
| **Foreign Affairs** | `https://www.foreignaffairs.com/rss.xml` | 战略分析 |
| **Atlantic Council** | `https://www.atlanticcouncil.org/feed/` | 智库政策 |
| **Bellingcat** | `https://news.google.com/rss/search?q=site:bellingcat.com` | 开源情报调查 |
| **Krebs Security** | `https://krebsonsecurity.com/feed/` | 网络安全 |
| **Arms Control Assn** | `https://news.google.com/rss/search?q=site:armscontrol.org` | 军备控制 |
| **Bulletin of Atomic Scientists** | `https://news.google.com/rss/search?q=site:thebulletin.org` | 核威胁评估 |
| **FAO News** | `https://www.fao.org/feeds/fao-newsroom-rss` | 粮食安全/制裁影响 |

---

### 2.3 `finance` 变体 — 金融/商品源

#### Markets（市场）
| 名称 | RSS URL |
|------|---------|
| CNBC | `https://www.cnbc.com/id/100003114/device/rss/rss.html` |
| Yahoo Finance | `https://finance.yahoo.com/rss/topstories` |
| Seeking Alpha | `https://seekingalpha.com/market_currents.xml` |
| Financial Times | `https://www.ft.com/rss/home` |
| Reuters Business | `https://news.google.com/rss/search?q=site:reuters.com business markets` |

#### Commodities（大宗商品）
| 名称 | RSS URL |
|------|---------|
| Oil & Gas | `https://news.google.com/rss/search?q=(oil price OR OPEC OR "natural gas" OR pipeline OR LNG) when:2d` |
| Gold & Metals | `https://news.google.com/rss/search?q=("gold price" OR "silver price" OR "precious metals" OR "copper price") when:2d` |

#### GCC / Gulf（海湾国家）
| 名称 | RSS URL |
|------|---------|
| Arabian Business | `https://news.google.com/rss/search?q=site:arabianbusiness.com (Saudi Arabia OR UAE OR GCC) when:7d` |
| The National | `https://news.google.com/rss/search?q=site:thenationalnews.com (Abu Dhabi OR UAE OR Saudi) when:7d` |
| Arab News | `https://news.google.com/rss/search?q=site:arabnews.com (Saudi Arabia OR investment OR infrastructure) when:7d` |
| Gulf FDI | `https://news.google.com/rss/search?q=(PIF OR "DP World" OR Mubadala OR ADNOC OR Masdar) infrastructure when:7d` |
| Vision 2030 | `https://news.google.com/rss/search?q="Vision 2030" (project OR investment OR announced) when:14d` |

---

### 2.4 `commodity` 变体 — 能源/矿产源

#### Energy（能源 — 原油专项）
| 名称 | RSS URL | 说明 |
|------|---------|------|
| **OilPrice.com** | `https://oilprice.com/rss/main` | 原油专业媒体，直连 |
| **Rigzone** | `https://www.rigzone.com/news/rss/rigzone_latest.aspx` | 油气行业 |
| EIA Reports | `https://news.google.com/rss/search?q=site:eia.gov energy oil gas when:14d` | 美国能源信息署 |
| OPEC News | `https://news.google.com/rss/search?q=(OPEC OR "oil price" OR "crude oil" OR WTI OR Brent OR "oil production") when:1d` | OPEC动态 |
| Natural Gas News | `https://news.google.com/rss/search?q=("natural gas" OR LNG OR "gas price" OR "Henry Hub") when:1d` | 天然气 |
| Energy Intel | `https://news.google.com/rss/search?q=(energy commodities OR "energy market" OR "energy prices") when:2d` | 能源市场 |
| Reuters Energy | `https://news.google.com/rss/search?q=site:reuters.com (oil OR gas OR energy) when:1d` | 路透能源 |

#### Supply Chain（供应链 — 霍尔木兹相关）
| 名称 | RSS URL |
|------|---------|
| Shipping & Freight | `https://news.google.com/rss/search?q=("bulk carrier" OR "dry bulk" OR "commodity shipping" OR "Port Hedland" OR "Strait of Hormuz") when:3d` |
| Trade Routes | `https://news.google.com/rss/search?q=("trade route" OR "supply chain" OR "commodity export" OR "mineral export") when:3d` |
| China Commodity Imports | `https://news.google.com/rss/search?q=China imports copper OR "iron ore" OR lithium OR cobalt OR "rare earth" when:3d` |

#### Critical Minerals（战略矿产）
| 名称 | RSS URL |
|------|---------|
| Critical Minerals | `https://news.google.com/rss/search?q=("critical minerals" OR "battery metals" OR lithium OR cobalt OR "rare earths") when:2d` |
| Uranium Market | `https://news.google.com/rss/search?q=(uranium price OR "uranium market" OR U3O8 OR nuclear fuel) when:3d` |

---

## 3. RSS 抓取机制

### 3.1 请求方式

```typescript
// 核心抓取函数（fetchRssText）
const response = await fetch(url, {
  headers: {
    'User-Agent': CHROME_UA,           // Chrome 浏览器 UA，规避机器人检测
    'Accept': 'application/rss+xml, application/xml, text/xml, */*',
    'Accept-Language': 'en-US,en;q=0.9',
  },
  signal: AbortSignal.timeout(8000),   // 8秒超时
});
```

### 3.2 Railway 中继回退

当直连 RSS 被 Vercel IP 封锁时，自动切换到 Railway 中继服务：

```typescript
// 直连失败 → 切换中继
if (!text) {
  const relayUrl = `${RAILWAY_RELAY_BASE}/rss?url=${encodeURIComponent(feedUrl)}`;
  const resp = await fetch(relayUrl, { headers: relayAuthHeaders });
  if (resp.ok) text = await resp.text();
}
```

**中继的意义**：Reuters、Bloomberg 等大媒体会屏蔽 Vercel 的公共 IP。Railway 实例有独立 IP，成功率更高。

### 3.3 并发控制

```
总并发限制：每批 20 个 feed 并行
每个 feed 超时：8 秒
整体 deadline：25 秒（超时则截断未完成的 feed）
每个 feed 取前 5 条新闻
每个分类最多保留 20 条
```

### 3.4 RSS / Atom XML 解析

使用纯正则解析（非 DOM，更快更轻量）：

```typescript
// RSS 2.0：匹配 <item>...</item>
const itemRegex = /<item[\s>]([\s\S]*?)<\/item>/gi;

// Atom：匹配 <entry>...</entry>
const entryRegex = /<entry[\s>]([\s\S]*?)<\/entry>/gi;

// 提取字段：title, link, pubDate/published/updated
```

**支持两种内容格式**：
- `<![CDATA[...]]>` 包裹的内容
- 普通文本内容（自动解码 XML 实体）

**XML 实体解码**：
```
&amp; → &
&lt;  → <
&gt;  → >
&quot; → "
&apos; → '
&#123; → 数字编码字符
&#x1F; → 十六进制编码字符
```

### 3.5 Google News 代理技巧

对于屏蔽直接 RSS 抓取的媒体，使用 Google News 搜索代理：

```javascript
const gn = (q) =>
  `https://news.google.com/rss/search?q=${encodeURIComponent(q)}&hl=en-US&gl=US&ceid=US:en`;

// 示例：
gn('site:reuters.com energy when:2d')         // 路透社能源，2天内
gn('(oil price OR OPEC OR WTI OR Brent) when:1d')  // 油价关键词，1天内
gn('site:bellingcat.com')                      // Bellingcat OSINT
```

**`when:Nd` 参数**：限制新闻时间范围，`when:1d`=今天，`when:2d`=两天内，`when:7d`=一周内。

---

## 4. 新闻分类系统

### 4.1 威胁等级（ThreatLevel）

| 等级 | 说明 | 置信度 | 触发预警 |
|------|------|--------|----------|
| `critical` | 核攻击、入侵、政变、大规模伤亡 | 0.9 | ✅ |
| `high` | 战争、空袭、导弹、制裁、恐袭 | 0.8 | ✅ |
| `medium` | 抗议、军演、外交危机、市场崩溃 | 0.7 | ❌ |
| `low` | 选举、峰会、协议、气候 | 0.6 | ❌ |
| `info` | 一般新闻或被排除的内容 | 0.3 | ❌ |

### 4.2 关键词分类规则（完整）

#### CRITICAL 级别关键词（`confidence: 0.9`）
```
nuclear strike → military     nuclear attack → military     nuclear war → military
invasion → conflict            declaration of war → conflict  martial law → military
coup → military                coup attempt → military        genocide → conflict
ethnic cleansing → conflict    chemical attack → terrorism    biological attack → terrorism
dirty bomb → terrorism         mass casualty → conflict       pandemic declared → health
health emergency → health      nato article 5 → military      evacuation order → disaster
meltdown → disaster            nuclear meltdown → disaster
```

#### HIGH 级别关键词（`confidence: 0.8`）
```
war → conflict                armed conflict → conflict       airstrike → conflict
air strike → conflict          drone strike → conflict         missile → military
missile launch → military      troops deployed → military      military escalation → military
bombing → conflict             casualties → conflict           hostage → terrorism
terrorist → terrorism          terror attack → terrorism       assassination → crime
cyber attack → cyber           ransomware → cyber              data breach → cyber
sanctions → economic           embargo → economic              earthquake → disaster
tsunami → disaster             hurricane → disaster            typhoon → disaster
```

#### MEDIUM 级别关键词（`confidence: 0.7`）
```
protest/protests → protest     riot/riots → protest            unrest → protest
demonstration → protest        strike action → protest         military exercise → military
naval exercise → military      arms deal → military            weapons sale → military
diplomatic crisis → diplomatic ambassador recalled → diplomatic expel diplomats → diplomatic
trade war → economic           tariff → economic               recession → economic
inflation → economic           market crash → economic         flood/flooding → disaster
wildfire → disaster            volcano → disaster              eruption → disaster
outbreak → health              epidemic → health               infection spread → health
oil spill → environmental      pipeline explosion → infrastructure  blackout → infrastructure
power outage → infrastructure  internet outage → infrastructure    derailment → infrastructure
```

#### LOW 级别关键词（`confidence: 0.6`）
```
election/vote/referendum → diplomatic   summit → diplomatic        treaty/agreement → diplomatic
negotiation/talks → diplomatic          peacekeeping → diplomatic   humanitarian aid → diplomatic
ceasefire → diplomatic                  peace treaty → diplomatic   climate change → environmental
emissions → environmental               pollution → environmental    deforestation → environmental
drought → environmental                 vaccine/vaccination → health disease → health
virus → health                          public health → health       covid → health
interest rate → economic                gdp → economic              unemployment → economic
regulation → economic
```

#### 排除列表（过滤娱乐/生活类内容）
```
protein, couples, relationship, dating, diet, fitness, recipe, cooking,
shopping, fashion, celebrity, movie, tv show, sports, game, concert,
festival, wedding, vacation, travel tips, life hack, self-care, wellness
```

### 4.3 事件分类（EventCategory）

```
conflict    | protest      | disaster   | diplomatic  | economic
terrorism   | cyber        | health     | environmental | military
crime       | infrastructure | tech     | general
```

### 4.4 短词边界匹配

以下短词使用 `\bword\b` 词边界正则，防止误匹配：
```
war, coup, ban, vote, riot, riots, hack, talks, ipo, gdp, virus, disease, flood
```

### 4.5 LLM 辅助分类增强

对于关键词分类置信度 < 0.9 的条目，查询 LLM 分类缓存进行增强：

```typescript
// 以标题小写的 SHA256 前16位作为缓存键
const hash = sha256Hex(title.toLowerCase()).slice(0, 16);
const cacheKey = `classify:sebuf:v1:${hash}`;

// 如果命中 LLM 缓存，置信度提升到 0.9，来源标记为 'llm'
if (cached.level && cached.level !== '_skip') {
  item.level = cached.level;
  item.category = cached.category;
  item.confidence = 0.9;
  item.classSource = 'llm';
}
```

**注意**：LLM 分类在后台异步写入缓存，前端读取时已是结果，无需实时调用 LLM。

---

## 5. 新闻聚合 Pipeline

### 5.1 完整流程图

```
HTTP Request (variant, lang)
    │
    ▼
检查 Redis 摘要缓存 news:digest:v1:{variant}:{lang}  (TTL: 15分钟)
    │  命中 → 直接返回
    │
    ▼ 缓存未命中
加载该 variant 所有 feed 配置
    │  (full variant 额外加载 INTEL_SOURCES → 归入 intel 分类)
    │  (按语言过滤，例如 lang=de 只加载 lang:de 的 feed)
    │
    ▼
并发批量拉取 RSS (批量大小: 20)
    │  每个 feed → 检查 Redis 单源缓存 rss:feed:v1:{variant}:{url} (TTL: 1小时)
    │              缓存未命中 → 直连 RSS → 失败则 Railway 中继
    │              解析 XML → 每个 feed 取前5条
    │              关键词分类 (classifyByKeyword)
    │
    ▼
合并所有 feed 的条目，按分类聚合
    │
    ▼
每个分类内按 publishedAt 降序排序
    │  每个分类最多保留 20 条
    │
    ▼
批量查询 LLM 分类缓存 (enrichWithAiCache)
    │  对所有关键词分类条目批量查 Redis
    │  命中则提升置信度，来源改为 llm
    │
    ▼
构建响应 (categories, feedStatuses, generatedAt)
    │  写入摘要缓存 Redis (TTL: 900秒 = 15分钟)
    │
    ▼
返回 ListFeedDigestResponse
```

### 5.2 响应数据结构

```typescript
interface ListFeedDigestResponse {
  categories: {
    [categoryName: string]: {
      items: NewsItem[];
    }
  };
  feedStatuses: {
    [feedName: string]: 'empty' | 'timeout'  // 只记录有问题的 feed
  };
  generatedAt: string;  // ISO timestamp
}

interface NewsItem {
  source: string;       // feed 名称，如 "Reuters World"
  title: string;        // 新闻标题（已解码 XML 实体）
  link: string;         // 原文链接
  publishedAt: number;  // 毫秒时间戳
  isAlert: boolean;     // true = critical 或 high 级别
  threat: {
    level: 'THREAT_LEVEL_CRITICAL' | 'THREAT_LEVEL_HIGH' | 'THREAT_LEVEL_MEDIUM' | 'THREAT_LEVEL_LOW' | 'THREAT_LEVEL_UNSPECIFIED';
    category: string;   // 事件分类
    confidence: number; // 0.3 ~ 0.9
    source: 'keyword' | 'llm';
  };
}
```

### 5.3 降级保护

```
Redis 摘要缓存为空且构建失败 → 使用内存 fallbackDigestCache（进程级）
内存缓存也空 → 返回空 categories: {}
```

---

## 6. LLM 摘要生成

### 6.1 支持的 LLM 提供商

| Provider | 说明 | 配置变量 |
|----------|------|----------|
| **Anthropic Claude** | 首选，质量最高 | `ANTHROPIC_API_KEY` |
| **OpenAI GPT** | 备选 | `OPENAI_API_KEY` |
| **Groq** | 速度极快，成本低 | `GROQ_API_KEY` |
| **OpenRouter** | 多模型路由 | `OPENROUTER_API_KEY` |
| **Ollama** | 本地离线推理 | `OLLAMA_API_URL` |

### 6.2 摘要参数

```
temperature: 0.3   (低随机性，保证一致性)
max_tokens: 100    (严格控制输出长度)
top_p: 0.9
超时: 25秒
```

### 6.3 三种摘要模式及 Prompt

#### `brief` 模式（地缘政治版）

**System Prompt**:
```
{Current date}. Provide geopolitical context appropriate for the current date.

Summarize the single most important headline in 2 concise sentences MAX (under 60 words total).
Rules:
- Each numbered headline below is a SEPARATE, UNRELATED story
- Pick the ONE most significant headline and summarize ONLY that story
- NEVER combine or merge people, places, or facts from different headlines into one sentence
- Lead with WHAT happened and WHERE - be specific
- NEVER start with "Breaking news", "Good evening", "Tonight", or TV-style openings
- Start directly with the subject of the chosen headline
- If intelligence context is provided, use it only if it relates to your chosen headline
- No bullet points, no meta-commentary, no elaboration beyond the core facts
```

**User Prompt**:
```
Each headline below is a separate story. Pick the most important ONE and summarize only that story:
1. [headline 1]
2. [headline 2]
...

[可选：geo_context 地理情报上下文]
```

#### `analysis` 模式（战略分析版）

**System Prompt**:
```
{Current date}. Provide geopolitical context appropriate for the current date.

Analyze the most significant development in 2 concise sentences MAX (under 60 words total). Be direct and specific.
Rules:
- Each numbered headline below is a SEPARATE, UNRELATED story
- Pick the ONE most significant story and analyze ONLY that
- NEVER combine or merge people, places, or facts from different headlines
- Lead with the insight - what's significant and why
- NEVER start with "Breaking news", "Tonight", "The key/dominant narrative is"
- Start with substance, no filler or elaboration
- If intelligence context is provided, use it only if it relates to your chosen headline
```

**User Prompt**:
```
Each headline is a separate story. What's the key pattern or risk?
1. [headline 1]
...
```

#### `translate` 模式

将摘要翻译为目标语言，保持新闻播报风格。

### 6.4 输出清洗（移除推理 token）

```typescript
// 移除各种 CoT/reasoning 标签
rawContent = rawContent
  .replace(/<think>[\s\S]*?<\/think>/gi, '')
  .replace(/<\|thinking\|>[\s\S]*?<\|\/thinking\|>/gi, '')
  .replace(/<reasoning>[\s\S]*?<\/reasoning>/gi, '')
  .replace(/<reflection>[\s\S]*?<\/reflection>/gi, '')
  .replace(/<\|begin_of_thought\|>[\s\S]*?<\|end_of_thought\|>/gi, '')
  // 移除未闭合的推理块
  .replace(/<think>[\s\S]*/gi, '')
  .trim();
```

### 6.5 输出质量过滤

以下情况拒绝输出并返回 null：
- 输出少于 20 个字符（过短无效）
- 输出以任务描述开头（推理前缀）：
  ```
  "we need to", "i need to", "let me", "i'll", "the task is",
  "summarize the top story", "here are the rules", "step 1"
  ```

### 6.6 缓存键生成

```typescript
// 缓存键基于：headlines内容 + mode + geoContext + variant + lang
const cacheKey = buildSummaryCacheKey(headlines, mode, geoContext, variant, lang);
// Redis TTL: 86400秒 = 24小时
```

---

## 7. Breaking News 预警系统

### 7.1 预警触发条件

```
威胁等级 = critical 或 high
AND
发布时间在 15 分钟以内
AND
未在去重表中出现（标题 + 来源 + 域名 三元组）
```

### 7.2 冷却机制

```
单事件冷却：30 分钟（同一事件不重复预警）
全局冷却：60 秒（防止预警风暴）
启动宽限：10 秒（忽略应用启动时已有的历史条目）
```

### 7.3 来源分级过滤

```
Tier 1-2（路透、BBC等主流媒体）：
  关键词匹配即触发预警

Tier 3+（智库、分析机构）：
  必须有 LLM 分类确认才触发，防止误报
```

### 7.4 预警来源类型

| 类型 | 说明 |
|------|------|
| `rss_alert` | RSS feed 中检测到高威胁新闻 |
| `keyword_spike` | 关键词趋势突刺 |
| `hotspot_escalation` | 地理热点升级 |
| `military_surge` | 军事活动激增 |
| `oref_siren` | 以色列红色警报（Pikud HaOref 实时数据） |

### 7.5 通知方式

- 自定义 DOM 事件：`wm:breaking-news`
- 操作系统桌面通知
- 音效提示
- localStorage 持久化去重记录

---

## 8. 缓存架构

### 8.1 缓存层次

```
L1 浏览器内存 (AppContext)          — 会话级，毫秒级读取
L2 Vercel CDN + Cloudflare        — 全球边缘，支持 ETag 条件请求
L3 Upstash Redis（核心缓存层）      — 跨实例共享
L4 进程内 fallbackDigestCache      — 降级保底，Map 结构
```

### 8.2 Redis 缓存键规范

| 缓存键 | TTL | 内容 |
|--------|-----|------|
| `rss:feed:v1:{variant}:{url}` | 3600s (1小时) | 单个 feed 解析结果 |
| `news:digest:v1:{variant}:{lang}` | 900s (15分钟) | 聚合摘要（所有分类） |
| `classify:sebuf:v1:{sha256_16bit}` | 长期 | LLM 对单条标题的分类结果 |
| `summary:{cache_key}` | 86400s (24小时) | LLM 摘要文本 |

### 8.3 防雪崩（Stampede Protection）

`cachedFetchJson` 函数使用 Promise 合并：同一缓存键的并发请求共享单次 RSS 拉取，不会产生 N 倍放大。

---

## 9. 部署与基础设施

### 9.1 核心服务

```
Vercel Edge Functions：
  - /api/news/list-feed-digest     — 聚合摘要接口
  - /api/news/summarize-article    — LLM 摘要接口
  - /api/youtube/live              — 直播流接口

Railway 中继服务：
  - /rss?url={encoded_url}         — RSS 代理（规避 IP 封锁）
  - 同时运行 seed 脚本（定期填充 Redis）

Upstash Redis：
  - 所有缓存的核心存储
```

### 9.2 刷新频率

```
RSS 摘要缓存：每 15 分钟自动刷新（惰性更新）
单 feed 缓存：每 1 小时刷新
LLM 摘要缓存：每 24 小时
Breaking News 检测：实时（前端轮询间隔约 5 分钟）
市场数据：每 1-2 分钟
```

### 9.3 ML Workers（浏览器端）

| Worker | 功能 | 模型 |
|--------|------|------|
| `analysis.worker.ts` | 新闻聚类（Jaccard 相似度）、跨域关联检测 | 规则 |
| `ml.worker.ts` | 语义嵌入、情感分析、命名实体识别（NER）、摘要 | MiniLM-L6 (ONNX) |

---

## 10. 原油/伊朗专项监控策略

### 10.1 推荐新闻源组合

针对**原油价格走势 + 美伊战争风险**研究，推荐以下优先级排序：

#### 第一优先级（必须覆盖）
```
1. OilPrice.com     → https://oilprice.com/rss/main
   (原油专业媒体，Brent/WTI分析、OPEC动态)

2. Reuters Energy   → https://news.google.com/rss/search?q=site:reuters.com (oil OR gas OR energy) when:1d
   (路透社能源报道，最权威)

3. OPEC/Crude Oil   → https://news.google.com/rss/search?q=(OPEC OR "crude oil" OR WTI OR Brent OR "oil production") when:1d
   (OPEC会议、减产/增产决策)

4. BBC Middle East  → https://feeds.bbci.co.uk/news/world/middle_east/rss.xml
   (中东局势，直接影响霍尔木兹海峡)

5. Al Jazeera       → https://www.aljazeera.com/xml/rss/all.xml
   (中东视角，伊朗/伊拉克/也门报道)
```

#### 第二优先级（强烈推荐）
```
6. Defense One      → https://www.defenseone.com/rss/all/
   (美军中东部署、打击伊朗决策)

7. The War Zone     → https://www.twz.com/feed
   (军事行动细节、F-35/B-2/航母动态)

8. USNI News        → https://news.usni.org/feed
   (美国海军，波斯湾舰队动态)

9. gCaptain         → https://gcaptain.com/feed/
   (霍尔木兹海峡油轮、航运、扣押事件)

10. Rigzone          → https://www.rigzone.com/news/rss/rigzone_latest.aspx
    (石油行业生产、钻探、管道)
```

#### 第三优先级（深度分析）
```
11. Foreign Policy   → https://foreignpolicy.com/feed/
    (战略分析，制裁效果、外交走向)

12. Atlantic Council → https://www.atlanticcouncil.org/feed/
    (美伊关系智库报告)

13. War on the Rocks → https://warontherocks.com/feed/
    (军事战略深度分析)

14. Bellingcat       → https://news.google.com/rss/search?q=site:bellingcat.com
    (伊朗导弹/无人机OSINT验证)

15. EIA Reports      → https://news.google.com/rss/search?q=site:eia.gov energy oil gas when:14d
    (美国能源信息署，库存/产量数据)
```

#### 补充源（伊朗视角）
```
16. BBC Persian      → https://feeds.bbci.co.uk/persian/rss.xml  (lang: fa)
    (伊朗国内舆论，需翻译)

17. Iran International → https://news.google.com/rss/search?q=site:iranintl.com when:3d
    (反伊朗政府英语媒体)

18. Arms Control Assn → https://news.google.com/rss/search?q=site:armscontrol.org
    (核协议谈判进展，JCPOA)

19. Bulletin of Atomic Scientists → https://news.google.com/rss/search?q=site:thebulletin.org
    (伊朗核武进展评估)
```

### 10.2 关键词监控策略

对原油/美伊冲突，以下关键词命中即为高优先级事件：

**CRITICAL 触发**（立即预警）：
```
invasion, nuclear attack, nuclear strike, coup, mass casualty,
nato article 5, chemical attack, biological attack, dirty bomb
```

**HIGH 触发**（重要预警）：
```
airstrike, air strike, drone strike, missile, bombing, casualties,
sanctions, embargo, war, armed conflict, assassination
```

**原油专项关键词**（需额外监控）：
```
"Strait of Hormuz"      霍尔木兹海峡封锁
"oil tanker"            油轮被扣/袭击
"OPEC cut"              减产决定
"pipeline explosion"    管道爆炸（基础设施）
"refinery attack"       炼油厂袭击
"oil embargo"           石油禁运
"crude supply"          原油供应中断
"Iran nuclear"          伊朗核计划
"Houthi attack"         胡塞武装袭击（红海/亚丁湾）
"Bab el-Mandeb"         曼德海峡（红海咽喉）
```

### 10.3 油价影响事件分级

基于 worldmonitor 的分类逻辑，以下事件对油价影响最大：

| 事件类型 | 威胁等级 | 预期油价影响 |
|----------|----------|-------------|
| 美伊开战（入侵/空袭） | CRITICAL | +15%~+40% |
| 霍尔木兹海峡封锁 | CRITICAL | +20%~+50% |
| 伊朗核设施被打击 | HIGH | +10%~+25% |
| OPEC 紧急减产 | HIGH | +5%~+15% |
| 油轮被扣押 | HIGH | +3%~+8% |
| 美国对伊新制裁 | MEDIUM-HIGH | +2%~+6% |
| 也门胡塞袭击沙特设施 | HIGH | +5%~+12% |
| 伊拉克示威/管道关闭 | MEDIUM | +1%~+4% |
| OPEC 部长级会议 | LOW-MEDIUM | ±3% |

### 10.4 地理热点监控区域

这些地区的新闻对原油走势影响最直接：

```
波斯湾（Persian Gulf）    → 霍尔木兹海峡，全球20%石油过境
红海（Red Sea）           → 苏伊士运河，欧亚石油航线
曼德海峡（Bab el-Mandeb） → 也门胡塞武装控制区
伊拉克巴士拉（Basra）    → 伊拉克主要出口港
沙特阿拉米科（Aramco）   → 全球最大石油公司设施
库尔德斯坦（Kurdistan）  → 柯克-杰伊汉管道起点
利比亚（Libya）          → 北非最大产油国，政治不稳定
尼日利亚三角洲（Niger Delta）→ 西非石油产区
```

---

## 11. 关键文件索引

| 文件路径 | 行数 | 核心功能 |
|----------|------|----------|
| `server/worldmonitor/news/v1/_feeds.ts` | 437 | **所有 RSS URL 定义**（含 INTEL_SOURCES） |
| `server/worldmonitor/news/v1/list-feed-digest.ts` | 380 | **RSS 抓取、解析、分类、聚合主流程** |
| `server/worldmonitor/news/v1/_classifier.ts` | 244 | **关键词分类规则（完整词表）** |
| `server/worldmonitor/news/v1/_shared.ts` | 131 | **LLM Prompt 构建（brief/analysis/translate）** |
| `server/worldmonitor/news/v1/summarize-article.ts` | 224 | **多 Provider LLM 摘要，含输出清洗** |
| `src/services/breaking-news-alerts.ts` | 256 | **预警触发、去重、冷却逻辑** |
| `src/config/feeds.ts` | ~1369 | 前端 feed 元数据（tier、分类、描述） |
| `src/config/pipelines.ts` | ~1036 | 全球油气管道数据库 |
| `src/workers/ml.worker.ts` | — | 浏览器端 ML（嵌入、NER、情感） |
| `src/workers/analysis.worker.ts` | — | 新闻聚类、跨域关联 |
| `server/_shared/redis.ts` | — | Redis 缓存工具（防雪崩） |
| `server/_shared/relay.ts` | — | Railway 中继配置 |
| `server/_shared/llm.ts` | — | Provider 凭证管理 |

---

## 附录：Google News RSS 参数速查

```
基础格式：
https://news.google.com/rss/search?q={query}&hl=en-US&gl=US&ceid=US:en

时间过滤：
when:1d    过去24小时
when:2d    过去48小时
when:3d    过去3天
when:7d    过去7天
when:14d   过去14天

站点过滤：
site:reuters.com         只看路透社
site:reuters.com energy  路透社能源

布尔运算：
(oil OR gas OR LNG)      多关键词OR
"Strait of Hormuz"       精确短语匹配
(OPEC OR WTI OR Brent) when:1d  组合使用

实际例子：
# 原油/美伊核心监控
https://news.google.com/rss/search?q=(Iran+OR+"Persian+Gulf"+OR+Hormuz+OR+IRGC)+when:1d&hl=en-US&gl=US&ceid=US:en

# OPEC动态
https://news.google.com/rss/search?q=(OPEC+OR+"crude+oil"+OR+WTI+OR+Brent+OR+"oil+production")+when:1d&hl=en-US&gl=US&ceid=US:en

# 中东军事
https://news.google.com/rss/search?q=(airstrike+OR+missile+OR+"drone+strike")+%22Middle+East%22+when:1d&hl=en-US&gl=US&ceid=US:en
```

---

*本文档基于 worldmonitor 项目代码（截至 2026-03-26）整理，覆盖 `_feeds.ts`、`_classifier.ts`、`list-feed-digest.ts`、`_shared.ts`、`summarize-article.ts` 等核心文件。*
