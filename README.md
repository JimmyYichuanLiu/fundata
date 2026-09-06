# FundTrack 基金净值看板

从 163 邮箱附件提取基金净值，写入 SQLite，由 FastAPI 提供数据接口，React/Vite 展示基金、对比、组合、市场和基差。保留独立邮件 Excel 导出和 ZX 数据导入工具。

本轮公网地址为 http://150.158.79.213/ ，采用匿名只读模式。GitHub 上传不等于服务器已部署，服务器由维护者通过腾讯云控制台执行发布和验收。原油、新闻、航运功能继续停用。

## 先了解项目

| 文件或目录 | 职责 |
|---|---|
| `get_163_email.py`、`smart_extractor.py` | 邮件附件及多工作表净值提取 |
| `sync_service.py` | 命令行、网页和定时任务共用的增量同步、运行记录、失败重试 |
| `fund_store.py` | 版本化迁移、备份、日期规范化、异常隔离和复权净值 |
| `organize_fund_data.py` | 邮件净值导出为“汇总 + 每基金一页”的 Excel |
| `api.py`、`admin_auth.py` | 查询、组合计算、管理员会话及只读权限 |
| `web/src/` | 浅色/深色响应式研究看板 |
| `zx_importer.py`、`db_merger.py` | 原有 ZX 导入及合并工具 |
| `get_market_data.py` | 指数、期货及基差所需行情 |
| `deploy/manage_release.py` | 服务器预检、固定版本发布及回滚 |

数据路径：邮箱 → 附件/工作表提取 → 质量校验与 SQLite → 有效净值视图 → API → 看板或 Excel。错误原始记录和冲突证据保留，但不进入正常图表和统计。

## 本地运行

以下命令均从 `D:\coding\fundata` 执行。不要覆盖已有 `.env`。

```powershell
python -m pip install -r requirements.txt
npm --prefix web ci
```

首次配置参考 `.env.example`。邮箱密码使用 163 的 **IMAP 授权码**，不是邮箱登录密码，不要提交到 GitHub。

```powershell
python -m uvicorn api:app --host 127.0.0.1 --port 8000 --workers 1 --no-proxy-headers
```

另开终端：

```powershell
npm --prefix web run dev -- --host 127.0.0.1
```

访问 http://127.0.0.1:5173 。首次 API 启动使用共享迁移入口，现库先备份到数据库旁的 `backups/`，在副本验证后迁移。只允许一个后端 worker；开发预览可设置 `FUNDATA_SCHEDULER_ENABLED=0`，避免预览时触发定时采集。

## 邮件同步与 Excel：如何使用

先确认 `.env` 中的 `EMAIL_USER`、`EMAIL_PASSWORD`、`DB_PATH` 正确：

```powershell
python get_163_email.py --db fund_data.db
python organize_fund_data.py --db fund_data.db --output fund_email_nav.xlsx
```

- 第一条命令处理新增邮件；中断后从已提交的邮件检查点继续。
- 第二条只导出来源为 `email` 的有效记录，不会把 ZX 数据误算成邮件数据。
- Excel 包含汇总表及每只基金的日期、单位净值、累计净值；文件名可自行指定。
- 若只需要现有数据的 Excel，直接执行第二条，不必重新连接邮箱。
- 导出全部来源：`python organize_fund_data.py --source all --output fund_data_organized.xlsx`。
- 连接、登录或读取失败会返回失败；附件部分失败标记“部分成功”，不会冒充完整成功。
- 同日相同数值为重复；同日不同数值保留冲突审计，不静默覆盖。
- 重试可定位的失败：`python get_163_email.py --db fund_data.db --retry-failure 失败记录ID`。
- 历史记录若没有 UID/UIDVALIDITY，不能自动定位原邮件，需要取得原附件后核查，不会把日志当作已恢复。

后台仍按北京时间每天 12:00、18:00 尝试邮件同步。“最后尝试”“最后成功”“最新邮件净值日期”和“下次运行”是不同概念；没有新邮件也可以是一轮真实成功。

## 本地管理员

默认 `FUNDATA_READONLY=1`：不开放登录、写操作、邮件详情和导出接口，后台定时采集仍可运行。

在**仅绑定本机且不经过反向代理**的开发环境中，可设置：

```powershell
$env:FUNDATA_READONLY = "0"
$env:FUNDATA_COOKIE_SECURE = "0"
$env:FUNDATA_SCHEDULER_ENABLED = "0"
python admin_auth.py --db fund_data.db --username owner
python -m uvicorn api:app --host 127.0.0.1 --port 8000 --workers 1 --no-proxy-headers
```

管理员密码在终端隐藏输入，至少 12 字符。网页登录后可同步、查看/重试失败、导出 Excel、管理净值与组合。会话采用服务端存储、HttpOnly Cookie、CSRF 校验；改密码会撤销旧会话。

**上述不安全 Cookie 选项只限本机。公网 HTTP 不登录、不传管理员凭据。** 未来启用公网管理员必须先完成 HTTPS，并设置精确的 `FUNDATA_ALLOWED_ORIGINS`。

## 看板入口

- `/`：有效基金与净值日期、同步状态、名称/代码搜索、策略/来源筛选、排序、列选择和分页。
- `/fund/:id`：单位/累计/复权净值、日期范围、基准、回撤和指标。
- `/compare`：最多八基金，选择写入 URL，刷新和分享保留；旧 `/compare/v2` 跳转兼容。
- `/portfolios`：组合列表；管理员可构建并计算组合。
- `/market`、`/basis`：行情与基差。
- `/admin`：本地管理员数据工作区；公网只读模式不开放。

复权数据缺失会给出原因，不能用单位净值逐点填补。基金净值或组合配置变化后旧缓存保留，但展示要求重新计算。详见 [数据与指标口径](docs/DATA_CONVENTIONS.md)。

## 测试、发布与回滚

```powershell
python -m pytest tests/test_admin_auth.py tests/test_upgrade_api.py tests/test_api_fund_endpoints.py tests/test_portfolio_api.py tests/test_fund_store.py tests/test_sync_service.py tests/test_email_ingestion_validation.py tests/test_organize_fund_data.py deploy/test_manage_release.py -q -p no:cacheprovider --basetemp .test-artifacts/pytest
npm --prefix web test
npm --prefix web run build
```

先创建 `.test-artifacts` 目录。旧数据库快照测试只在本地存在相应数据文件时运行；全新环境使用合成数据回归。

服务器步骤见 [腾讯云发布与回滚](docs/DEPLOYMENT.md)，本轮测试证据见 [验收记录](docs/ACCEPTANCE.md)。必须使用服务器自身的数据库备份、迁移与邮件增量补抓，不能直接覆盖本地数据库。部署后需要核对新前端、有效日期、真实同步和匿名写操作拒绝，才能宣布上线。

数据库、附件、备份、实际 Excel、测试产物和凭据不提交 GitHub。原有 ZX 工具仍保留；执行历史合并流程前先备份，并在共享迁移/质量校验后检查来源及冲突。
