# 腾讯云控制台发布与回滚

本工具只在操作员明确选择的服务器项目目录内构建前端、保存记录和备份数据库。它不会上传或覆盖为本地电脑生成的数据库，不会创建或修改 systemd/nginx 配置。公网 `http://150.158.79.213/` 本轮必须保持匿名只读。

## 先核实实际安装情况

在腾讯云控制台打开服务器终端，先查看现有服务，而不是直接假设安装位置为 `/opt/fundata`：

```bash
systemctl show fundata-api.service --property=WorkingDirectory,ExecStart,EnvironmentFiles
```

确认实际项目根目录、运行服务名、虚拟环境位置和 `.env` 中的 `DB_PATH`。不要把 `.env` 或邮件授权码粘贴到聊天、GitHub、发布日志中。下文的 `PROJECT`、`DB`、`SERVICE`、`SHA` 都要替换为核实后的实际值；`SHA` 必须是已验收、已推送的完整 40 位提交号。

项目 `.env` 必须明确包含以下配置，保留既有邮件凭据：

```dotenv
FUNDATA_READONLY=1
FUNDATA_COOKIE_SECURE=1
FUNDATA_SCHEDULER_ENABLED=1
```

服务必须只加载该项目的 `.env`，工作目录必须等于项目目录，显式使用 `--host 127.0.0.1 --workers 1`。nginx 继续代理 `/api/` 并提供该目录下 `web/dist/` 静态资源。没有 HTTPS 时不要在公网启用管理员登录。

`deploy/nginx-site.conf` 提供本轮 HTTP 只读模板：先替换模板中的静态目录为已核实路径，保留既有必要配置，再由维护者安装到 nginx 的实际配置位置、运行 `nginx -t` 后重载。该门禁拒绝写方法与管理/邮件详情路径，应独立于应用版本保留。发布脚本不会擅自修改 nginx。

## 获取代码与只读预检

在项目目录执行 `git status`，先保留服务器上已有的修改。工具遇到未提交或未跟踪文件会停止；不要通过删除数据库或强制重置来绕过检查。数据库、备份、构建产物必须被忽略。

```bash
git fetch origin
python deploy/manage_release.py preflight --project PROJECT --db DB --service SERVICE --commit SHA
```

首次安装本工具时，如果当前旧版本没有 `deploy/manage_release.py`，先将本次已审核的单个脚本放在项目内被忽略的 `backups/manage_release.py`，从该路径执行它；不要为了获取工具提前切换正在运行的旧版本。脚本源应对应待发布的固定提交，可用 `git show SHA:deploy/manage_release.py` 核对。后续版本直接使用仓库中的工具。

预检只读取信息，输出当前/目标提交、项目与数据库位置、服务状态、单 worker 和只读配置检查、数据库完整性、API 健康状态；不输出凭据。目标提交须已存在于服务器 Git 对象库中。需要按运维配置授予操作员停止/启动该服务的权限；不要无目的地以 root 身份运行依赖安装。

## 固定版本发布

```bash
python deploy/manage_release.py release --project PROJECT --db DB --service SERVICE --commit SHA --python venv/bin/python
```

工具按顺序停止服务、确认已停止、用 SQLite backup API 备份服务器当前数据库、记录旧提交与备份指纹、切换目标提交、安装依赖并构建前端、在副本上验证迁移后迁移原库、启动服务并检查健康。Python/npm 缓存和前端构建均留在项目中。备份和发布记录位于 `PROJECT/backups/`，保留脚本打印的准确 `release-*.json` 路径。

中途失败会清楚报错；停服后发生错误时，不会盲目继续启动不完整版本。用下面的发布记录回滚。单独审查迁移可使用 `migrate` 子命令：它要求当前提交等于 `--commit`，备份并迁移后保留停服状态。

## 邮件补抓与公网验收

发布健康后，由同一个服务账户在项目根目录执行邮件增量同步（读取服务器自己的 `.env`）：

```bash
venv/bin/python get_163_email.py --db DB
venv/bin/python organize_fund_data.py --db DB
```

同步任务互斥；如果定时任务正在运行，等待该次任务完成再执行。检查 JSON 结果及 `sync_runs`：`success` 表示完整成功，`partial_success` 表示已有可定位的附件失败记录，`error` 表示需要处理的错误。不能把“零新增”单独当成失败或成功依据。

有定位信息的失败可通过 `--retry-failure ID` 重试。历史记录缺少 UID/UIDVALIDITY 时自动重试不可用，需人工找到原邮件。不要直接重置 `last_uid` 来掩盖失败。

打开公网网站，核对新版界面、邮件来源筛选、有效净值截止日期、基金详情、对比、组合只读展示及市场/基差页面。匿名写请求必须被后端拒绝；页面隐藏按钮不构成权限验证。检查 `/api/health` 与 `/api/sync/status`，并核对一只本次新增净值的基金。公网没有确认前，交付状态只能写“GitHub 已上传/等待服务器发布”，不能写“已上线”。

## 按准确发布记录回滚

`OLD_SHA` 使用发布记录里的 `previous_commit`，`MANIFEST` 使用同一次发布记录的准确绝对路径：

```bash
python deploy/manage_release.py rollback --project PROJECT --db DB --service SERVICE --commit OLD_SHA --manifest MANIFEST --python venv/bin/python
```

回滚会核对项目、数据库、服务、旧提交和备份 SHA-256；停止并确认服务停止；另存当前数据库为 `before-rollback-*.db`；切回旧提交、重建前端，再通过 SQLite backup API 恢复那一份明确的发布前备份，最后启动和健康检查。回滚后的新增邮件数据仍保存在 `before-rollback` 备份中，可以审计与补抓，不会静默丢失。

操作员仍需确认没有其他进程或临时采集命令在写同一数据库。`systemctl stop` 只能停止所指定的 API 服务。备份含真实基金和邮件来源数据，按服务器访问权限保管，不提交 GitHub。
# 旧版本回滚安全边界

工具拒绝自动发布或回滚到没有管理员权限保护的旧提交。旧 API 不识别 `FUNDATA_READONLY`，只恢复环境变量无法保证只读。若必须手动恢复更早版本，先由维护者配置独立于 Git 工作区的 nginx 只读门禁（拒绝写方法和邮件/管理详情），验证后才能恢复外网服务。工具在新版启动后也会实际检查匿名净值写入、登录及邮件详情均返回 403；检查失败会停止服务。
