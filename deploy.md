  部署步骤（腾讯云 Ubuntu 22.04）

  第一步：服务器初始准备

  # SSH 登入服务器
  ssh ubuntu@你的公网IP

  # 更新系统
  sudo apt update && sudo apt upgrade -y

  # 安装基础依赖
  sudo apt install -y git nginx python3.11
  python3.11-venv python3-pip \
                      nodejs npm build-essential        

  # 验证版本
  python3.11 --version   # 需要 3.10+
  node --version         # 需要 18+
  npm --version

  第二步：添加 Swap（2GB 内存必做，防止 akshare
  撑爆内存）

  sudo fallocate -l 2G /swapfile
  sudo chmod 600 /swapfile
  sudo mkswap /swapfile
  sudo swapon /swapfile
  # 开机自动挂载
  echo '/swapfile none swap sw 0 0' | sudo tee -a       
  /etc/fstab

  第三步：拉取代码

  # 建议放在 /opt/fundata
  sudo mkdir -p /opt/fundata
  sudo chown ubuntu:ubuntu /opt/fundata

  cd /opt/fundata
  git clone https://github.com/你的用户名/你的仓库.git .
  # 或者用 scp / rsync 直接传文件

  第四步：配置 Python 环境

  cd /opt/fundata
  python3.11 -m venv venv
  source venv/bin/activate
  pip install --upgrade pip
  pip install -r requirements.txt
  # curl_cffi 可能需要多等一会儿

  第五步：配置 .env

  cp .env.example .env
  nano .env  # 填入真实邮箱账号和授权码

  .env 内容示例：
  DB_PATH=fund_data.db
  CLEAN_DB_PATH=fund_clean.db
  EMAIL_USER=xxx@163.com
  EMAIL_PASSWORD=你的IMAP授权码
  API_HOST=0.0.0.0
  API_PORT=8000
  MARKET_INTRADAY_MODE=0

  第六步：构建前端

  cd /opt/fundata/web
  npm install
  npm run build
  # 产物在 web/dist/
  ls dist/   # 确认有 index.html

  第七步：配置 nginx

  # 复制配置文件
  sudo cp /opt/fundata/deploy/nginx-site.conf
  /etc/nginx/sites-available/fundata

  # 编辑 server_name（填你的公网IP或域名）
  sudo nano /etc/nginx/sites-available/fundata
  # 把 server_name _; 改成 server_name 你的IP;

  # 启用站点，删掉默认站点
  sudo ln -s /etc/nginx/sites-available/fundata
  /etc/nginx/sites-enabled/
  sudo rm -f /etc/nginx/sites-enabled/default

  # 验证配置并重载
  sudo nginx -t
  sudo systemctl reload nginx

  第八步：配置 systemd 服务

  # 复制服务文件
  sudo cp /opt/fundata/deploy/fundata-api.service       
  /etc/systemd/system/

  # 重载 systemd，启动并设置开机自启
  sudo systemctl daemon-reload
  sudo systemctl enable fundata-api
  sudo systemctl start fundata-api

  # 查看状态
  sudo systemctl status fundata-api
  # 查看实时日志
  sudo journalctl -u fundata-api -f

  第九步：初次拉取数据

  cd /opt/fundata
  source venv/bin/activate

  # 拉取基金邮件数据（首次全量，需要几分钟）
  python get_163_email.py

  # 拉取 A 股行情（首次约 8 年历史，需要 5-10 分钟）    
  python get_market_data.py

  # 拉取原油数据
  python get_crude_data.py

  第十步：验证

  浏览器访问
  http://你的公网IP，正常显示基金列表即部署成功。       

  API 文档：http://你的公网IP/docs

  ---
  后续维护

  更新代码：
  cd /opt/fundata
  git pull
  # 如有新依赖
  source venv/bin/activate && pip install -r
  requirements.txt
  # 如有前端改动
  cd web && npm run build
  # 重启后端
  sudo systemctl restart fundata-api

  查看日志：
  sudo journalctl -u fundata-api -n 100 --no-pager      

  手动触发同步（代替页面按钮）：
  cd /opt/fundata && source venv/bin/activate
  python get_163_email.py

  ---
  注意事项

  ┌──────────┬──────────────────────────────────────┐   
  │  风险点  │                 说明                 │   
  ├──────────┼──────────────────────────────────────┤   
  │ 内存     │ akshare 同步时峰值可达 600MB+，已加  │   
  │          │ 2GB Swap 兜底                        │   
  ├──────────┼──────────────────────────────────────┤   
  │ 带宽 3M  │ nginx 已开启 gzip，React 静态文件约  │   
  │          │ 300KB（压缩后）                      │   
  ├──────────┼──────────────────────────────────────┤   
  │ 代理     │ 服务器在国内无需设代理，腾讯QQ财经/  │   
  │          │ 新浪/CFFEX 均可直连                  │   
  ├──────────┼──────────────────────────────────────┤   
  │          │ api.py 内置 APScheduler，自动在      │   
  │ 定时任务 │ 12:00/18:00 同步邮件、11:30/15:15    │   
  │          │ 同步行情                             │   
  ├──────────┼──────────────────────────────────────┤   
  │ 数据库备 │ fund_data.db 不在 git 里，建议定期   │   
  │ 份       │ scp 备回本地                         │   
  ├──────────┼──────────────────────────────────────┤   
  │ 防火墙   │ 腾讯云安全组只开放 80 端口（和 22    │   
  │          │ 端口 SSH），8000 端口不需要对外暴露  │   
  └──────────┴──────────────────────────────────────┘