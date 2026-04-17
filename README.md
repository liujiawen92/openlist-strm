# OpenList-strm

alist-strm 管理界面的独立分支，不依赖原版更新源。

## 主要特性

- 🚫 **无自动更新**：禁用崩溃自动更新，容器启动即稳定
- ⚡ **直接运行**：零配置，开箱即用
- 🔧 **独立部署**：不从任何外部源自动更新
- 🖥️ **深色主题 UI**：Bootstrap 5 暗色管理界面
- ⏰ **定时任务**：支持系统 cron + APScheduler 双模式

## 快速开始

### Docker 部署

```bash
docker run -d \
  --name openlist-strm \
  -p 5246:5246 \
  -v $(pwd)/data:/app/data \
  -v $(pwd)/logs:/app/logs \
  -v $(pwd)/video:/volume1/docker/alist-strm/video \
  -e WEB_PORT=5246 \
  -e SECURITY_CODE=your_code \
  liujiawen92/openlist-strm
```

### 本地开发

```bash
pip install -r requirements.txt
python app.py
```

## 目录结构

```
openlist-strm/
├── app.py              # Flask 主应用
├── main.py             # WebDAV 同步脚本
├── db_handler.py       # SQLite 数据库封装
├── logger.py           # 日志模块
├── task_scheduler.py   # 定时任务调度
├── templates/          # Jinja2 模板
├── static/             # CSS/JS/图片
└── Dockerfile
```

## 环境变量

| 变量 | 默认值 | 说明 |
|------|--------|------|
| WEB_PORT | 5246 | Web 服务端口 |
| SECURITY_CODE | 自动生成 | 安全码 |
| SECRET_KEY | 自动生成 | Flask 密钥 |
| APP_VERSION | 6.0.9-fixed | 应用版本号 |

## 许可证

MIT
