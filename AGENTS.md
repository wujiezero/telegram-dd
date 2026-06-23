# AGENTS.md

## Cursor Cloud specific instructions

本仓库是一个 **Telegram 下载守护进程**（不是机器人）：一个 asyncio + Telethon 客户端，
外加一个 Flask + Socket.IO 的 Web 监控/管理界面（端口 `7373`）。纯逻辑工具函数抽离到
`tdd_utils.py` / `sessionManager.py`，便于在不连 Telegram 的情况下做单元测试。

### 环境与依赖
- Python 依赖装在仓库根目录的虚拟环境 `.venv`（由启动更新脚本创建）。统一用 `.venv/bin/python`
  / `.venv/bin/pytest` 调用，不要用系统 `python3` 直接跑（系统环境没有装依赖）。
- 创建 venv 依赖系统包 `python3.12-venv`（环境里已预装）。
- 运行测试还需要 `pytest`，它**不在** `requirements.txt` 里，更新脚本会单独装。

### 测试 / 语法检查
- 单元测试：`.venv/bin/python -m pytest tests/ -v`（46 个用例，约 0.2s）。也可用 `./tdd.sh test`
  或纯标准库 `.venv/bin/python -m unittest discover -s tests -v`。
- 本项目**没有配置任何 linter**（无 flake8/ruff/pylint 配置）。需要做静态检查时用
  `.venv/bin/python -m py_compile telegram-download-daemon.py tdd_utils.py sessionManager.py fast_download.py`。

### 运行应用（本地直跑，非 Docker）
- 本云环境**没有安装 Docker**，所以 README/`tdd.sh` 里的 `docker compose` 流程在这里跑不通。
  请直接运行：`.venv/bin/python telegram-download-daemon.py`。
- 必需的环境变量（否则构造 Telethon 客户端会失败）：`TELEGRAM_DAEMON_API_ID`、
  `TELEGRAM_DAEMON_API_HASH`、`TELEGRAM_DAEMON_CHANNEL`。建议同时设置本地路径，避免写到
  容器默认的 `/downloads` `/session`：`TELEGRAM_DAEMON_DEST`、`TELEGRAM_DAEMON_SESSION_PATH`、
  `TELEGRAM_DAEMON_LOG_DIR`。完整变量见 `.env.example` 与 README。
- **非常重要的启动顺序坑**：守护进程会**先** `client.connect()` 连上 Telegram 数据中心，
  **之后**才启动 Web 服务器。所以如果到 Telegram DC 的网络不通，连 Web UI（含 `/healthz`）都起不来。
  本环境实测可直连 Telegram DC（`149.154.167.x:443`）。
- 用 **假的** `api_id/api_hash` 也能把服务跑起来：Web 仪表盘会渲染、`/healthz` 返回 200、各
  `/api/*` 数据接口可用；但登录流程在点“发送验证码”时会返回
  `The api_id/api_hash combination is invalid`（这恰好证明前端→Flask→Telethon→Telegram 全链路是通的）。
- **真正下载文件**需要：真实的 Telegram api_id/api_hash（来自 https://my.telegram.org）+ 手机号
  网页登录 + 一个你有权限的频道 ID。这些属于用户私密凭据，应作为 Secrets 提供。

### 健康检查 / 冒烟
- `curl http://127.0.0.1:7373/healthz` → `{"ok":true,...}`
- `curl http://127.0.0.1:7373/api/status`、`/api/history`、`/api/tasks` 返回实时数据（无 Telegram 登录时为空）。
- 若设置了 `TELEGRAM_DAEMON_WEB_TOKEN`，访问 Web 页面/接口需要带该 token；不设则为开放访问。
