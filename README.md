# telegram-dd

此项目致敬🫡 https://github.com/alfem/telegram-download-daemon

在前辈的基础上：
```
1. 增加了代理的配置。
2. 优化了一些细节。
3. 增加了一个简单的页面用于查看下载记录，在7373端口。
4. 增加了下载文件按规则归类到不同路径。
5. 支持直接发送 Telegram 消息链接（含私密频道 t.me/c/... 链接）自动下载。
```
具体的归类规则如下：
```
IGNORE: part, desktop
Music: mp3, aac, flac, ogg, wma, m4a, aiff, wav, amr
Videos: flv, ogv, avi, mp4, mpg, mpeg, 3gp, mkv, ts, webm, vob, wmv, srt
Pictures: png, jpeg, gif, jpg, bmp, svg, webp, psd, tiff
Archives: rar, zip, 7z, gz, bz2, tar, tgz, xz, iso, cpio
Documents: txt, pdf, doc, docx, odf, xls, xlsv, xlsx, ppt, pptx, ppsx, odp, odt, ods, md, json, csv
Books: mobi, epub, chm
DEBPackages: deb
Programs: exe, msi
RPMPackages: rpm
Mac: dmg, pkg
Linux: sh, rpm, deb
Android: apk
```

一个用于自动化文件下载的 Telegram 守护进程（不是机器人），[适用于您拥有管理员权限的频道]。

如果您有一台联网的电脑或 NAS，并且想要自动化从 Telegram 频道下载文件，这个守护进程非常适合您。

允许下载的最大大小受 Telegram API 限制为 2GB。

# 安装
您需要 Python3（3.6+）。

通过运行以下命令安装依赖：

    pip install -r requirements.txt

（如果您不想安装 `cryptg` 及其依赖，您只需要安装 `telethon`）

警告：如果您收到 "File size too large message" 错误，请检查您使用的 Telethon 库版本。旧版本有 1.5GB 的文件大小限制。

获取您自己的 api id：https://core.telegram.org/api/obtaining_api_id

# 使用

您需要配置以下值：

| 环境变量                 | 命令行参数       | 描述                                                  | 默认值                |
|--------------------------|:-----------------:|-------------------------------------------------------|-----------------------|
| `TELEGRAM_DAEMON_API_ID`   | `--api-id`        | 从 https://core.telegram.org/api/obtaining_api_id 获取的 api_id |                       |
| `TELEGRAM_DAEMON_API_HASH` | `--api-hash`      | 从 https://core.telegram.org/api/obtaining_api_id 获取的 api_hash |                       |
| `TELEGRAM_DAEMON_DEST`     | `--dest`          | 下载文件的目标路径                                    | `/telegram-downloads` |
| `TELEGRAM_DAEMON_TEMP`     | `--temp`          | 临时文件（下载中）的目标路径                           | 使用 --dest 的值      |
| `TELEGRAM_DAEMON_CHANNEL`  | `--channel`       | 要从中下载的频道 ID |                       |
| `TELEGRAM_DAEMON_DUPLICATES`  | `--duplicates`       | 如何处理重复文件：忽略、覆盖或重命名                  | rename                |
| `TELEGRAM_DAEMON_WORKERS`  | `--workers`       | 同时下载的数量                                        | 等于处理器核心数       |
| `TELEGRAM_DAEMON_PROXY_HOST` | `--proxy-host`    | 代理服务器主机地址                                    |                       |
| `TELEGRAM_DAEMON_PROXY_PORT` | `--proxy-port`    | 代理服务器端口                                        |                       |
| `TELEGRAM_DAEMON_PROXY_TYPE` | `--proxy-type`    | 代理类型（socks5, http, mtproxy）                      | socks5                |
| `TELEGRAM_DAEMON_PROXY_USERNAME` | `--proxy-username` | 代理服务器用户名（如果需要认证）                    |                       |
| `TELEGRAM_DAEMON_PROXY_PASSWORD` | `--proxy-password` | 代理服务器密码（如果需要认证）                    |                       |
| `TELEGRAM_DAEMON_PROXY_RESOLVE_ONCE` | `--proxy-resolve-once` / `--no-proxy-resolve-once` | 启动时只解析一次代理域名并固定本次进程使用的代理 IP，适合 DNS 负载均衡代理 | 0 |
| `TELEGRAM_DAEMON_LOCK_FILE` |  | 单实例锁文件路径；未设置时优先跟 session 放在同一目录，否则默认 `/tmp/DownloadDaemon.lock` | 自动推导 |
| `TELEGRAM_DAEMON_LINK_DOWNLOAD` | `--link-download` / `--no-link-download` | 监听频道里出现的 Telegram 消息链接并下载对应媒体 | 1（开启） |
| `TELEGRAM_DAEMON_LINK_ALBUM` | `--link-album` / `--no-link-album` | 链接指向相册中的一条时，整组一起下载 | 1（开启） |
| `TELEGRAM_DAEMON_LINK_AUTO_JOIN` | `--link-auto-join` | 遇到未加入的 `t.me/+xxx` 邀请链接时自动加入该频道 | 0（关闭） |
| `TELEGRAM_DAEMON_LINK_MAX_MESSAGES` | `--link-max-messages` | 单条消息里的链接合计最多展开多少条 Telegram 消息 | 50 |

如果在 Docker 里看到 `Permission denied: '/session/DownloadDaemon.lock'`，说明挂载到 `/session` 的宿主目录对容器当前用户不可写。当前版本会自动把锁文件降级到 `/tmp/DownloadDaemon.lock`，服务仍可启动；如果您希望显式指定，也可以设置 `TELEGRAM_DAEMON_LOCK_FILE=/tmp/DownloadDaemon.lock`。注意：这样做后，锁文件不再跟随共享的 session 目录，多容器共用同一份 session 时请务必保证只启动一个实例。

您可以将它们定义为环境变量，或者作为命令行参数，例如：

    python telegram-download-daemon.py --api-id <your-id> --api-hash <your-hash> --channel <channel-number>

使用代理的示例：

    python telegram-download-daemon.py --api-id <your-id> --api-hash <your-hash> --channel <channel-number> --proxy-host <proxy-host> --proxy-port <proxy-port> --proxy-type socks5 --proxy-username <username> --proxy-password <password>

如果您遇到 `AUTH_KEY_DUPLICATED` 或类似“同一个 session 出现在多个 IP” 的错误，优先检查下面几项：

1. 确保只有一个守护进程实例在使用这份 session。
2. 尽量使用固定出口 IP 的代理，避免代理供应商在后台切换出口。
3. 如果您的代理是域名接入并且后端做 DNS 负载均衡，可以加上 `--proxy-resolve-once`，或者设置 `TELEGRAM_DAEMON_PROXY_RESOLVE_ONCE=1`，让守护进程启动后固定到本次解析出的单个代理 IP。
4. 当前版本会在启动时创建单实例锁；如果您确实需要多开，请务必给每个实例使用不同的 session 和不同的锁文件路径。

注意：如果代理服务商在同一个接入点后面仍然会切换真实出口 IP，那么这类问题无法完全靠代码规避，仍然需要更稳定的代理线路或独立出口。

如果守护进程已经碰到 `AUTH_KEY_DUPLICATED`，当前版本会自动归档旧 session 文件并继续保留 Web 页面，页面会提示您重新登录，而不是直接退出整个服务。

最后，将任何文件链接重新发送到频道即可开始下载。这个守护进程可以同时管理多个下载。

您还可以使用 Telegram 客户端与这个守护进程 "对话"：

* 发送 "list" 获取目标路径中可用文件的列表。
* 发送 "status" 检查当前状态。
* 发送 "clean" 从临时目录中删除过期的 (*.tdd) 文件。
* 发送 "queue" 列出等待开始的待处理文件。

# 私密频道链接下载

除了把文件本身转发进被监听频道，现在也可以**直接把一条 Telegram 消息链接丢进频道**，守护进程会用当前登录的账号把链接指向的那条消息抓回来，再把里面的视频 / 文件按原有流程下载、归类、写库、在 Web 页面展示。这一步复用的是同一份用户 session，因此和 Telegram 客户端本身看到的内容完全一致——**私密频道的前提是这个账号本来就在那个频道里**，守护进程不会（也没法）绕过 Telegram 的权限。

支持的链接形态：

| 链接 | 说明 |
|------|------|
| `https://t.me/c/1234567890/456` | 私密频道（`c` 后面是频道内部数字 ID） |
| `https://t.me/c/1234567890/12/456` | 私密频道的话题（forum topic）里的消息 |
| `https://t.me/c/1234567890/456-460` | 连续区间，一次抓多条 |
| `https://t.me/channelname/456` | 公开频道 |
| `https://t.me/s/channelname/456` | 公开频道的网页预览形态 |
| `https://t.me/+AbCdEfGh/456` | 邀请链接（未加入时需开启 `TELEGRAM_DAEMON_LINK_AUTO_JOIN`） |
| `tg://privatepost?channel=123&post=456` | 客户端内部协议链接 |

行为细节：

* 一条消息里可以同时贴多个链接，全部会被解析；正文里的裸链接和"中文文字挂着超链接"两种形式都能识别。
* 链接指向相册（一次发多张图/多个视频）中的一条时，默认把整组一起下载；链接自带 `?single` 时只下这一条。用 `TELEGRAM_DAEMON_LINK_ALBUM=0` 可以永远只下链接指向的那条。
* 单条消息最多展开 `TELEGRAM_DAEMON_LINK_MAX_MESSAGES`（默认 50）条 Telegram 消息，避免 `a-b` 区间链接把队列打爆；超出部分会在回复里明确说明。
* 守护进程会先回一条"正在解析…"，处理完把它编辑成结果汇总（每个链接一行：入队几个 / 没有可下载文件 / 无权限）。前 5 个文件还会各自有一条带进度的回复，再多就只静默入队，进度看 Web 页面，避免触发 Telegram 限流。
* **不会往别人的频道里发任何消息**：状态和失败通知一律回到被监听频道。
* 冷启动后第一次遇到某个私密频道时，Telethon 缓存里可能还没有该频道的 `access_hash`，守护进程会自动拉一次会话列表再重试（日志里能看到 `Refreshing dialog cache`）。账号确实不在该频道时，会回复"没有这个私密频道的访问权限"。

常见问题：

* **回复"没有这个私密频道的访问权限"** —— 用登录这个守护进程的那个账号去加入该频道，然后重发链接即可。
* **想关掉这个能力** —— 设置 `TELEGRAM_DAEMON_LINK_DOWNLOAD=0`，此时纯文本消息又只按 `list/status/clean/queue` 命令处理，行为和旧版一致。

> 这个能力对齐了 `tg_forward_bot` 里用 `tdl` 抓私密频道链接的做法，但没有引入 `tdl` 二进制和第二份登录态：链接解析和抓取都走本项目已有的 Telethon 会话，因此队列、重试、断点恢复、Web UI、缩略图、按类型归档这些都自动生效。

# Docker

## 配置环境变量

使用 docker-compose 前，先把示例配置复制为 `.env` 并填入你的密钥（`docker compose` 会自动读取同目录下的 `.env`）：

```bash
cp .env.example .env
# 编辑 .env，至少填写 TELEGRAM_DAEMON_API_ID / API_HASH / CHANNEL
# 强烈建议同时设置 TELEGRAM_DAEMON_WEB_TOKEN 开启 Web 鉴权
```

`.env` 已在 `.gitignore` 中忽略，不会被提交，请放心填写真实密钥。各配置项的含义见 `.env.example` 内的注释，或上文「使用」表格。

## 构建与运行

推荐自行编译镜像，而不是使用预编译的镜像。
只需要注释掉 `docker-compose.yml` 中的 `image` 语句，放开 `build` 的注释，并执行 `docker-compose build --no-cache`。

`docker pull wujiezero/telegram-dd`

当我们使用 [`TelegramClient`](https://docs.telethon.dev/en/latest/quick-references/client-reference.html#telegramclient) 方法时，它要求我们与 `Console` 交互，提供电话号码并使用安全码确认。

要做到这一点，在使用 *Docker* 时，您需要**交互式**运行容器第一次。

当您使用 `docker-compose` 时，存储登录信息的 `.session` 文件保存在容器外部的 *Volume* 中。因此，在使用 docker-compose 时，您需要：

```bash
$ docker-compose run --rm telegram-dd
# 与控制台交互进行身份验证。
# 看到消息 "Signed in successfully as {your name}"
# 关闭容器
$ docker-compose up -d
```

查看 [docker-compose.yml](docker-compose.yml) 文件中的 `sessions` 卷配置。

# 管理脚本

仓库提供统一的管理脚本 `tdd.sh`，封装了常用的启停与运维操作（基于 `docker compose`，自动兼容旧版 `docker-compose`）：

```bash
./tdd.sh start            # 启动服务（后台）
./tdd.sh stop             # 停止并移除容器
./tdd.sh restart          # 重启服务
./tdd.sh status           # 查看容器状态并探测健康检查
./tdd.sh logs [-f]        # 查看日志（-f 持续跟随）
./tdd.sh rebuild          # 无缓存重建镜像并重启
./tdd.sh shell            # 进入容器交互式 shell
./tdd.sh login            # 首次交互式登录（输入手机号 + 验证码）
./tdd.sh test             # 在本地虚拟环境运行单元测试
./tdd.sh health           # 仅探测 /healthz 健康检查端点
```

首次部署的推荐流程：

```bash
./tdd.sh login    # 按提示完成 Telegram 登录，看到 "Signed in successfully" 后 Ctrl+C 退出
./tdd.sh start    # 后台启动
./tdd.sh status   # 确认运行正常
```

> 旧的 `rebuild.sh` 仍可使用，它现在会转发到 `./tdd.sh rebuild`。

# 单元测试

纯逻辑（文件名清洗、路径安全、文件类型归类、分页参数归一化、session 锁路径、Telegram 链接解析等）已抽离到 `tdd_utils.py` / `sessionManager.py` / `tg_links.py`，便于在不启动 Telegram 客户端的情况下测试。

守护进程主文件在 import 时就会解析命令行参数并连接 Telegram，没法直接 import，因此 `tests/test_link_extraction.py` 与 `tests/test_link_handler.py` 用 AST 把待测函数从**真实源码**里摘出来单独编译执行，再替换掉它依赖的 Telegram 客户端。改动这两个函数的签名时记得同步这两个测试。

```bash
# 方式一：通过管理脚本
./tdd.sh test

# 方式二：直接用 pytest
python -m pytest tests/ -v

# 方式三：仅用标准库 unittest（无需安装 pytest）
python -m unittest discover -s tests -v
```
