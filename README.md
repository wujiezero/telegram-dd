# telegram-dd

此项目致敬🫡 https://github.com/alfem/telegram-download-daemon

在前辈的基础上：
```
1. 增加了代理的配置。
2. 优化了一些细节。
3. 增加了一个简单的页面用于查看下载记录，在7373端口。
4. 增加了下载文件按规则归类到不同路径。
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

# Docker

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
