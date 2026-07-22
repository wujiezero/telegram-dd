# Use official Python image with pip mirror for mainland China
#
# 编译层刻意和运行层用**同一个** slim 镜像：requirements.txt 里需要编译的三个依赖
# （cffi / cryptg / Pillow）在 x86_64 与 aarch64 上都有现成的 manylinux 轮子，
# 用不到完整镜像里的 gcc。少拉一个 ~1GB 的基础镜像，构建更快，也少一次
# Docker Hub 往返（国内网络下 `load metadata for python:3.12` 经常读到一半断流）。
FROM python:3.12-slim AS compile-image

# Copy requirements.txt first for better caching
COPY requirements.txt ./

# 先装在 /install 下，便于后续 COPY --from 直接拿到纯净的 site-packages
#
# 前三次尝试显式清掉代理变量：docker CLI 会把 ~/.docker/config.json 里 proxies 段的
# 配置自动注入成构建期的 HTTP_PROXY/HTTPS_PROXY。宿主上写的往往是 127.0.0.1:xxxx，
# 而在构建容器里 127.0.0.1 指的是容器自己，于是 pip 每个请求都撞上
# "Cannot connect to proxy / Connection reset by peer"——连本来直连就通的国内源
# 也一起遭殃。国内源排在前面（更快且无需代理）；最后一档保留原始代理环境去官方源，
# 供确实必须走代理才能出网的主机兜底。
RUN NOPROXY="env -u HTTP_PROXY -u HTTPS_PROXY -u http_proxy -u https_proxy -u ALL_PROXY -u all_proxy -u NO_PROXY -u no_proxy"; \
    $NOPROXY pip install --no-cache-dir --prefix=/install -i https://mirrors.aliyun.com/pypi/simple/ -r requirements.txt || \
    $NOPROXY pip install --no-cache-dir --prefix=/install -i https://pypi.tuna.tsinghua.edu.cn/simple -r requirements.txt || \
    $NOPROXY pip install --no-cache-dir --prefix=/install -r requirements.txt || \
    pip install --no-cache-dir --prefix=/install -r requirements.txt

FROM python:3.12-slim AS run-image

# 只复制安装好的依赖 + 可执行脚本（如果有的话）
COPY --from=compile-image /install /usr/local

# 按用户要求：容器以 root 身份运行，便于挂载任意宿主目录。
# 如需以非 root 重新启用，加回 USER 指令并对挂载目录做 chown。
RUN mkdir -p /app /downloads /session /app/db /app/logs

WORKDIR /app
COPY *.py ./
COPY templates ./templates

# 声明外部卷：下载区 / session / DB 都可以挂主机目录
VOLUME ["/downloads", "/session", "/app/db", "/app/logs"]

# 默认暴露 Web UI 端口
EXPOSE 7373

# 更直观的默认路径，docker-compose 再覆盖也行
ENV TELEGRAM_DAEMON_DEST=/downloads \
    TELEGRAM_DAEMON_SESSION_PATH=/session \
    TELEGRAM_DAEMON_LOG_DIR=/app/logs

HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD python3 -c "import urllib.request, sys; \
                    r=urllib.request.urlopen('http://127.0.0.1:7373/healthz', timeout=3); \
                    sys.exit(0 if r.status == 200 else 1)" || exit 1

CMD [ "python3", "./telegram-download-daemon.py" ]
