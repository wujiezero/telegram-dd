# Use official Python image with pip mirror for mainland China
FROM python:3.12 AS compile-image

# Copy requirements.txt first for better caching
COPY requirements.txt ./

# 先装在 /install 下，便于后续 COPY --from 直接拿到纯净的 site-packages
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt || \
    pip install --no-cache-dir --prefix=/install -i https://mirrors.aliyun.com/pypi/simple/ -r requirements.txt || \
    pip install --no-cache-dir --prefix=/install -i https://pypi.tuna.tsinghua.edu.cn/simple -r requirements.txt

FROM python:3.12-slim AS run-image

# 只复制安装好的依赖 + 可执行脚本（如果有的话）
COPY --from=compile-image /install /usr/local

# 以非 root 身份运行：UID/GID 可通过 --build-arg 覆盖，便于和宿主机用户对齐
ARG APP_UID=1000
ARG APP_GID=1000

RUN groupadd --system --gid ${APP_GID} tdd \
    && useradd --system --uid ${APP_UID} --gid ${APP_GID} --home /app --shell /usr/sbin/nologin tdd \
    && mkdir -p /app /downloads /session /app/db /app/logs \
    && chown -R tdd:tdd /app /downloads /session

WORKDIR /app
COPY --chown=tdd:tdd *.py ./
COPY --chown=tdd:tdd templates ./templates

# 声明外部卷：下载区 / session / DB 都可以挂主机目录
VOLUME ["/downloads", "/session", "/app/db", "/app/logs"]

# 默认暴露 Web UI 端口
EXPOSE 7373

USER tdd

# 更直观的默认路径，docker-compose 再覆盖也行
ENV TELEGRAM_DAEMON_DEST=/downloads \
    TELEGRAM_DAEMON_SESSION_PATH=/session \
    TELEGRAM_DAEMON_LOG_DIR=/app/logs

HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD python3 -c "import urllib.request, sys; \
                    r=urllib.request.urlopen('http://127.0.0.1:7373/healthz', timeout=3); \
                    sys.exit(0 if r.status == 200 else 1)" || exit 1

CMD [ "python3", "./telegram-download-daemon.py" ]
