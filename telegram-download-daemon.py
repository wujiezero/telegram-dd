#!/usr/bin/env python3
# Telegram Download Daemon
# Author: Alfonso E.M. <alfonso@el-magnifico.org>
# You need to install telethon (and cryptg to speed up downloads)

from os import getenv, path
from shutil import move
import math
import time
import random
import string
import os
import os.path
import socket
import sys
import threading
import sqlite3
import glob
from mimetypes import guess_extension, guess_type
import socks
from flask import Flask, jsonify, make_response, render_template_string, request, send_file
from flask_socketio import SocketIO, disconnect
import hmac
import secrets as _secrets

from sessionManager import (
    SingleInstanceLockError,
    acquireProcessLock,
    archiveSessionArtifacts,
    getLockPath,
    getSession,
    releaseProcessLock,
    saveSession,
)
from tdd_utils import (
    WINDOWS_RESERVED_NAMES as _WINDOWS_RESERVED_NAMES,
    build_safe_path,
    compute_total_pages,
    ensure_existing_path_within,
    getFileTypeCategory,
    getRandomId,
    normalize_pagination,
    sanitize_filename,
)
from fast_download import (
    align_down,
    choose_part_size,
    CdnRedirectNeeded,
    download_file as fast_download_file,
    get_parallel_location,
)
from tg_links import parse_telegram_links, utf16_slice

from telethon import TelegramClient, events, __version__
from telethon.tl.functions.messages import (
    CheckChatInviteRequest,
    ImportChatInviteRequest,
)
from telethon.tl.types import (
    PeerChannel,
    DocumentAttributeFilename,
    DocumentAttributeVideo,
    MessageEntityTextUrl,
    MessageEntityUrl,
)
from telethon.errors import AuthKeyDuplicatedError, SessionPasswordNeededError
import logging
from logging.handlers import RotatingFileHandler

def _env_or_none(name, default=None):
    """把"设了但是空值"的环境变量当成没设。

    docker-compose 用 ``${VAR:-}`` 传递可选项时，未配置的项会以**空字符串**进到
    容器里，而不是不存在。这对 ``--api-id`` / ``--channel`` / ``--proxy-port``
    这类 ``type=int`` 的参数是致命的：argparse 会把字符串默认值也套一遍 type
    转换，``int("")`` 直接让进程起不来，而且报的是 "invalid int value: ''"
    这种看不出所以然的错。空值当没设，才能正常走到"缺必填项"的提示上。
    """
    value = getenv(name)
    if value is None or not str(value).strip():
        return default
    return value


LOG_FORMAT = '[%(levelname) 5s/%(asctime)s] %(name)s: %(message)s'
LOG_LEVEL_NAME = getenv("TELEGRAM_DAEMON_LOG_LEVEL", "INFO").upper()
LOG_LEVEL = getattr(logging, LOG_LEVEL_NAME, logging.INFO)
LOG_DIR = getenv("TELEGRAM_DAEMON_LOG_DIR", os.path.join(os.getcwd(), "logs"))
LOG_FILE = getenv("TELEGRAM_DAEMON_LOG_FILE", "telegram-download-daemon.log")
LOG_MAX_BYTES = int(getenv("TELEGRAM_DAEMON_LOG_MAX_BYTES", str(5 * 1024 * 1024)))
LOG_BACKUP_COUNT = int(_env_or_none("TELEGRAM_DAEMON_LOG_BACKUP_COUNT", "5"))

root_logger = logging.getLogger()
root_logger.setLevel(LOG_LEVEL)
root_logger.handlers.clear()

console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter(LOG_FORMAT))
console_handler.setLevel(LOG_LEVEL)
root_logger.addHandler(console_handler)

# 文件日志是"锦上添花"而不是"必需品"——Docker bind-mount 挂出来的宿主目录
# 可能属 root（UID 0），容器里的非 root 用户（UID 1000）就无权写入。
# 这里做"失败降级"：目录建不了或文件打不开，就只用 stdout，日志仍能走 Docker logs 通道。
log_path = os.path.join(LOG_DIR, LOG_FILE)
_file_log_error = None
try:
    os.makedirs(LOG_DIR, exist_ok=True)
    file_handler = RotatingFileHandler(
        log_path,
        maxBytes=LOG_MAX_BYTES,
        backupCount=LOG_BACKUP_COUNT,
        encoding='utf-8',
    )
    file_handler.setFormatter(logging.Formatter(LOG_FORMAT))
    file_handler.setLevel(LOG_LEVEL)
    root_logger.addHandler(file_handler)
except (OSError, PermissionError) as e:
    _file_log_error = e
    # 还没有 logger 可用，先 stderr 打一条给运维看
    sys.stderr.write(
        f"[WARN] Cannot open log file {log_path!r}: {e!s}. "
        f"Falling back to stdout-only logging. "
        f"Hint: if you see this in Docker, chown the bind-mounted logs dir to "
        f"the container UID (default 1000:1000) or set TELEGRAM_DAEMON_LOG_DIR to a writable path.\n"
    )

logger = logging.getLogger('telegram-download-daemon')
if _file_log_error is not None:
    logger.warning(
        "File logging disabled (%s); running with console handler only.",
        _file_log_error,
    )

import multiprocessing
import argparse
import asyncio
import contextlib


TDD_VERSION="2.0"

TELEGRAM_DAEMON_API_ID = _env_or_none("TELEGRAM_DAEMON_API_ID")
TELEGRAM_DAEMON_API_HASH = _env_or_none("TELEGRAM_DAEMON_API_HASH")
TELEGRAM_DAEMON_CHANNEL = _env_or_none("TELEGRAM_DAEMON_CHANNEL")

TELEGRAM_DAEMON_SESSION_PATH = _env_or_none("TELEGRAM_DAEMON_SESSION_PATH")

TELEGRAM_DAEMON_DEST=_env_or_none("TELEGRAM_DAEMON_DEST", "/telegram-downloads")
TELEGRAM_DAEMON_TEMP=_env_or_none("TELEGRAM_DAEMON_TEMP", "")
TELEGRAM_DAEMON_DUPLICATES=_env_or_none("TELEGRAM_DAEMON_DUPLICATES", "rename")

TELEGRAM_DAEMON_TEMP_SUFFIX="tdd"

TELEGRAM_DAEMON_WORKERS=_env_or_none("TELEGRAM_DAEMON_WORKERS", multiprocessing.cpu_count())
TELEGRAM_DAEMON_PROXY_HOST=_env_or_none("TELEGRAM_DAEMON_PROXY_HOST")
TELEGRAM_DAEMON_PROXY_PORT=_env_or_none("TELEGRAM_DAEMON_PROXY_PORT")
TELEGRAM_DAEMON_PROXY_TYPE=_env_or_none("TELEGRAM_DAEMON_PROXY_TYPE", "socks5")
TELEGRAM_DAEMON_PROXY_USERNAME=_env_or_none("TELEGRAM_DAEMON_PROXY_USERNAME")
TELEGRAM_DAEMON_PROXY_PASSWORD=_env_or_none("TELEGRAM_DAEMON_PROXY_PASSWORD")
TELEGRAM_DAEMON_PROXY_RESOLVE_ONCE=str(_env_or_none("TELEGRAM_DAEMON_PROXY_RESOLVE_ONCE", "0")).strip().lower() in ("1", "true", "yes", "on")

# 可配置参数
# 下载超时的**保底值**。真正生效的是按文件大小折算出来的值，见 MIN_SPEED_BPS：
# 一刀切的总时长会把"正常但慢"的大文件误杀，而真正卡死的下载由 NO_PROGRESS_TIMEOUT 兜底。
TELEGRAM_DAEMON_DOWNLOAD_TIMEOUT=int(_env_or_none("TELEGRAM_DAEMON_DOWNLOAD_TIMEOUT", "3600"))  # 下载超时下限，默认1小时
# 判定"慢到不值得等"的速度线（字节/秒）。单文件超时 = max(DOWNLOAD_TIMEOUT, 文件大小 / 该速度)。
# 默认 48KB/s：2GB 的文件因此有约 12 小时额度，而不是被 3600 秒一刀切掉。
TELEGRAM_DAEMON_MIN_SPEED_BPS=int(_env_or_none("TELEGRAM_DAEMON_MIN_SPEED_BPS", "49152"))
TELEGRAM_DAEMON_UPDATE_FREQUENCY=int(_env_or_none("TELEGRAM_DAEMON_UPDATE_FREQUENCY", "10"))  # 进度更新频率，默认10秒
TELEGRAM_DAEMON_START_TIMEOUT=int(_env_or_none("TELEGRAM_DAEMON_START_TIMEOUT", "120"))  # 开始下载超时，默认2分钟
TELEGRAM_DAEMON_NO_PROGRESS_TIMEOUT=int(_env_or_none("TELEGRAM_DAEMON_NO_PROGRESS_TIMEOUT", "300"))  # 无进度超时，默认5分钟
TELEGRAM_DAEMON_MAX_RETRIES=int(_env_or_none("TELEGRAM_DAEMON_MAX_RETRIES", "3"))  # 最大重试次数，默认3次
TELEGRAM_DAEMON_NOTIFY_FAILURE=bool(int(_env_or_none("TELEGRAM_DAEMON_NOTIFY_FAILURE", "1")))  # 失败通知，默认开启
TELEGRAM_DAEMON_QUEUE_WARN_SECONDS=int(_env_or_none("TELEGRAM_DAEMON_QUEUE_WARN_SECONDS", "120"))
# 通知类 RPC（编辑状态消息、发提醒）的统一超时（秒），见 bounded_notify
TELEGRAM_DAEMON_NOTIFY_RPC_TIMEOUT=int(_env_or_none("TELEGRAM_DAEMON_NOTIFY_RPC_TIMEOUT", "30"))
# 队列非空但 0 个 worker 在消化，持续超过该秒数就按"流水线停摆"升级 CRITICAL
TELEGRAM_DAEMON_PIPELINE_STALL_CRITICAL_SECONDS=int(_env_or_none("TELEGRAM_DAEMON_PIPELINE_STALL_CRITICAL_SECONDS", "600"))
# 单文件并行下载连接数：>1 时对足够大的文件启用多连接并行分块下载。
# Telegram 的下载限速按连接计，单连接（=1）就是天花板，非 Premium 账号尤其明显。
# 默认 4：明显提速且不容易触发 flood-wait；网络好可以往上调到 8。
TELEGRAM_DAEMON_PARALLEL_CONNECTIONS=int(_env_or_none("TELEGRAM_DAEMON_PARALLEL_CONNECTIONS", "4"))
# 只有体积 >= 该阈值（MB）的文件才走并行下载；小文件并行收益低且更易触发限流。默认 10MB。
TELEGRAM_DAEMON_PARALLEL_MIN_SIZE_MB=int(_env_or_none("TELEGRAM_DAEMON_PARALLEL_MIN_SIZE_MB", "10"))
# 断点续传：下载超时 / 出错时保留 .tdd 临时文件，重试时从已下载的字节数接着下。
# 关掉的话行为回到老样子（每次重试都从 0 开始）。
TELEGRAM_DAEMON_RESUME=str(_env_or_none("TELEGRAM_DAEMON_RESUME", "1")).strip().lower() in ("1", "true", "yes", "on")
# 残留临时文件的保留时长（小时）。续传要靠这些文件，所以比原先的 24 小时放宽。
TELEGRAM_DAEMON_TEMP_MAX_AGE_HOURS=int(_env_or_none("TELEGRAM_DAEMON_TEMP_MAX_AGE_HOURS", "72"))


def _env_flag(name, default="0"):
    return str(getenv(name, default)).strip().lower() in ("1", "true", "yes", "on")


# ---------------------------------------------------------------------------
# 消息链接下载（私密频道链接监听）
# ---------------------------------------------------------------------------
# 被监听频道里出现 https://t.me/c/<频道内部ID>/<消息ID> 这类链接时，守护进程会用
# 当前登录的账号把对应消息抓回来并下载其中的媒体（等价于 tg_forward_bot 里 tdl
# 的能力，但复用了本项目的队列 / 数据库 / Web UI / 重试机制）。
# 前提：登录的账号本身是那个私密频道的成员，否则 Telegram 会直接拒绝。
TELEGRAM_DAEMON_LINK_DOWNLOAD=_env_flag("TELEGRAM_DAEMON_LINK_DOWNLOAD", "1")
# 链接指向相册（grouped_id）中的一条时，是否把整组一起下载（链接带 ?single 时始终只下这一条）
TELEGRAM_DAEMON_LINK_ALBUM=_env_flag("TELEGRAM_DAEMON_LINK_ALBUM", "1")
# 遇到未加入的 t.me/+xxx 邀请链接时，是否用当前账号自动加入该频道。
# 默认关闭：这是会改变账号状态的动作，需要显式开启。
TELEGRAM_DAEMON_LINK_AUTO_JOIN=_env_flag("TELEGRAM_DAEMON_LINK_AUTO_JOIN", "0")
# 单条消息里所有链接合计最多展开多少条 Telegram 消息，防止 a-b 区间链接把队列打爆
TELEGRAM_DAEMON_LINK_MAX_MESSAGES=int(_env_or_none("TELEGRAM_DAEMON_LINK_MAX_MESSAGES", "50"))

parser = argparse.ArgumentParser(
    description="Script to download files from a Telegram Channel.")
parser.add_argument(
    "--proxy-host",
    type=str,
    default=TELEGRAM_DAEMON_PROXY_HOST,
    help=
    'Proxy host to use for Telegram connection (default is TELEGRAM_DAEMON_PROXY_HOST env var)'
)
parser.add_argument(
    "--proxy-port",
    type=int,
    default=TELEGRAM_DAEMON_PROXY_PORT,
    help=
    'Proxy port to use for Telegram connection (default is TELEGRAM_DAEMON_PROXY_PORT env var)'
)
parser.add_argument(
    "--proxy-type",
    type=str,
    default=TELEGRAM_DAEMON_PROXY_TYPE,
    help=
    'Proxy type to use for Telegram connection (default is TELEGRAM_DAEMON_PROXY_TYPE env var, default: socks5)'
)
parser.add_argument(
    "--proxy-username",
    type=str,
    default=TELEGRAM_DAEMON_PROXY_USERNAME,
    help=
    'Proxy username (default is TELEGRAM_DAEMON_PROXY_USERNAME env var)'
)
parser.add_argument(
    "--proxy-password",
    type=str,
    default=TELEGRAM_DAEMON_PROXY_PASSWORD,
    help=
    'Proxy password (default is TELEGRAM_DAEMON_PROXY_PASSWORD env var)'
)
parser.add_argument(
    "--proxy-resolve-once",
    dest="proxy_resolve_once",
    action="store_true",
    default=TELEGRAM_DAEMON_PROXY_RESOLVE_ONCE,
    help=
    'Resolve the proxy host once at startup and pin that IP for the process lifetime. Useful for DNS load-balanced proxies.'
)
parser.add_argument(
    "--no-proxy-resolve-once",
    dest="proxy_resolve_once",
    action="store_false",
    help=
    'Disable one-time proxy host resolution and keep using the configured proxy hostname directly.'
)
parser.add_argument(
    "--api-id",
    required=TELEGRAM_DAEMON_API_ID == None,
    type=int,
    default=TELEGRAM_DAEMON_API_ID,
    help=
    'api_id from https://core.telegram.org/api/obtaining_api_id (default is TELEGRAM_DAEMON_API_ID env var)'
)
parser.add_argument(
    "--api-hash",
    required=TELEGRAM_DAEMON_API_HASH == None,
    type=str,
    default=TELEGRAM_DAEMON_API_HASH,
    help=
    'api_hash from https://core.telegram.org/api/obtaining_api_id (default is TELEGRAM_DAEMON_API_HASH env var)'
)
parser.add_argument(
    "--dest",
    type=str,
    default=TELEGRAM_DAEMON_DEST,
    help=
    'Destination path for downloaded files (default is /telegram-downloads).')
parser.add_argument(
    "--temp",
    type=str,
    default=TELEGRAM_DAEMON_TEMP,
    help=
    'Destination path for temporary files (default is using the same downloaded files directory).')
parser.add_argument(
    "--channel",
    required=TELEGRAM_DAEMON_CHANNEL == None,
    type=int,
    default=TELEGRAM_DAEMON_CHANNEL,
    help=
    'Channel id to download from it (default is TELEGRAM_DAEMON_CHANNEL env var'
)
parser.add_argument(
    "--duplicates",
    choices=["ignore", "rename", "overwrite"],
    type=str,
    default=TELEGRAM_DAEMON_DUPLICATES,
    help=
    '"ignore"=do not download duplicated files, "rename"=add a random suffix, "overwrite"=redownload and overwrite.'
)
parser.add_argument(
    "--workers",
    type=int,
    default=TELEGRAM_DAEMON_WORKERS,
    help=
    'number of simultaneous downloads'
)
parser.add_argument(
    "--parallel-connections",
    type=int,
    default=TELEGRAM_DAEMON_PARALLEL_CONNECTIONS,
    help=
    'connections per file for parallel chunked download (>1 enables FastTelethon-style '
    'parallel download to saturate Premium high-speed quota; 1 keeps the default serial download). '
    'Default is TELEGRAM_DAEMON_PARALLEL_CONNECTIONS env var.'
)
parser.add_argument(
    "--parallel-min-size-mb",
    type=int,
    default=TELEGRAM_DAEMON_PARALLEL_MIN_SIZE_MB,
    help=
    'only files at least this many MB use parallel download (default is '
    'TELEGRAM_DAEMON_PARALLEL_MIN_SIZE_MB env var).'
)
parser.add_argument(
    "--link-download",
    dest="link_download",
    action="store_true",
    default=TELEGRAM_DAEMON_LINK_DOWNLOAD,
    help=
    'Download the media behind Telegram message links (t.me/c/<id>/<msg>, t.me/<user>/<msg>, ...) '
    'posted in the monitored channel. Default is TELEGRAM_DAEMON_LINK_DOWNLOAD env var (enabled).'
)
parser.add_argument(
    "--no-link-download",
    dest="link_download",
    action="store_false",
    help='Ignore Telegram message links and keep treating plain text as commands only.'
)
parser.add_argument(
    "--link-album",
    dest="link_album",
    action="store_true",
    default=TELEGRAM_DAEMON_LINK_ALBUM,
    help=
    'When a link points to one item of an album, queue the whole album (links carrying "?single" '
    'always download just that item). Default is TELEGRAM_DAEMON_LINK_ALBUM env var (enabled).'
)
parser.add_argument(
    "--no-link-album",
    dest="link_album",
    action="store_false",
    help='Only download the exact message the link points to, never the rest of its album.'
)
parser.add_argument(
    "--link-auto-join",
    dest="link_auto_join",
    action="store_true",
    default=TELEGRAM_DAEMON_LINK_AUTO_JOIN,
    help=
    'Automatically join private channels through t.me/+<hash> invite links when not a member yet. '
    'Default is TELEGRAM_DAEMON_LINK_AUTO_JOIN env var (disabled).'
)
parser.add_argument(
    "--link-max-messages",
    type=int,
    default=TELEGRAM_DAEMON_LINK_MAX_MESSAGES,
    help=
    'Maximum number of Telegram messages a single incoming text may expand to '
    '(default is TELEGRAM_DAEMON_LINK_MAX_MESSAGES env var).'
)
args = parser.parse_args()


def resolve_proxy_host_once(host, port):
    if not host:
        return host

    for family in (socket.AF_INET, socket.AF_INET6):
        try:
            socket.inet_pton(family, host)
            return host
        except OSError:
            continue

    try:
        address_info = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
    except socket.gaierror as exc:
        logger.warning("Failed to resolve proxy host %s:%s once: %s", host, port, exc)
        return host

    ipv4_matches = [entry for entry in address_info if entry[0] == socket.AF_INET]
    preferred_entry = ipv4_matches[0] if ipv4_matches else address_info[0]
    resolved_host = preferred_entry[4][0]
    logger.info("Resolved proxy host %s:%s to %s for this process", host, port, resolved_host)
    return resolved_host

api_id = args.api_id
api_hash = args.api_hash
channel_id = args.channel
downloadFolder = args.dest
tempFolder = args.temp
duplicates=args.duplicates
worker_count = args.workers
parallel_connections = max(1, int(args.parallel_connections))
parallel_min_size_bytes = max(0, int(args.parallel_min_size_mb)) * 1024 * 1024
updateFrequency = TELEGRAM_DAEMON_UPDATE_FREQUENCY
download_timeout = TELEGRAM_DAEMON_DOWNLOAD_TIMEOUT
min_speed_bps = max(1, TELEGRAM_DAEMON_MIN_SPEED_BPS)
resume_enabled = TELEGRAM_DAEMON_RESUME
temp_max_age_seconds = max(1, TELEGRAM_DAEMON_TEMP_MAX_AGE_HOURS) * 3600
start_timeout = TELEGRAM_DAEMON_START_TIMEOUT
no_progress_timeout = TELEGRAM_DAEMON_NO_PROGRESS_TIMEOUT
max_retries = TELEGRAM_DAEMON_MAX_RETRIES
notify_failure = TELEGRAM_DAEMON_NOTIFY_FAILURE
link_download_enabled = bool(args.link_download)
link_album_enabled = bool(args.link_album)
link_auto_join_enabled = bool(args.link_auto_join)
link_max_messages = max(1, int(args.link_max_messages))
lastUpdate = 0

if not tempFolder:
    tempFolder = downloadFolder
   
# Proxy configuration
connection = None
proxy = None
proxy_configured_host = None
proxy_runtime_host = None
proxy_resolved_once = False
if args.proxy_host and args.proxy_port:
    # 使用字符串格式的代理类型，确保兼容性
    proxy_type_str = args.proxy_type.lower()
    
    # 确保代理类型是Telethon支持的格式
    if proxy_type_str not in ['socks5', 'http', 'mtproxy']:
        proxy_type_str = 'socks5'  # 默认使用SOCKS5
    
    # 将字符串代理类型映射到 PySocks 常量
    proxy_type_map = {
        'socks5': socks.SOCKS5,
        'http': socks.HTTP,
    }
    proxy_type_const = proxy_type_map.get(proxy_type_str, socks.SOCKS5)
    proxy_configured_host = args.proxy_host
    proxy_runtime_host = args.proxy_host

    if args.proxy_resolve_once and proxy_type_str in ['socks5', 'http']:
        proxy_runtime_host = resolve_proxy_host_once(args.proxy_host, int(args.proxy_port))
        proxy_resolved_once = proxy_runtime_host != args.proxy_host
    elif proxy_type_str in ['socks5', 'http']:
        try:
            socket.inet_pton(socket.AF_INET, args.proxy_host)
        except OSError:
            try:
                socket.inet_pton(socket.AF_INET6, args.proxy_host)
            except OSError:
                logger.warning(
                    "Proxy host %s is a hostname. If your provider does DNS load balancing and Telegram reports AUTH_KEY_DUPLICATED, enable --proxy-resolve-once or TELEGRAM_DAEMON_PROXY_RESOLVE_ONCE=1.",
                    args.proxy_host,
                )
    
    # 根据是否有认证信息创建代理配置
    if args.proxy_username and args.proxy_password:
        proxy = (
            proxy_type_const,
            proxy_runtime_host,
            int(args.proxy_port),
            False,
            args.proxy_username,
            args.proxy_password
        )
        print(f"Using proxy: {proxy_type_str}://{args.proxy_username}:******@{args.proxy_host}:{args.proxy_port}")
    else:
        proxy = (
            proxy_type_const,
            proxy_runtime_host,
            int(args.proxy_port),
            False
        )
        print(f"Using proxy without auth: {proxy_type_str}://{args.proxy_host}:{args.proxy_port}")

# 文件类型归类规则 (FILE_TYPE_RULES) 与 getFileTypeCategory() 已抽到 tdd_utils，便于单元测试。

# Database Configuration
# Use /app/db directory for database file in container, or current directory in development
DB_DIR = '/app/db' if os.path.exists('/app/db') else os.path.dirname(__file__)
DB_PATH = os.path.join(DB_DIR, 'downloads.db')
logger.info(f"Database path: {DB_PATH}")

# Initialize database
try:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    cursor = conn.cursor()
    logger.info("Database connection established successfully")
    
    # Create downloads table if not exists
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS downloads (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        filename TEXT NOT NULL,
        file_type TEXT NOT NULL,
        status TEXT NOT NULL,
        size INTEGER DEFAULT 0,
        progress REAL DEFAULT 0.0,
        download_path TEXT,
        thumbnail_path TEXT,
        retry_count INTEGER DEFAULT 0,
        source_channel_id INTEGER,
        source_message_id INTEGER,
        source_message_link TEXT,
        target_dir TEXT,
        start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        end_time TIMESTAMP,
        error_message TEXT
    )
    ''')
    
    # 检查并添加 thumbnail_path 列（升级旧数据库）
    try:
        cursor.execute("ALTER TABLE downloads ADD COLUMN thumbnail_path TEXT")
        logger.info("Added thumbnail_path column to downloads table")
    except sqlite3.OperationalError:
        pass  # 列已存在
    
    # 检查并添加 retry_count 列
    try:
        cursor.execute("ALTER TABLE downloads ADD COLUMN retry_count INTEGER DEFAULT 0")
        logger.info("Added retry_count column to downloads table")
    except sqlite3.OperationalError:
        pass  # 列已存在

    try:
        cursor.execute("ALTER TABLE downloads ADD COLUMN source_channel_id INTEGER")
        logger.info("Added source_channel_id column to downloads table")
    except sqlite3.OperationalError:
        pass

    try:
        cursor.execute("ALTER TABLE downloads ADD COLUMN source_message_id INTEGER")
        logger.info("Added source_message_id column to downloads table")
    except sqlite3.OperationalError:
        pass

    try:
        cursor.execute("ALTER TABLE downloads ADD COLUMN source_message_link TEXT")
        logger.info("Added source_message_link column to downloads table")
    except sqlite3.OperationalError:
        pass

    try:
        cursor.execute("ALTER TABLE downloads ADD COLUMN target_dir TEXT")
        logger.info("Added target_dir column to downloads table")
    except sqlite3.OperationalError:
        pass

    # 断点续传：记住这条下载正在写哪个 .tdd 临时文件。重试时据此判断"这个残留文件
    # 是我自己的半成品"，从而接着下，而不是当成重名文件改名后从 0 重来。
    try:
        cursor.execute("ALTER TABLE downloads ADD COLUMN temp_path TEXT")
        logger.info("Added temp_path column to downloads table")
    except sqlite3.OperationalError:
        pass

    for idx_sql, idx_desc in [
        ("CREATE INDEX IF NOT EXISTS idx_downloads_status ON downloads(status)", "status"),
        ("CREATE INDEX IF NOT EXISTS idx_downloads_file_type ON downloads(file_type)", "file_type"),
        ("CREATE INDEX IF NOT EXISTS idx_downloads_start_time ON downloads(start_time)", "start_time"),
        ("CREATE INDEX IF NOT EXISTS idx_downloads_filename ON downloads(filename)", "filename"),
    ]:
        try:
            cursor.execute(idx_sql)
        except Exception as idx_err:
            logger.warning("Failed to create index on %s: %s", idx_desc, idx_err)

    conn.commit()
    logger.info("Downloads table created or already exists")
except Exception as e:
    logger.error(f"Database initialization error: {e}")
    raise

# Database helper functions
def get_db_connection():
    """获取数据库连接（线程安全）"""
    return sqlite3.connect(DB_PATH, check_same_thread=False)

def db_execute_query(query, params=(), fetch=False):
    """执行数据库查询，支持事务"""
    local_conn = get_db_connection()
    local_cursor = local_conn.cursor()
    try:
        local_cursor.execute(query, params)
        if fetch:
            result = local_cursor.fetchall()
        else:
            result = local_cursor.lastrowid
        local_conn.commit()
        return result
    finally:
        local_cursor.close()
        local_conn.close()

def db_execute_many(query, params_list):
    """批量执行数据库操作"""
    local_conn = get_db_connection()
    local_cursor = local_conn.cursor()
    try:
        local_cursor.executemany(query, params_list)
        local_conn.commit()
    finally:
        local_cursor.close()
        local_conn.close()

def cleanup_temp_files():
    """清理残留的临时文件。

    开启断点续传后 .tdd 文件是**有价值的**（重试要靠它接着下），所以只清理确实
    放了很久、不会再有人来续的那些，保留时长由 TELEGRAM_DAEMON_TEMP_MAX_AGE_HOURS 控制。
    """
    try:
        temp_files = glob.glob(os.path.join(tempFolder, f"*.{TELEGRAM_DAEMON_TEMP_SUFFIX}"))
        for temp_file in temp_files:
            file_age = time.time() - os.path.getmtime(temp_file)
            if file_age > temp_max_age_seconds:
                os.remove(temp_file)
                logger.info(f"Cleaned up stale temp file: {temp_file}")
    except Exception as e:
        logger.error(f"Error cleaning up temp files: {e}")


def cleanup_temp_file_for_filename(filename):
    try:
        temp_file_path = build_safe_path(tempFolder, f"{filename}.{TELEGRAM_DAEMON_TEMP_SUFFIX}")
        if os.path.exists(temp_file_path):
            os.remove(temp_file_path)
            logger.info(f"Removed temp file for recovery: {temp_file_path}")
    except Exception as e:
        logger.error(f"Error removing temp file for {filename}: {e}")

def generate_thumbnail(file_path, file_category):
    """生成缩略图（仅对图片和视频）"""
    thumbnail_path = None
    try:
        if file_category == 'Pictures':
            # 使用 PIL 生成图片缩略图
            try:
                from PIL import Image
                img = Image.open(file_path)
                # 创建缩略图目录
                thumb_dir = os.path.join(os.path.dirname(file_path), '.thumbnails')
                os.makedirs(thumb_dir, exist_ok=True)
                thumb_name = os.path.basename(file_path) + '.jpg'
                thumbnail_path = os.path.join(thumb_dir, thumb_name)
                # 生成 200x200 的缩略图
                img.thumbnail((200, 200))
                img.convert('RGB').save(thumbnail_path, 'JPEG', quality=80)
                logger.info(f"Generated thumbnail: {thumbnail_path}")
            except ImportError:
                logger.warning("PIL not installed, skipping thumbnail generation")
            except Exception as e:
                logger.error(f"Error generating thumbnail: {e}")
        elif file_category == 'Videos':
            # 使用 ffmpeg 生成视频缩略图
            try:
                thumb_dir = os.path.join(os.path.dirname(file_path), '.thumbnails')
                os.makedirs(thumb_dir, exist_ok=True)
                thumb_name = os.path.basename(file_path) + '.jpg'
                thumbnail_path = os.path.join(thumb_dir, thumb_name)
                # 使用 ffmpeg 提取第一帧
                import subprocess
                result = subprocess.run([
                    'ffmpeg', '-i', file_path, '-ss', '00:00:01', 
                    '-vframes', '1', '-vf', 'scale=200:-1',
                    '-y', thumbnail_path
                ], capture_output=True, timeout=30)
                if result.returncode == 0:
                    logger.info(f"Generated video thumbnail: {thumbnail_path}")
                else:
                    logger.warning(f"ffmpeg failed: {result.stderr.decode()}")
                    thumbnail_path = None
            except FileNotFoundError:
                logger.warning("ffmpeg not installed, skipping video thumbnail")
            except Exception as e:
                logger.error(f"Error generating video thumbnail: {e}")
                thumbnail_path = None
    except Exception as e:
        logger.error(f"Error in generate_thumbnail: {e}")
    
    return thumbnail_path

def handle_interrupted_tasks():
    """处理中断的任务：将 downloading 状态改为 interrupted"""
    try:
        cursor.execute('''
            UPDATE downloads SET status = 'interrupted', error_message = 'Container restarted'
            WHERE status = 'downloading'
        ''')
        conn.commit()
        affected = cursor.rowcount
        if affected > 0:
            logger.info(f"Marked {affected} interrupted tasks")
    except Exception as e:
        logger.error(f"Error handling interrupted tasks: {e}")

# End of interesting parameters

# Web Server Configuration
app = Flask(__name__)
app.config['JSON_SORT_KEYS'] = False

# ---------------------------------------------------------------------------
# Web UI 鉴权 + CSRF 防护
# ---------------------------------------------------------------------------
# 主路径：前端把 token 存在 sessionStorage，后续所有 fetch 都走 Authorization: Bearer。
# 因为浏览器不会跨站自动附带这个 header，没有 ambient credentials 就不存在 CSRF 问题。
# 同时保留 cookie 路径，方便 curl / 书签访问：用 cookie 时要求同时带 X-TDD-Auth header
# 来挡住浏览器 CSRF。
WEB_AUTH_TOKEN = (getenv("TELEGRAM_DAEMON_WEB_TOKEN") or "").strip()
WEB_AUTH_COOKIE_NAME = "tdd_token"
WEB_AUTH_COOKIE_MAX_AGE = int(getenv("TELEGRAM_DAEMON_WEB_COOKIE_MAX_AGE", str(30 * 24 * 3600)))
WEB_AUTH_COOKIE_SECURE = (getenv("TELEGRAM_DAEMON_WEB_COOKIE_SECURE", "").lower() in ("1", "true", "yes"))
WEB_AUTH_COOKIE_SAMESITE = "Lax"
WEB_AUTH_PUBLIC_PATHS = {"/healthz", "/api/ui-auth", "/api/ui-auth-status"}
WEB_CSRF_SAFE_METHODS = {"GET", "HEAD", "OPTIONS"}
WEB_CSRF_HEADER = "X-TDD-Auth"


def web_auth_configured() -> bool:
    return bool(WEB_AUTH_TOKEN)


def _constant_time_eq(a, b) -> bool:
    if a is None or b is None:
        return False
    if not isinstance(a, (bytes, str)) or not isinstance(b, (bytes, str)):
        return False
    a_b = a.encode("utf-8") if isinstance(a, str) else a
    b_b = b.encode("utf-8") if isinstance(b, str) else b
    return hmac.compare_digest(a_b, b_b)


def _get_bearer_token() -> str:
    auth_header = request.headers.get("Authorization", "")
    if auth_header.lower().startswith("bearer "):
        return auth_header[7:].strip()
    return ""


def _get_cookie_token() -> str:
    return request.cookies.get(WEB_AUTH_COOKIE_NAME) or ""


def extract_request_token() -> str:
    """Bearer 优先，其次 cookie。"""
    return _get_bearer_token() or _get_cookie_token()


def is_request_authenticated() -> bool:
    if not web_auth_configured():
        return True
    return _constant_time_eq(extract_request_token(), WEB_AUTH_TOKEN)


@app.before_request
def _web_auth_gate():
    """全站鉴权 + CSRF 防护。"""
    path_ = request.path or "/"
    if path_ in WEB_AUTH_PUBLIC_PATHS or path_.startswith("/static/"):
        return None
    if path_.startswith("/socket.io"):
        # Socket.IO 握手的鉴权在 @socketio.on("connect") 里处理
        return None

    if not web_auth_configured():
        return None  # 未启用鉴权：向后兼容

    # 未登录状态下也允许拿到首页 HTML，让前端渲染登录面板
    if path_ == "/" and request.method.upper() == "GET":
        return None

    if not is_request_authenticated():
        return jsonify({"error": "Unauthorized"}), 401

    if request.method.upper() not in WEB_CSRF_SAFE_METHODS:
        # 使用 Bearer 的请求天然无 CSRF 风险；cookie-only 的 mutating 请求必须带 X-TDD-Auth
        if not _get_bearer_token():
            csrf_value = request.headers.get(WEB_CSRF_HEADER, "")
            if not _constant_time_eq(csrf_value, WEB_AUTH_TOKEN):
                return jsonify({"error": "CSRF check failed"}), 403
    return None


@app.after_request
def _web_security_headers(response):
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    response.headers["X-XSS-Protection"] = "0"
    response.headers["Content-Security-Policy"] = (
        "default-src 'self'; "
        "script-src 'self' 'unsafe-inline' https://cdnjs.cloudflare.com; "
        "style-src 'self' 'unsafe-inline'; "
        "img-src 'self' data: blob:; "
        "media-src 'self'; "
        "connect-src 'self' ws: wss: https://cdnjs.cloudflare.com; "
        "frame-ancestors 'none'; "
        "base-uri 'self'; "
        "form-action 'self'"
    )
    return response


@app.route("/healthz", methods=["GET"])
def _healthz():
    """健康检查端点，不需要鉴权。"""
    try:
        authed = bool(telegram_auth_state.get("authorized"))
    except Exception:
        authed = False
    return jsonify({"ok": True, "telegram_authorized": authed, "version": TDD_VERSION})


@app.route("/api/ui-auth-status", methods=["GET"])
def _ui_auth_status():
    """供前端知道是否需要登录。"""
    return jsonify({
        "auth_required": web_auth_configured(),
        "authenticated": is_request_authenticated(),
    })


@app.route("/api/ui-auth", methods=["POST"])
def _ui_auth():
    """前端提交 token，通过则在响应中 set cookie 方便 curl / 单页刷新回来用。"""
    if not web_auth_configured():
        return jsonify({"error": "Web auth is not enabled"}), 400
    payload = request.get_json(silent=True) or {}
    token = (payload.get("token") or "").strip()
    if not _constant_time_eq(token, WEB_AUTH_TOKEN):
        return jsonify({"error": "Invalid token"}), 401
    resp = make_response(jsonify({"ok": True}))
    resp.set_cookie(
        WEB_AUTH_COOKIE_NAME,
        token,
        max_age=WEB_AUTH_COOKIE_MAX_AGE,
        secure=WEB_AUTH_COOKIE_SECURE,
        httponly=False,  # 需要 JS 可读以便 fetch 时同步带上 X-TDD-Auth
        samesite=WEB_AUTH_COOKIE_SAMESITE,
        path="/",
    )
    return resp


@app.route("/api/ui-logout", methods=["POST"])
def _ui_logout():
    resp = make_response(jsonify({"ok": True}))
    resp.set_cookie(WEB_AUTH_COOKIE_NAME, "", max_age=0, path="/")
    return resp


# Initialize SocketIO
WEB_CORS_ORIGINS = (getenv("TELEGRAM_DAEMON_WEB_CORS_ORIGINS") or "").strip()
socketio = SocketIO(app, cors_allowed_origins=WEB_CORS_ORIGINS.split(",") if WEB_CORS_ORIGINS else [])


@socketio.on("connect")
def _socketio_on_connect(auth=None):
    """SocketIO 握手时强制鉴权（仅在配置 token 后生效）。"""
    if not web_auth_configured():
        return True
    token = ""
    if isinstance(auth, dict):
        token = (auth.get("token") or "").strip()
    if not token:
        token = extract_request_token()
    if not _constant_time_eq(token, WEB_AUTH_TOKEN):
        logger.warning("Rejected unauthenticated Socket.IO connection")
        disconnect()
        return False
    return True

# Global variables for Web Server
start_time = time.time()
web_client = None
web_in_progress = {}
web_queue_items = []
telegram_user_info = None
telegram_channel_info = None
web_retry_scheduler = None
# 供 /api/cancel 使用：把进行中的 download_task 注册进来，方便 Web UI 取消
active_download_tasks = {}  # download_id(str) -> asyncio.Task
# 队列中尚未取走的 item 快照 (download_id(str) -> queue_item)
active_queue_items_by_id = {}
web_cancel_scheduler = None
cancelled_download_ids = set()
# 暂停/恢复支持
paused_download_ids = {}   # download_id(str) -> threading.Event（is_set=可运行，cleared=暂停）
web_pause_scheduler = None
web_resume_scheduler = None
telegram_auth_state = {
    'authorized': False,
    'awaiting_code': False,
    'requires_password': False,
    'phone': '',
    'message': 'Checking Telegram authorization...',
}
web_auth_send_code = None
web_auth_verify_code = None
web_auth_verify_password = None
AUTH_SEND_CODE_COOLDOWN_SECONDS = 60
auth_send_code_cooldown_until = 0.0
auth_send_code_lock = threading.Lock()
web_server_thread = None


def set_relogin_required_state(message):
    global web_client, web_in_progress, web_queue_items, telegram_user_info, telegram_channel_info
    global web_retry_scheduler, telegram_auth_state, web_auth_send_code, web_auth_verify_code
    global web_auth_verify_password, auth_send_code_cooldown_until

    web_client = None
    web_in_progress = {}
    web_queue_items = []
    telegram_user_info = None
    telegram_channel_info = None
    web_retry_scheduler = None
    web_auth_send_code = None
    web_auth_verify_code = None
    web_auth_verify_password = None
    auth_send_code_cooldown_until = 0.0
    telegram_auth_state = {
        'authorized': False,
        'awaiting_code': False,
        'requires_password': False,
        'phone': '',
        'resend_available_in': 0,
        'message': message,
    }


def handle_auth_key_duplicated_recovery():
    archived_paths = archiveSessionArtifacts("auth_key_duplicated")
    archived_note = " Previous session files were archived." if archived_paths else ""
    message = (
        "Telegram invalidated the previous session because it appeared from multiple IP addresses. "
        "Please sign in again from this page."
        f"{archived_note}"
    )
    set_relogin_required_state(message)
    if archived_paths:
        logger.warning("Archived invalid session artifacts: %s", ", ".join(archived_paths))
    else:
        logger.warning("Telegram session was invalidated, but no local session artifact was found to archive")
    return message


def ensure_web_server_started():
    global web_server_thread

    if web_server_thread and web_server_thread.is_alive():
        return

    web_server_thread = threading.Thread(target=run_web_server, daemon=True)
    web_server_thread.start()
    logger.info("Web server started on http://0.0.0.0:7373")


def get_auth_send_code_remaining():
    remaining = auth_send_code_cooldown_until - time.time()
    if remaining <= 0:
        return 0
    return math.ceil(remaining)


# -------- total_tasks 计数缓存 --------
# emit_status_update / /api/status 以前每次都会跑一次 `SELECT COUNT(*) FROM downloads`。
# 在并发下载 + WebSocket 广播密集时，这会对 SQLite 造成不必要的压力。
# 这里改为 TTL 缓存 + 显式失效：
#   - get_total_tasks_count()：带 TTL 的缓存读取（默认 3 秒），过期或失效才访问 DB
#   - invalidate_total_tasks_count()：在 INSERT/DELETE downloads 之后调用，强制下一次读取刷新
_total_tasks_cache_lock = threading.Lock()
_total_tasks_cache = {
    'value': None,     # 上次查询到的 count
    'expires_at': 0.0, # 在这个时间戳之前可以直接复用
}
TOTAL_TASKS_CACHE_TTL = float(getenv("TELEGRAM_DAEMON_TOTAL_TASKS_CACHE_TTL", "3.0"))


def get_total_tasks_count(force_refresh=False):
    """获取 downloads 总条数，带 TTL 缓存。失败返回缓存中上次成功的值（没有则 0）。"""
    now = time.time()
    with _total_tasks_cache_lock:
        if (
            not force_refresh
            and _total_tasks_cache['value'] is not None
            and now < _total_tasks_cache['expires_at']
        ):
            return _total_tasks_cache['value']
    # 缓存过期 —— 走一次 DB（不要在锁里做 I/O）
    try:
        result = db_execute_query('SELECT COUNT(*) FROM downloads', fetch=True)
        value = result[0][0] if result else 0
    except Exception as e:
        logger.error(f'Error getting total tasks count: {e}', exc_info=True)
        # 回退到旧缓存值，实在没有就 0
        with _total_tasks_cache_lock:
            return _total_tasks_cache['value'] or 0
    with _total_tasks_cache_lock:
        _total_tasks_cache['value'] = value
        _total_tasks_cache['expires_at'] = time.time() + TOTAL_TASKS_CACHE_TTL
    return value


def invalidate_total_tasks_count():
    """在新增 / 删除记录后调用，让下一次读取强制刷新。"""
    with _total_tasks_cache_lock:
        _total_tasks_cache['expires_at'] = 0.0
    invalidate_history_count_cache()


_history_count_cache_lock = threading.Lock()
_history_count_cache = {}  # key: filter-hash -> {'value': int, 'expires_at': float}
HISTORY_COUNT_CACHE_TTL = float(getenv("TELEGRAM_DAEMON_HISTORY_COUNT_CACHE_TTL", "10.0"))


def get_cached_history_count(where_clause, params):
    """缓存 api_history 的 COUNT 查询结果，key 是 filter 的哈希。"""
    import hashlib
    cache_key = hashlib.md5(
        (where_clause + "|" + ",".join(str(p) for p in params)).encode("utf-8")
    ).hexdigest()

    now = time.time()
    with _history_count_cache_lock:
        entry = _history_count_cache.get(cache_key)
        if entry and now < entry["expires_at"]:
            return entry["value"]

    local_conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    local_cursor = local_conn.cursor()
    count_query = f"SELECT COUNT(*) FROM downloads{where_clause}"
    local_cursor.execute(count_query, params)
    value = local_cursor.fetchone()[0]
    local_cursor.close()
    local_conn.close()

    with _history_count_cache_lock:
        _history_count_cache[cache_key] = {"value": value, "expires_at": now + HISTORY_COUNT_CACHE_TTL}
    return value


def invalidate_history_count_cache():
    with _history_count_cache_lock:
        _history_count_cache.clear()


# Function to emit status update event
def emit_status_update():
    try:
        # Get total historical tasks count (TTL 缓存，避免每次广播都 COUNT(*))
        total_tasks = get_total_tasks_count()

        total_download_speed_mbps = 0.0
        for task_info in web_in_progress.values():
            speed_bps = task_info.get('speed_bps', 0.0)
            if isinstance(speed_bps, (int, float)) and speed_bps > 0:
                total_download_speed_mbps += speed_bps / (1024 * 1024)
        
        # Emit status update event
        socketio.emit('status_update', {
            'active_downloads': len(web_in_progress),
            'queue_size': len(web_queue_items),
            'total_tasks': total_tasks,
            'total_download_speed_mbps': round(total_download_speed_mbps, 2),
        })
    except Exception as e:
        logger.error(f'Error emitting status update: {e}', exc_info=True)

# API Endpoints
@app.route('/')
def index():
    global web_client
    
    # Get proxy info
    proxy_info = None
    if proxy:
        # 把 PySocks 的数值常量反查回人类可读字符串
        socks_const_to_name = {
            socks.SOCKS5: 'socks5',
            socks.HTTP: 'http',
        }
        if isinstance(proxy, tuple):
            raw_type = proxy[0] if len(proxy) > 0 else 'socks5'
            type_name = socks_const_to_name.get(raw_type, raw_type if isinstance(raw_type, str) else 'socks5')
            # Handle tuple format proxy
            proxy_info = {
                'type': type_name,
                'host': proxy_configured_host or (proxy[1] if len(proxy) > 1 else ''),
                'runtime_host': proxy_runtime_host or (proxy[1] if len(proxy) > 1 else ''),
                'port': proxy[2] if len(proxy) > 2 else '',
                'username': proxy[4] if len(proxy) > 4 else '',
                'resolved_once': proxy_resolved_once,
            }
        else:
            # Handle dict format proxy
            raw_type = proxy.get('proxy_type', 'socks5')
            type_name = socks_const_to_name.get(raw_type, raw_type if isinstance(raw_type, str) else 'socks5')
            proxy_info = {
                'type': type_name,
                'host': proxy.get('addr', ''),
                'runtime_host': proxy_runtime_host or proxy.get('addr', ''),
                'port': proxy.get('port', ''),
                'username': proxy.get('username', ''),
                'resolved_once': proxy_resolved_once,
            }
    
    # Get telegram user info (stored in a global variable that's updated when client starts)
    global telegram_user_info
    telegram_user = telegram_user_info
    auth_state = telegram_auth_state
    
    # Read template from file
    template_path = os.path.join(os.path.dirname(__file__), 'templates', 'index.html')
    with open(template_path, 'r', encoding='utf-8') as f:
        template_content = f.read()
    
    return render_template_string(
        template_content,
        version=TDD_VERSION,
        proxy=proxy_info,
        telegram_user=telegram_user,
        auth_state=auth_state
    )


@app.route('/api/auth/status')
def api_auth_status():
    try:
        auth_state = dict(telegram_auth_state)
        auth_state['resend_available_in'] = get_auth_send_code_remaining()
        return jsonify(auth_state)
    except Exception as e:
        logger.error(f'API auth status error: {e}', exc_info=True)
        return jsonify({'error': 'Internal server error'}), 500


@app.route('/api/auth/send-code', methods=['POST'])
def api_auth_send_code():
    try:
        global web_auth_send_code, auth_send_code_cooldown_until
        if web_auth_send_code is None:
            return jsonify({'error': 'Telegram auth service is not ready yet'}), 503

        payload = request.get_json(silent=True) or {}
        phone = (payload.get('phone') or '').strip()
        if not phone:
            return jsonify({'error': 'Phone number is required'}), 400

        with auth_send_code_lock:
            remaining = get_auth_send_code_remaining()
            if remaining > 0:
                return jsonify({
                    'error': f'Please wait {remaining} seconds before requesting a new code.',
                    'resend_available_in': remaining,
                }), 429

            result = web_auth_send_code(phone)
            auth_send_code_cooldown_until = time.time() + AUTH_SEND_CODE_COOLDOWN_SECONDS

        result = dict(result)
        result['resend_available_in'] = get_auth_send_code_remaining()
        return jsonify(result)
    except Exception as e:
        logger.error(f'API auth send code error: {e}', exc_info=True)
        return jsonify({'error': str(e)}), 500


@app.route('/api/auth/verify-code', methods=['POST'])
def api_auth_verify_code():
    try:
        global web_auth_verify_code
        if web_auth_verify_code is None:
            return jsonify({'error': 'Telegram auth service is not ready yet'}), 503

        payload = request.get_json(silent=True) or {}
        phone = (payload.get('phone') or '').strip()
        code = (payload.get('code') or '').strip()
        if not phone or not code:
            return jsonify({'error': 'Phone number and code are required'}), 400

        result = web_auth_verify_code(phone, code)
        return jsonify(result)
    except Exception as e:
        logger.error(f'API auth verify code error: {e}', exc_info=True)
        return jsonify({'error': str(e)}), 500


@app.route('/api/auth/verify-password', methods=['POST'])
def api_auth_verify_password():
    try:
        global web_auth_verify_password
        if web_auth_verify_password is None:
            return jsonify({'error': 'Telegram auth service is not ready yet'}), 503

        payload = request.get_json(silent=True) or {}
        password = payload.get('password') or ''
        if not password:
            return jsonify({'error': 'Password is required'}), 400

        result = web_auth_verify_password(password)
        return jsonify(result)
    except Exception as e:
        logger.error(f'API auth verify password error: {e}', exc_info=True)
        return jsonify({'error': str(e)}), 500

@app.route('/api/status')
def api_status():
    try:
        global start_time, web_in_progress, web_queue_items, telegram_channel_info
        
        # Calculate uptime
        uptime_seconds = int(time.time() - start_time)
        hours, remainder = divmod(uptime_seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        uptime = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
        
        # Get total historical tasks count (TTL 缓存，避免每次 /api/status 都 COUNT(*))
        total_tasks = get_total_tasks_count()

        total_download_speed_mbps = 0.0
        for task_info in web_in_progress.values():
            speed_bps = task_info.get('speed_bps', 0.0)
            if isinstance(speed_bps, (int, float)) and speed_bps > 0:
                total_download_speed_mbps += speed_bps / (1024 * 1024)
        
        return jsonify({
            'uptime': uptime,
            'active_downloads': len(web_in_progress),
            'total_download_speed_mbps': round(total_download_speed_mbps, 2),
            'queue_size': len(web_queue_items),
            'version': TDD_VERSION,
            'channel_id': channel_id,
            'channel_info': telegram_channel_info,
            'total_tasks': total_tasks,
            'authorized': telegram_auth_state.get('authorized', False),
            'telegram_user': telegram_user_info,
        })
    except Exception as e:
        logger.error(f'API status error: {e}', exc_info=True)
        return jsonify({'error': 'Internal server error'}), 500

@app.route('/api/tasks')
def api_tasks():
    try:
        global web_in_progress, web_queue_items
        
        tasks = []
        
        # Add active downloads
        for task_id, task_info in web_in_progress.items():
            filename = task_info.get('filename', 'unknown')
            progress = task_info.get('progress', '0 % (0 / 0)')
            size = task_info.get('size', 0)
            source_message_link = task_info.get('source_message_link', '')
            started_at = task_info.get('started_at')
            speed_bps = task_info.get('speed_bps', 0.0)

            tasks.append({
                'task_id': str(task_id),
                'filename': filename,
                'status': 'paused' if task_info.get('paused') else 'downloading',
                'progress': progress,
                # 用任务首次入 in_progress 时记录的真实开始时间，而不是请求时刻
                'downloadTime': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(started_at)) if started_at else None,
                'started_at': started_at,
                'size': size,
                'speed_bps': 0 if task_info.get('paused') else speed_bps,
                'source_message_link': source_message_link
            })
        
        # Add queued items
        for item in web_queue_items:
            event = item[0]
            filename = getFilename(event)
            task_id = str(item[3]) if len(item) > 3 and item[3] is not None else ''
            queued_at = item[4] if len(item) > 4 and isinstance(item[4], (int, float)) else None
            # 走统一的口径：照片有 5 种 PhotoSize 变体，只看 document 的话
            # 排队中的照片会一直显示 0 字节（见 photo_size_byte_count）
            size = get_message_media_size(event)

            tasks.append({
                'task_id': task_id,
                'filename': filename,
                'status': 'queued',
                'progress': 'Waiting for download',
                'downloadTime': None,
                'size': size,
                'source_message_link': build_message_link(event),
                'queued_at': queued_at,
                'queue_age_seconds': int(max(time.time() - queued_at, 0)) if queued_at else None
            })
        
        return jsonify({'tasks': tasks})
    except Exception as e:
        logger.error(f'API tasks error: {e}', exc_info=True)
        return jsonify({'error': 'Internal server error'}), 500

@app.route('/api/history')
def api_history():
    try:
        # Get pagination parameters（带边界校验：防止 per_page=0 触发除零、
        # per_page<0 让 SQLite 返回全表，以及超大 per_page 拖垮查询）
        page, per_page, offset = normalize_pagination(
            request.args.get('page'),
            request.args.get('per_page'),
            default_per_page=10,
            max_per_page=200,
        )

        # Get filter parameters
        filename = request.args.get('filename', None)
        file_type = request.args.get('file_type', None)
        status = request.args.get('status', None)
        sort_by = request.args.get('sort_by', 'start_time', type=str)
        sort_dir = request.args.get('sort_dir', 'desc', type=str)

        # Create a new connection for this request to ensure thread safety
        local_conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        local_cursor = local_conn.cursor()
        
        # Build WHERE clause for filters
        where_clause = ""
        params = []
        
        if filename:
            where_clause += " AND filename LIKE ?"
            params.append(f"%{filename}%")
        
        if file_type:
            where_clause += " AND file_type = ?"
            params.append(file_type)
        
        if status:
            where_clause += " AND status = ?"
            params.append(status)
        
        # Remove leading AND if where_clause is not empty
        if where_clause:
            where_clause = " WHERE " + where_clause[5:]
        
        # Get total count with filters (TTL-cached by filter hash)
        total = get_cached_history_count(where_clause, tuple(params))
        
        allowed_sort_columns = {
            'time': 'start_time',
            'start_time': 'start_time',
            'size': 'size',
            'status': 'status',
            'name': 'filename',
            'filename': 'filename',
            'type': 'file_type',
            'progress': 'progress'
        }
        sort_column = allowed_sort_columns.get((sort_by or 'start_time').lower(), 'start_time')
        sort_direction = 'ASC' if (sort_dir or '').lower() == 'asc' else 'DESC'

        # Get historical downloads with filters
        select_query = f'''
        SELECT id, filename, file_type, status, size, progress, download_path, thumbnail_path, retry_count,
               source_channel_id, source_message_id, source_message_link, target_dir, start_time, end_time, error_message
        FROM downloads
        {where_clause}
        ORDER BY {sort_column} {sort_direction}, id DESC
        LIMIT ? OFFSET ?
        '''
        
        # Add pagination params
        query_params = params + [per_page, offset]
        local_cursor.execute(select_query, query_params)
        rows = local_cursor.fetchall()
        
        # 磁盘文件可能已被清理（如手动删除 /downloads），但 DB 记录仍在并引用旧路径。
        # 这里按实际存在性修正返回值，避免前端继续请求已不存在的缩略图/预览而刷一堆 404。
        def _path_exists_within(stored_path):
            if not stored_path:
                return False
            try:
                return os.path.exists(ensure_existing_path_within(downloadFolder, stored_path))
            except Exception:
                return False

        # Format response
        history = []
        for row in rows:
            download_path = row[6]
            thumbnail_path = row[7]
            thumb_exists = _path_exists_within(thumbnail_path)
            history.append({
                'id': row[0],
                'filename': row[1],
                'file_type': row[2],
                'status': row[3],
                'size': row[4],
                'progress': row[5],
                'download_path': download_path,
                # 缩略图文件不在了就置空，前端据此不再发起 /api/thumbnail 请求
                'thumbnail_path': thumbnail_path if thumb_exists else None,
                # 主文件是否仍在磁盘上，供前端决定要不要显示预览/下载入口
                'file_exists': _path_exists_within(download_path),
                'retry_count': row[8],
                'source_channel_id': row[9],
                'source_message_id': row[10],
                'source_message_link': row[11],
                'target_dir': row[12],
                'start_time': row[13],
                'end_time': row[14],
                'error_message': row[15]
            })
        
        # Close the local connection
        local_cursor.close()
        local_conn.close()
        
        return jsonify({
            'history': history,
            'total': total,
            'page': page,
            'per_page': per_page,
            'pages': compute_total_pages(total, per_page),
            'sort_by': sort_column,
            'sort_dir': sort_direction.lower()
        })
    except Exception as e:
        logger.error(f'API history error: {e}', exc_info=True)
        return jsonify({'error': 'Internal server error'}), 500

@app.route('/api/download')
def api_download():
    try:
        # Get parameters
        task_id = request.args.get('task_id', type=str)
        filename = request.args.get('filename', type=str)
        delete_file = request.args.get('delete_file', default='1', type=str) != '0'
        
        if not task_id or not filename:
            return jsonify({'error': 'Missing task_id or filename parameter'}), 400
        
        # Extract actual task id from task_id string (e.g., "history-123" -> "123")
        actual_task_id = task_id.split('-')[-1]
        
        # Create a new connection for this request to ensure thread safety
        local_conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        local_cursor = local_conn.cursor()
        
        # Get file path from database
        local_cursor.execute('SELECT download_path, status FROM downloads WHERE id = ?', (actual_task_id,))
        result = local_cursor.fetchone()
        local_cursor.close()
        local_conn.close()

        if not result:
            return jsonify({'error': 'File not found in database'}), 404

        download_path_value, status_value = result
        if not download_path_value:
            return jsonify({'error': 'File is not available for download yet'}), 409

        # 仅允许下载已完成的文件；否则拉到的可能是 tmp 过程中的残缺文件
        if status_value != 'completed':
            return jsonify({'error': f'File is not ready to download (status={status_value})'}), 409

        file_path = ensure_existing_path_within(downloadFolder, download_path_value)

        # Check if file exists
        if not os.path.exists(file_path):
            return jsonify({'error': 'File not found on disk'}), 404

        # Send the file
        return send_file(file_path, as_attachment=True, download_name=os.path.basename(file_path))
    except Exception as e:
        logger.error(f'API download error: {e}', exc_info=True)
        return jsonify({'error': 'Internal server error'}), 500

@app.route('/api/retry', methods=['POST'])
def api_retry():
    try:
        global web_retry_scheduler

        task_id = request.args.get('task_id', type=str)
        if not task_id:
            return jsonify({'error': 'Missing task_id parameter'}), 400

        actual_task_id = task_id.split('-')[-1]

        local_conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        local_cursor = local_conn.cursor()
        retry_dir = request.args.get('retry_dir', type=str)
        resolved_retry_dir = resolve_retry_directory(retry_dir) if retry_dir else None

        local_cursor.execute(
            'SELECT filename, source_channel_id, source_message_id, source_message_link, download_path, target_dir FROM downloads WHERE id = ?',
            (actual_task_id,)
        )
        result = local_cursor.fetchone()
        local_cursor.close()
        local_conn.close()

        if not result:
            return jsonify({'error': 'File not found in database'}), 404

        filename, source_channel_id, source_message_id, source_message_link, download_path, target_dir = result
        if not source_channel_id or not source_message_id:
            return jsonify({'error': 'This task does not have source message metadata for retry'}), 400

        if web_retry_scheduler is None:
            return jsonify({'error': 'Retry service is not ready yet'}), 503

        # 复用旧记录（将其状态改回 queued），避免每次重试都产生新的历史行
        retry_result = web_retry_scheduler(
            int(source_channel_id),
            int(source_message_id),
            resolved_retry_dir,
            int(actual_task_id),
        )
        return jsonify({
            'success': True,
            'message': f'Retry queued for {filename}',
            'filename': retry_result.get('filename', filename),
            'source_message_link': source_message_link,
            'retry_dir': resolved_retry_dir or target_dir or (os.path.dirname(download_path) if download_path else '')
        })
    except Exception as e:
        logger.error(f'API retry error: {e}', exc_info=True)
        return jsonify({'error': str(e)}), 500

@app.route('/api/cancel', methods=['POST'])
def api_cancel():
    """取消一个队列中的或正在下载的任务。"""
    try:
        task_id = request.args.get('task_id', type=str)
        if not task_id:
            return jsonify({'error': 'Missing task_id parameter'}), 400

        actual_task_id = task_id.split('-')[-1]
        if not actual_task_id.isdigit():
            return jsonify({'error': 'Invalid task_id'}), 400

        if web_cancel_scheduler is None:
            return jsonify({'error': 'Cancel service is not ready yet'}), 503

        result = web_cancel_scheduler(int(actual_task_id))
        if not result.get('found'):
            return jsonify({'error': 'Task is not active (already completed, failed, or never started)'}), 404
        return jsonify({'success': True, **result})
    except Exception as e:
        logger.error(f'API cancel error: {e}', exc_info=True)
        return jsonify({'error': str(e)}), 500


@app.route('/api/pause', methods=['POST'])
def api_pause():
    """暂停一个正在下载的任务。"""
    try:
        task_id = request.args.get('task_id', type=str)
        if not task_id:
            return jsonify({'error': 'Missing task_id parameter'}), 400

        actual_task_id = task_id.split('-')[-1]
        if not actual_task_id.isdigit():
            return jsonify({'error': 'Invalid task_id'}), 400

        if web_pause_scheduler is None:
            return jsonify({'error': 'Pause service is not ready yet'}), 503

        result = web_pause_scheduler(int(actual_task_id))
        if not result.get('found'):
            return jsonify({'error': 'Task is not active or already paused'}), 404
        return jsonify({'success': True, **result})
    except Exception as e:
        logger.error(f'API pause error: {e}', exc_info=True)
        return jsonify({'error': str(e)}), 500


@app.route('/api/resume', methods=['POST'])
def api_resume():
    """恢复一个已暂停的任务。"""
    try:
        task_id = request.args.get('task_id', type=str)
        if not task_id:
            return jsonify({'error': 'Missing task_id parameter'}), 400

        actual_task_id = task_id.split('-')[-1]
        if not actual_task_id.isdigit():
            return jsonify({'error': 'Invalid task_id'}), 400

        if web_resume_scheduler is None:
            return jsonify({'error': 'Resume service is not ready yet'}), 503

        result = web_resume_scheduler(int(actual_task_id))
        if not result.get('found'):
            return jsonify({'error': 'Task is not paused'}), 404
        return jsonify({'success': True, **result})
    except Exception as e:
        logger.error(f'API resume error: {e}', exc_info=True)
        return jsonify({'error': str(e)}), 500


@app.route('/api/delete', methods=['DELETE'])
def api_delete():
    try:
        # Get parameters
        task_id = request.args.get('task_id', type=str)
        filename = request.args.get('filename', type=str)
        delete_file = request.args.get('delete_file', default='1', type=str) != '0'

        if not task_id or not filename:
            return jsonify({'error': 'Missing task_id or filename parameter'}), 400

        # Extract actual task id from task_id string (e.g., "history-123" -> "123")
        actual_task_id = task_id.split('-')[-1]

        # 不允许直接删除正在下载 / 排队中的任务，避免把 worker 脚下的地抽掉
        if actual_task_id in web_in_progress or actual_task_id in active_queue_items_by_id:
            return jsonify({
                'error': 'Task is still active. Cancel it first before deleting.',
            }), 409

        # Create a new connection for this request to ensure thread safety
        local_conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        local_cursor = local_conn.cursor()

        # Get file paths from database
        local_cursor.execute('SELECT download_path, thumbnail_path, filename, status FROM downloads WHERE id = ?', (actual_task_id,))
        result = local_cursor.fetchone()

        if not result:
            local_cursor.close()
            local_conn.close()
            return jsonify({'error': 'File not found in database'}), 404

        download_path, thumbnail_path, stored_filename, stored_status = result

        # 二次兜底：即使 web_in_progress 没跟上，也拒绝删 downloading / queued
        if stored_status in ('downloading', 'queued', 'paused'):
            local_cursor.close()
            local_conn.close()
            return jsonify({
                'error': f'Task is still active (status={stored_status}). Cancel it first before deleting.',
            }), 409

        if delete_file and download_path:
            file_path = ensure_existing_path_within(downloadFolder, download_path)
            if os.path.exists(file_path):
                os.remove(file_path)
                logger.info(f'Deleted file: {file_path}')

        if delete_file and thumbnail_path:
            safe_thumbnail_path = ensure_existing_path_within(downloadFolder, thumbnail_path)
            if os.path.exists(safe_thumbnail_path):
                os.remove(safe_thumbnail_path)
                logger.info(f'Deleted thumbnail: {safe_thumbnail_path}')

        if delete_file:
            cleanup_temp_file_for_filename(stored_filename or filename)
        
        # Delete record from database
        local_cursor.execute('DELETE FROM downloads WHERE id = ?', (actual_task_id,))
        local_conn.commit()
        logger.info(f'Deleted download record: {actual_task_id}')
        # 让 total_tasks 缓存下一次读取刷新
        invalidate_total_tasks_count()

        # Close the local connection
        local_cursor.close()
        local_conn.close()
        
        return jsonify({
            'success': True,
            'message': 'File and record deleted successfully' if delete_file else 'Record deleted successfully'
        })
    except Exception as e:
        logger.error(f'API delete error: {e}', exc_info=True)
        return jsonify({'error': 'Internal server error'}), 500

@app.route('/api/rename', methods=['POST'])
def api_rename():
    try:
        # Get parameters
        task_id = request.args.get('task_id', type=str)
        new_filename = request.args.get('new_filename', type=str)
        
        if not task_id or not new_filename:
            return jsonify({'error': 'Missing task_id or new_filename parameter'}), 400
        
        # Extract actual task id from task_id string (e.g., "history-123" -> "123")
        actual_task_id = task_id.split('-')[-1]
        
        # Create a new connection for this request to ensure thread safety
        local_conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        local_cursor = local_conn.cursor()
        
        # Get file path from database
        local_cursor.execute('SELECT download_path FROM downloads WHERE id = ?', (actual_task_id,))
        result = local_cursor.fetchone()
        
        if not result:
            local_cursor.close()
            local_conn.close()
            return jsonify({'error': 'File not found in database'}), 404
        
        old_file_path = ensure_existing_path_within(downloadFolder, result[0])
        
        # Check if file exists
        if not os.path.exists(old_file_path):
            local_cursor.close()
            local_conn.close()
            return jsonify({'error': 'File not found on disk'}), 404
        
        # Get directory path and extension
        dir_path = os.path.dirname(old_file_path)
        safe_new_filename = sanitize_filename(new_filename)
        old_extension = os.path.splitext(old_file_path)[1]
        new_extension = os.path.splitext(safe_new_filename)[1]
        if old_extension and not new_extension:
            safe_new_filename = f"{safe_new_filename}{old_extension}"

        # Create new file path with same extension
        new_file_path = build_safe_path(dir_path, safe_new_filename)
        if os.path.exists(new_file_path):
            local_cursor.close()
            local_conn.close()
            return jsonify({'error': 'Target filename already exists'}), 409

        old_thumbnail_path = None
        thumbnail_dir = os.path.join(dir_path, '.thumbnails')
        candidate_thumbnail = build_safe_path(thumbnail_dir, os.path.basename(old_file_path) + '.jpg')
        if os.path.exists(candidate_thumbnail):
            old_thumbnail_path = candidate_thumbnail
        new_thumbnail_path = build_safe_path(thumbnail_dir, os.path.basename(new_file_path) + '.jpg') if old_thumbnail_path else None
        
        # Rename file on disk
        os.rename(old_file_path, new_file_path)
        logger.info(f'Renamed file: {old_file_path} -> {new_file_path}')

        if old_thumbnail_path and new_thumbnail_path:
            os.rename(old_thumbnail_path, new_thumbnail_path)
            logger.info(f'Renamed thumbnail: {old_thumbnail_path} -> {new_thumbnail_path}')
        
        # Update paths in database
        local_cursor.execute(
            'UPDATE downloads SET filename = ?, download_path = ?, thumbnail_path = ? WHERE id = ?',
            (safe_new_filename, new_file_path, new_thumbnail_path, actual_task_id)
        )
        local_conn.commit()
        logger.info(f'Updated download record filename: {actual_task_id} -> {safe_new_filename}')
        
        # Close the local connection
        local_cursor.close()
        local_conn.close()
        
        return jsonify({'success': True, 'message': 'File renamed successfully'})
    except Exception as e:
        logger.error(f'API rename error: {e}', exc_info=True)
        return jsonify({'error': 'Internal server error'}), 500

@app.route('/api/thumbnail')
def api_thumbnail():
    """获取缩略图"""
    try:
        task_id = request.args.get('task_id', type=str)
        if not task_id:
            return jsonify({'error': 'Missing task_id parameter'}), 400
        
        actual_task_id = task_id.split('-')[-1]
        
        local_conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        local_cursor = local_conn.cursor()
        local_cursor.execute('SELECT thumbnail_path FROM downloads WHERE id = ?', (actual_task_id,))
        result = local_cursor.fetchone()
        local_cursor.close()
        local_conn.close()
        
        if not result or not result[0]:
            return jsonify({'error': 'Thumbnail not found'}), 404
        
        thumbnail_path = ensure_existing_path_within(downloadFolder, result[0])
        if not os.path.exists(thumbnail_path):
            return jsonify({'error': 'Thumbnail file not found'}), 404
        
        return send_file(thumbnail_path, mimetype='image/jpeg')
    except Exception as e:
        logger.error(f'API thumbnail error: {e}', exc_info=True)
        return jsonify({'error': 'Internal server error'}), 500

@app.route('/api/image-preview')
def api_image_preview():
    """Return the original image when available, otherwise fall back to the thumbnail."""
    try:
        task_id = request.args.get('task_id', type=str)
        if not task_id:
            return jsonify({'error': 'Missing task_id parameter'}), 400

        actual_task_id = task_id.split('-')[-1]

        local_conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        local_cursor = local_conn.cursor()
        local_cursor.execute('SELECT download_path, thumbnail_path FROM downloads WHERE id = ?', (actual_task_id,))
        result = local_cursor.fetchone()
        local_cursor.close()
        local_conn.close()

        if not result:
            return jsonify({'error': 'Preview not found'}), 404

        download_path, thumbnail_path = result

        if download_path:
            safe_download_path = ensure_existing_path_within(downloadFolder, download_path)
            if os.path.exists(safe_download_path):
                guessed_type, _ = guess_type(safe_download_path)
                if guessed_type and guessed_type.startswith('image/'):
                    return send_file(safe_download_path, mimetype=guessed_type)

        if thumbnail_path:
            safe_thumbnail_path = ensure_existing_path_within(downloadFolder, thumbnail_path)
            if os.path.exists(safe_thumbnail_path):
                return send_file(safe_thumbnail_path, mimetype='image/jpeg')

        return jsonify({'error': 'Preview file not found'}), 404
    except Exception as e:
        logger.error(f'API image preview error: {e}', exc_info=True)
        return jsonify({'error': 'Internal server error'}), 500

@app.route('/api/media-preview')
def api_media_preview():
    """Return the original image/video inline for lightweight browser preview."""
    try:
        task_id = request.args.get('task_id', type=str)
        if not task_id:
            return jsonify({'error': 'Missing task_id parameter'}), 400

        actual_task_id = task_id.split('-')[-1]

        local_conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        local_cursor = local_conn.cursor()
        local_cursor.execute('SELECT download_path FROM downloads WHERE id = ?', (actual_task_id,))
        result = local_cursor.fetchone()
        local_cursor.close()
        local_conn.close()

        if not result or not result[0]:
            return jsonify({'error': 'Preview not found'}), 404

        file_path = ensure_existing_path_within(downloadFolder, result[0])
        if not os.path.exists(file_path):
            return jsonify({'error': 'Preview file not found'}), 404

        guessed_type, _ = guess_type(file_path)
        if not guessed_type or (
            not guessed_type.startswith('image/')
            and not guessed_type.startswith('video/')
            and not guessed_type.startswith('audio/')
        ):
            return jsonify({'error': 'Unsupported preview media type'}), 415

        response = send_file(file_path, mimetype=guessed_type, as_attachment=False, conditional=True)
        response.headers['Accept-Ranges'] = 'bytes'
        response.headers['Cache-Control'] = 'public, max-age=3600'
        return response
    except Exception as e:
        logger.error(f'API media preview error: {e}', exc_info=True)
        return jsonify({'error': 'Internal server error'}), 500

# Web Server Thread Function
def run_web_server():
    logger.info("Starting web server on http://0.0.0.0:7373")
    while True:
        try:
            socketio.run(app, host='0.0.0.0', port=7373, debug=False, use_reloader=False, allow_unsafe_werkzeug=True)
            logger.info("Web server stopped")
            break
        except Exception as e:
            logger.error(f"Web server error: {e}", exc_info=True)
            logger.info("Restarting web server in 5 seconds...")
            time.sleep(5)

async def log_premium_status(client: TelegramClient) -> None:
    """启动时打印当前登录账号是否为 Telegram Premium，便于确认高速下载权益是否可用。"""
    try:
        me = await client.get_me()
    except Exception as exc:
        logger.warning("Could not determine account Premium status: %s", exc)
        return

    is_premium = bool(getattr(me, "premium", False))
    username = getattr(me, "username", None) or getattr(me, "id", "unknown")
    if is_premium:
        logger.info(
            "Logged-in account @%s is Telegram Premium — high-speed download quota available.",
            username,
        )
    else:
        logger.warning(
            "Logged-in account @%s is NOT Telegram Premium; downloads are capped at the free quota "
            "regardless of parallel connections.",
            username,
        )

    if parallel_connections > 1:
        logger.info(
            "Parallel download enabled: %d connections per file for files >= %d MB.",
            parallel_connections, parallel_min_size_bytes // (1024 * 1024),
        )
    else:
        logger.info(
            "Parallel download disabled (TELEGRAM_DAEMON_PARALLEL_CONNECTIONS=1); using a single "
            "connection per file. Telegram throttles per connection, so raising this is the main "
            "lever on download speed.")


async def sendHelloMessage(client: TelegramClient, peerChannel: PeerChannel) -> None:
    entity = await client.get_entity(peerChannel)
    print(f"Telegram Download Daemon {TDD_VERSION} using Telethon {__version__}")
    print(f"  Simultaneous downloads: {worker_count}")
    await log_premium_status(client)
    await client.send_message(entity, f"Telegram Download Daemon {TDD_VERSION} using Telethon {__version__}")
    await client.send_message(entity, "Hi! Ready for your files!")


def build_client(loop):
    return TelegramClient(getSession(), api_id, api_hash, proxy=proxy, loop=loop)


def connect_client_with_recovery(loop):
    global client, web_client

    client = build_client(loop)
    try:
        loop.run_until_complete(client.connect())
        web_client = client
        return None
    except AuthKeyDuplicatedError:
        message = handle_auth_key_duplicated_recovery()
        with contextlib.suppress(Exception):
            loop.run_until_complete(client.disconnect())

        client = build_client(loop)
        loop.run_until_complete(client.connect())
        web_client = client
        logger.info("Telegram client reinitialized with a fresh session; waiting for web login")
        return message


def disconnect_client_and_loop(loop, client_instance):
    if loop is None:
        return

    if client_instance is not None:
        with contextlib.suppress(Exception):
            if not loop.is_closed():
                loop.run_until_complete(client_instance.disconnect())

    if not loop.is_closed():
        loop.close()


async def bounded_notify(rpc_coro, what: str):
    """通知类 RPC 的保险丝：超时/失败一律丢弃通知并返回 None。

    主连接陷入重连风暴时，Telethon 的 pending 请求会无限挂起而不抛错——
    2026-08-14 生产上两个 worker 就冻死在重试通知的 message.edit() 上，
    队列从此永远 "Waiting for download"。通知发不出去只能丢，绝不允许拖死
    调用方的流水线。主动取消（CancelledError）照常向上抛，暂停/取消机制不受影响。
    """
    try:
        return await asyncio.wait_for(rpc_coro, timeout=TELEGRAM_DAEMON_NOTIFY_RPC_TIMEOUT)
    except Exception as notify_error:
        logger.warning(f"Notification RPC dropped ({what}): {notify_error}")
        return None


async def log_reply(message: events.NewMessage.Event, reply: str) -> None:
    print(reply)
    if message is not None:
        await bounded_notify(message.edit(reply), "edit status message")


def compute_download_timeout(size):
    """单文件的总超时。

    一刀切的 3600 秒会把"正常但慢"的大文件误杀——文件越大越必然超时，而重试又
    从 0 开始，于是永远下不完。这里改成按体积折算：给每个文件至少
    ``size / min_speed_bps`` 的时间，并以 ``download_timeout`` 为下限。

    真正卡死的下载不靠这个判定，由 ``no_progress_timeout``（默认 5 分钟没有任何
    字节进来就砍）负责，那才是"卡住"的正确判据。
    """
    try:
        size = int(size or 0)
    except (TypeError, ValueError):
        size = 0
    if size <= 0:
        return download_timeout
    return max(download_timeout, math.ceil(size / min_speed_bps))


def resumable_offset(temp_path, size, part_size_hint=None):
    """已下载的字节数中可以安全续传的那部分（向下对齐到分块边界）。

    Telegram 只接受对齐到分块大小的 offset，所以把尾巴上不足一块的部分丢掉重下。
    返回 0 表示没有可用的断点，应当从头下载。
    """
    if not resume_enabled or not temp_path or not os.path.exists(temp_path):
        return 0
    try:
        downloaded = os.path.getsize(temp_path)
    except OSError:
        return 0
    try:
        size = int(size or 0)
    except (TypeError, ValueError):
        size = 0
    # 大小不明或者临时文件比目标还大（元信息对不上）时不冒险续传。
    if size <= 0 or downloaded <= 0 or downloaded >= size:
        return 0
    part_size = part_size_hint or choose_part_size(size)
    return align_down(downloaded, part_size)


async def download_media_dispatch(message_obj, temp_path, progress_callback,
                                  resume_from=0, refresh_document=None):
    """下载媒体到 ``temp_path``，可选地从 ``resume_from`` 字节处续传。

    Document 类的媒体走本项目自己的分块下载器（``fast_download``）：它支持任意
    起始偏移量，而且能开多连接——Telegram 的下载限速按连接计，单连接就是天花板。
    连接数为 1 时它同样能跑，只是退化成单连接，好处是仍然支持续传，并且用的是本次
    下载专属的连接，而不是和其它 worker 抢 Telethon 那条共享的主连接。

    照片、缩略图等非 Document 媒体，以及并行路径出错时，回退到 ``client.download_media``。
    该函数不支持续传，回退时会把半成品清掉从头下。
    """
    document = get_parallel_location(message_obj)
    document_size = int(getattr(document, "size", 0)) if document is not None else 0
    # 续传只有分块下载器做得到，所以一旦有断点就无条件走它，不看体积阈值。
    use_chunked = document is not None and (
        resume_from > 0
        or (parallel_connections > 1 and document_size >= parallel_min_size_bytes)
    )

    if use_chunked:
        try:
            if resume_from > 0:
                # 'r+b' + seek + truncate：把不足一整块的尾巴切掉，从对齐的位置续写。
                with open(temp_path, "r+b") as out:
                    out.seek(resume_from)
                    out.truncate()
                    await fast_download_file(
                        client, document, out,
                        progress_callback=progress_callback,
                        connection_count=parallel_connections,
                        start_offset=resume_from,
                        refresh_document=refresh_document,
                    )
            else:
                with open(temp_path, "wb") as out:
                    await fast_download_file(
                        client, document, out,
                        progress_callback=progress_callback,
                        connection_count=parallel_connections,
                        refresh_document=refresh_document,
                    )
            return temp_path
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            downloaded_now = 0
            with contextlib.suppress(OSError):
                if os.path.exists(temp_path):
                    downloaded_now = os.path.getsize(temp_path)

            # 分块路径已经写进去的字节是有价值的：把错误原样抛出去，让 worker 的
            # 重试逻辑下一轮从这里**续传**。
            #
            # 老逻辑无条件删半成品再从 0 跑原生下载器，于是一个 2GB 的文件每撞上
            # 一次瞬时断连就前功尽弃；而且原生下载器不支持 offset，重试时
            # `progressed_bytes = getsize(temp) - resume_from` 会算成 0，
            # 这一轮还要白白消耗一次重试次数，3 次之后直接判死刑。
            # 下载耗时越长越必然撞上——这就是"大文件总是失败"的另一半原因。
            #
            # CDN 重定向是例外：分块下载器处理不了，再重试多少次都是同一个结果，
            # 必须清掉半成品交给原生下载器。
            if (resume_enabled
                    and not isinstance(exc, CdnRedirectNeeded)
                    and downloaded_now > resume_from):
                logger.warning(
                    "Chunked download failed at %d bytes (%s); keeping the partial file so the "
                    "next attempt resumes instead of restarting",
                    downloaded_now, exc,
                )
                raise

            logger.warning(
                "Chunked download failed (%s); falling back to the native downloader for %s",
                exc, temp_path,
            )
            # 原生下载器不支持续传，半成品留着只会被当成完整文件，必须清掉。
            with contextlib.suppress(Exception):
                if os.path.exists(temp_path):
                    os.remove(temp_path)

    return await client.download_media(
        message_obj, temp_path, progress_callback=progress_callback)

# NOTE: getRandomId / _WINDOWS_RESERVED_NAMES / sanitize_filename /
# build_safe_path / ensure_existing_path_within 已迁移到 tdd_utils 模块，
# 便于单元测试。此处保持 import 别名，行为与之前完全一致。


def resolve_retry_directory(target_dir: str | None) -> str | None:
    if not target_dir:
        return None

    candidate = os.path.abspath(target_dir)
    download_root = os.path.abspath(downloadFolder)
    if os.path.commonpath([download_root, candidate]) != download_root:
        raise ValueError(f"Retry path must stay inside download root: {download_root}")
    return candidate


def get_message_object(message_or_event):
    if hasattr(message_or_event, 'original_update') and hasattr(message_or_event, 'message'):
        return message_or_event.message
    return message_or_event


def get_source_channel_id(message_or_event) -> int:
    message_obj = get_message_object(message_or_event)
    peer = getattr(message_obj, 'peer_id', None)
    if peer and hasattr(peer, 'channel_id'):
        return peer.channel_id
    return channel_id


def build_message_link(message_or_event) -> str:
    message_obj = get_message_object(message_or_event)
    message_id = getattr(message_obj, 'id', None)
    if not message_id:
        return ""

    chat = getattr(message_obj, 'chat', None)
    username = getattr(chat, 'username', None)
    if username:
        return f"https://t.me/{username}/{message_id}"

    source_channel_id = get_source_channel_id(message_obj)
    return f"https://t.me/c/{source_channel_id}/{message_id}"


def extract_entity_urls(message_obj) -> list:
    """把消息实体里的 URL 抽出来。

    覆盖两种情况：``MessageEntityTextUrl``（显示文字是中文/表情，真链接藏在实体里）
    和 ``MessageEntityUrl``（裸链接，实体只给出正文里的 offset/length）。后者其实
    正则也能扫到，这里一并取出是为了兼容表情/代理字符导致偏移的极端文本。
    """
    urls = []
    entities = getattr(message_obj, 'entities', None) or []
    raw_text = getattr(message_obj, 'message', '') or ''

    for entity in entities:
        if isinstance(entity, MessageEntityTextUrl):
            if entity.url:
                urls.append(entity.url)
        elif isinstance(entity, MessageEntityUrl):
            # Telegram 的 offset/length 以 UTF-16 码元计，直接用字符下标会被 emoji 顶偏
            sliced = utf16_slice(raw_text, entity.offset, entity.length)
            if sliced:
                urls.append(sliced)
    return urls


def extract_telegram_links(message_obj, max_messages: int) -> list:
    """从一条消息里解析出所有可下载的 Telegram 消息链接。"""
    message_obj = get_message_object(message_obj)
    text = getattr(message_obj, 'message', '') or ''
    if not text:
        return []
    return parse_telegram_links(
        text,
        extra_urls=extract_entity_urls(message_obj),
        max_range=max_messages,
    )


def getFilename(message_or_event) -> str:
    message_obj = get_message_object(message_or_event)
    mediaFileName = "unknown"

    if getattr(message_obj, 'photo', None):
        mediaFileName = f"{message_obj.photo.id}.jpeg"
    elif getattr(message_obj, 'document', None):
        # 优先使用文件名属性
        for attribute in message_obj.document.attributes:
            if isinstance(attribute, DocumentAttributeFilename): 
                mediaFileName = attribute.file_name
                break      
        # 如果没有文件名属性，尝试使用其他方式
        if mediaFileName == "unknown":
            if getattr(message_obj, 'message', '') != '':
                mediaFileName = message_obj.message
            else:    
                mediaFileName = str(message_obj.document.id)
            # 添加适当的扩展名
            extension = guess_extension(message_obj.document.mime_type)
            if extension:
                mediaFileName += extension
    
    return sanitize_filename(mediaFileName)

def photo_size_byte_count(photo_size) -> int:
    """一个 PhotoSize 变体下载下来会占多少字节。

    **必须和 Telethon 的口径完全一致**，否则下载完成后的大小校验会拿错数字去比，
    照片会被无辜判成失败（2026-08-15 就是这么挂的）。Telegram 的 5 个变体里
    只有 ``PhotoSize`` 有 ``size`` 字段，别的各有各的算法：

    - ``PhotoSize``            → ``size``（int）
    - ``PhotoSizeProgressive`` → ``sizes``（渐进式 JPEG 的分级大小**列表**），取 max；
      **这个变体没有 size 字段**，而它往往正是分辨率最高、Telethon 真正会下载的那个
    - ``PhotoCachedSize``      → ``bytes`` 就是图片内容本身
    - ``PhotoStrippedSize``    → ``bytes`` 是掐头去尾的缩略图，下载时会补回标准
      JPEG 头尾，正好 622 字节（首字节为 1 才是这种压缩格式）
    - ``PhotoPathSize``        → ``bytes`` 是动画贴纸轮廓的 SVG 路径，不是图片，
      Telethon 会把它排除掉，这里同样记 0

    对应 telethon/utils.py::_photo_size_byte_count 和
    telethon/client/downloads.py::_get_thumb / _download_photo。
    用 getattr 而不是 isinstance，是为了让测试能用简单替身，也不受 Telethon 版本影响。
    """
    # PhotoSizeProgressive：注意字段是复数 sizes，且没有 size
    progressive_sizes = getattr(photo_size, 'sizes', None)
    if isinstance(progressive_sizes, (list, tuple)):
        numeric = [int(item) for item in progressive_sizes if isinstance(item, int)]
        if numeric:
            return max(numeric)

    candidate_size = getattr(photo_size, 'size', None)
    if isinstance(candidate_size, int):
        return candidate_size

    raw_bytes = getattr(photo_size, 'bytes', None)
    if isinstance(raw_bytes, (bytes, bytearray)):
        if type(photo_size).__name__ == 'PhotoPathSize':
            return 0
        if len(raw_bytes) >= 3 and raw_bytes[0] == 1:
            return len(raw_bytes) + 622
        return len(raw_bytes)

    return 0


def get_message_media_size(message_or_event) -> int:
    message_obj = get_message_object(message_or_event)

    if getattr(message_obj, 'document', None) and getattr(message_obj.document, 'size', None):
        return int(message_obj.document.size)

    if getattr(message_obj, 'photo', None):
        # Telethon 下载的是"字节数最大"的那个变体（_get_thumb 按字节数排序取末位），
        # 这里必须用同一口径挑同一个变体，否则校验必然对不上。
        return max(
            (photo_size_byte_count(photo_size)
             for photo_size in getattr(message_obj.photo, 'sizes', []) or []),
            default=0
        )

    return 0


# 移除全局变量，将在 start 函数内部管理状态


try:
    logger.info(f"Starting Telegram Download Daemon v{TDD_VERSION}")
    logger.info(f"Using Telethon v{__version__}")
    logger.info(f"API ID: {api_id}, Channel ID: {channel_id}")
    logger.info(f"Download folder: {downloadFolder}, Temp folder: {tempFolder}")
    logger.info(f"Worker count: {worker_count}")
    logger.info(f"Download timeout: >= {download_timeout}s, scaled by size at {min_speed_bps} B/s (e.g. 2GB -> {compute_download_timeout(2 * 1024**3)}s), Start timeout: {start_timeout}s, No-progress timeout: {no_progress_timeout}s, Update frequency: {updateFrequency}s, Max retries: {max_retries}, Notify failure: {notify_failure}")
    logger.info(f"Resume partial downloads: {'on' if resume_enabled else 'off'} (temp files kept for {temp_max_age_seconds // 3600}h)")
    logger.info(
        "Message link download: %s (album=%s, auto-join=%s, max messages per text=%s)",
        "enabled" if link_download_enabled else "disabled",
        link_album_enabled,
        link_auto_join_enabled,
        link_max_messages,
    )
    logger.info("Session lock path: %s", getLockPath())
    acquireProcessLock()
    
    # 清理残留的临时文件
    cleanup_temp_files()
    
    # 处理中断的任务
    handle_interrupted_tasks()
    
    # Log proxy configuration
    if proxy:
        if isinstance(proxy, tuple):
            configured_endpoint = proxy_configured_host or proxy[1]
            auth_mode = 'authentication' if len(proxy) > 4 and proxy[4] else 'no authentication'
            if proxy_resolved_once and proxy_runtime_host:
                logger.info(
                    "Using proxy: %s:%s pinned to %s with %s",
                    configured_endpoint,
                    proxy[2],
                    proxy_runtime_host,
                    auth_mode,
                )
            else:
                logger.info("Using proxy: %s:%s with %s", configured_endpoint, proxy[2], auth_mode)
        else:
            logger.info(f"Using proxy: {proxy.get('addr')}:{proxy.get('port')} with {'authentication' if proxy.get('username') else 'no authentication'}")
    else:
        logger.info("No proxy configured")
    
    # Create client without interactive auth prompts
    main_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(main_loop)
    initial_auth_message = connect_client_with_recovery(main_loop)
    logger.info("Telegram client initialized successfully")
    
    # Start Web Server in a separate thread
    ensure_web_server_started()

    async def start(initial_auth_message=None):
        # 在 start 函数内部管理所有状态
        in_progress = {}
        lastUpdate = 0
        
        # 创建锁来保护共享资源
        status_lock = asyncio.Lock()
        # 用于同步回调的锁
        sync_lock = threading.Lock()
        # 用于保护 queue_items 列表的锁
        queue_lock = asyncio.Lock()
        # 用于保护数据库操作的异步锁
        db_lock = asyncio.Lock()
        # 用于保护数据库操作的同步锁（用于回调函数）
        sync_db_lock = threading.Lock()
        
        # 创建多个队列，分别用于不同类型的文件
        photo_queue = asyncio.Queue()
        video_queue = asyncio.Queue()
        other_queue = asyncio.Queue()
        
        # 三条队列里"当前可取的任务总数"。worker 先领一个名额再去取，
        # 这样就不需要三个并发的 queue.get() 相互竞争（见 pop_next_queue_item）。
        queue_slots = asyncio.Semaphore(0)

        # 为每个队列创建跟踪列表
        photo_queue_items = []
        video_queue_items = []
        other_queue_items = []

        # 合并所有队列项用于命令查询
        queue_items = []
        
        peerChannel = PeerChannel(channel_id)
        auth_ready_event = asyncio.Event()
        auth_context = {
            'phone': '',
            'phone_code_hash': '',
        }
        
        # Link web variables to local variables
        global web_in_progress, web_queue_items, telegram_user_info, telegram_channel_info
        web_in_progress = in_progress
        web_queue_items = queue_items

        def get_queue_target(message_obj):
            is_photo = getattr(message_obj, 'photo', None) is not None
            is_video = False
            if getattr(message_obj, 'document', None):
                for attribute in message_obj.document.attributes:
                    if isinstance(attribute, DocumentAttributeVideo):
                        is_video = True
                        break

            if is_photo:
                return photo_queue, photo_queue_items, 'photo'
            if is_video:
                return video_queue, video_queue_items, 'video'
            return other_queue, other_queue_items, 'other'

        def rebuild_web_queue_items():
            nonlocal queue_items
            global web_queue_items
            queue_items = photo_queue_items + video_queue_items + other_queue_items
            web_queue_items = queue_items

        async def push_queue_item(queue_item):
            target_queue, target_items, queue_type = get_queue_target(queue_item[0])
            async with queue_lock:
                await target_queue.put(queue_item)
                target_items.append(queue_item)
                # 放完货再放名额，顺序不能反：先 release 会让 worker 在
                # 队列还没写进去的时候就跑去取，取不到东西。
                queue_slots.release()
                rebuild_web_queue_items()
                # 维护 id -> queue_item 索引，便于 /api/cancel 直接定位待取出的 item
                queue_download_id = queue_item[3] if len(queue_item) > 3 else None
                if queue_download_id is not None:
                    active_queue_items_by_id[str(queue_download_id)] = queue_item
                logger.info(
                    "Queued task id=%s filename=%s queue=%s sizes(photo=%s, video=%s, other=%s)",
                    queue_download_id,
                    getFilename(queue_item[0]),
                    queue_type,
                    photo_queue.qsize(),
                    video_queue.qsize(),
                    other_queue.qsize()
                )
            return queue_type

        async def pop_next_queue_item():
            """取下一个待下载任务。

            这里**不能**用"给三条队列各起一个 get() 然后 asyncio.wait(FIRST_COMPLETED)"
            的写法。asyncio.wait 在有多个 getter 同时就绪时会把它们**全部**放进 done，
            而队列元素在 get() 返回的那一刻就已经从 asyncio.Queue 里移走了。老代码只
            取 done.pop() 一个、把其余的 result() 丢掉，于是"只要同时有两条以上队列非空，
            多出来的任务就凭空消失"——它们仍留在 queue_items / web_queue_items 里，
            数据库状态也还是 queued，页面上永远显示"排队中"，但没有任何 worker 会再碰它。
            重试路径同样是 push_queue_item，所以失败重投的任务也会这样被吞掉。

            改成计数信号量：先领一个"有货"的名额，领到就保证一定能取到东西，
            然后按固定优先级从非空队列里拿。全程没有并发的 getter，也就没有这个竞态。
            """
            await queue_slots.acquire()
            took = False
            try:
                async with queue_lock:
                    for queue_ref, queue_items_list_ref, queue_type in (
                        (photo_queue, photo_queue_items, 'photo'),
                        (video_queue, video_queue_items, 'video'),
                        (other_queue, other_queue_items, 'other'),
                    ):
                        if queue_ref.empty():
                            continue
                        element = queue_ref.get_nowait()
                        took = True
                        if element in queue_items_list_ref:
                            queue_items_list_ref.remove(element)
                        # 从"待取出"索引中剔除
                        element_download_id = element[3] if len(element) > 3 else None
                        if element_download_id is not None:
                            active_queue_items_by_id.pop(str(element_download_id), None)
                        rebuild_web_queue_items()
                        break
                    else:
                        # 名额和队列对不上，说明有别的地方绕过 push_queue_item 动了队列
                        raise RuntimeError(
                            "queue_slots is out of sync with the queues "
                            f"(photo={photo_queue.qsize()}, video={video_queue.qsize()}, "
                            f"other={other_queue.qsize()})")

                logger.info(
                    "Dequeued task id=%s filename=%s queue=%s remaining(photo=%s, video=%s, other=%s)",
                    element[3] if len(element) > 3 else None,
                    getFilename(element[0]),
                    queue_type,
                    photo_queue.qsize(),
                    video_queue.qsize(),
                    other_queue.qsize()
                )
                return element, queue_ref, queue_items_list_ref, queue_type
            finally:
                # 没真正取到东西（异常/取消）就把名额还回去，否则会永久漏掉一个任务
                if not took:
                    queue_slots.release()

        async def refresh_channel_info():
            global telegram_channel_info
            try:
                entity = await client.get_entity(peerChannel)
                telegram_channel_info = {
                    'title': getattr(entity, 'title', '') or 'Unknown Channel',
                    'id': channel_id,
                    'username': getattr(entity, 'username', None),
                }
            except Exception as e:
                logger.warning(f'Failed to load channel info: {e}')
                telegram_channel_info = {
                    'title': 'Unknown Channel',
                    'id': channel_id,
                    'username': None,
                }

        async def refresh_auth_state(message=None):
            global telegram_user_info, telegram_auth_state
            authorized = await client.is_user_authorized()
            if authorized:
                me = await client.get_me()
                telegram_user_info = {
                    'username': me.username,
                    'first_name': me.first_name,
                    'last_name': me.last_name or ''
                }
                saveSession(client.session)
                telegram_auth_state = {
                    'authorized': True,
                    'awaiting_code': False,
                    'requires_password': False,
                    'phone': auth_context.get('phone', ''),
                    'resend_available_in': get_auth_send_code_remaining(),
                    'message': message or f"Signed in as {me.first_name}",
                }
                auth_ready_event.set()
                await refresh_channel_info()
                logger.info(f"Telegram user: {me.username} ({me.first_name} {me.last_name})")
            else:
                telegram_user_info = None
                telegram_auth_state = {
                    'authorized': False,
                    'awaiting_code': bool(auth_context.get('phone_code_hash')),
                    'requires_password': False,
                    'phone': auth_context.get('phone', ''),
                    'resend_available_in': get_auth_send_code_remaining(),
                    'message': message or 'Please sign in from the web page.',
                }

        async def send_login_code(phone):
            auth_context['phone'] = phone.strip()
            auth_context['phone_code_hash'] = ''
            result = await client.send_code_request(auth_context['phone'])
            auth_context['phone_code_hash'] = result.phone_code_hash
            await refresh_auth_state('Verification code sent. Check Telegram or SMS.')
            return telegram_auth_state

        async def verify_login_code(phone, code):
            auth_context['phone'] = phone.strip()
            if not auth_context.get('phone_code_hash'):
                raise ValueError('No verification request is active. Send a code first.')

            try:
                await client.sign_in(
                    phone=auth_context['phone'],
                    code=code.strip(),
                    phone_code_hash=auth_context['phone_code_hash']
                )
                auth_context['phone_code_hash'] = ''
                await refresh_auth_state('Login successful.')
                return telegram_auth_state
            except SessionPasswordNeededError:
                telegram_auth_state.update({
                    'authorized': False,
                    'awaiting_code': False,
                    'requires_password': True,
                    'phone': auth_context.get('phone', ''),
                    'resend_available_in': get_auth_send_code_remaining(),
                    'message': 'Two-step verification is enabled. Enter your password.',
                })
                return telegram_auth_state

        async def verify_login_password(password):
            await client.sign_in(password=password)
            auth_context['phone_code_hash'] = ''
            await refresh_auth_state('Login successful.')
            return telegram_auth_state

        def schedule_auth_call(coro):
            future = asyncio.run_coroutine_threadsafe(coro, main_loop)
            return future.result(timeout=120)

        global web_auth_send_code, web_auth_verify_code, web_auth_verify_password
        web_auth_send_code = lambda phone: schedule_auth_call(send_login_code(phone))
        web_auth_verify_code = lambda phone, code: schedule_auth_call(verify_login_code(phone, code))
        web_auth_verify_password = lambda password: schedule_auth_call(verify_login_password(password))

        await refresh_auth_state(initial_auth_message)
        if not telegram_auth_state.get('authorized'):
            logger.info("Telegram client is waiting for web login")
            await auth_ready_event.wait()
        
        # 内部的 set_progress 函数，使用闭包访问状态
        async def set_progress(download_id, filename, message, received, total, size=0, source_message_link=""):
            nonlocal lastUpdate
            
            async with status_lock:
                global web_in_progress
                if total and received >= total:
                    try:
                        in_progress.pop(str(download_id), None)
                        web_in_progress = in_progress
                    except:
                        pass
                    return

                percentage = math.trunc(received / total * 10000) / 100 if total else 0.0
                progress_message = "{0} % ({1} / {2})".format(percentage, received, total)
                in_progress[str(download_id)] = {
                    'filename': filename,
                    'progress': progress_message,
                    'size': size,
                    'source_message_link': source_message_link,
                }
                web_in_progress = in_progress

                currentTime = time.time()
                if (currentTime - lastUpdate) > updateFrequency:
                    await log_reply(message, progress_message)
                    lastUpdate = currentTime

        async def persist_queued_download(message_obj, target_dir_override=None, existing_download_id=None, recovery_note=None):
            filename = getFilename(message_obj)
            file_category = getFileTypeCategory(filename)
            size = get_message_media_size(message_obj)
            source_channel = get_source_channel_id(message_obj)
            source_message_id = getattr(message_obj, 'id', None)
            source_message_link = build_message_link(message_obj)
            resolved_target_dir = resolve_retry_directory(target_dir_override) if target_dir_override else None

            async with db_lock:
                if existing_download_id is None:
                    cursor.execute(
                        '''
                        INSERT INTO downloads (
                            filename, file_type, status, size, progress, source_channel_id,
                            source_message_id, source_message_link, target_dir, error_message
                        )
                        VALUES (?, ?, 'queued', ?, 0.0, ?, ?, ?, ?, ?)
                        ''',
                        (
                            filename, file_category, size, source_channel, source_message_id,
                            source_message_link, resolved_target_dir, recovery_note
                        )
                    )
                    conn.commit()
                    # 新增一行 —— 让 total_tasks 缓存下一次读取刷新
                    invalidate_total_tasks_count()
                    return cursor.lastrowid

                cursor.execute(
                    '''
                    UPDATE downloads
                    SET filename = ?, file_type = ?, status = 'queued', size = ?, progress = 0.0,
                        source_channel_id = ?, source_message_id = ?, source_message_link = ?,
                        target_dir = ?, download_path = NULL, end_time = NULL, error_message = COALESCE(?, error_message)
                    WHERE id = ?
                    ''',
                    (
                        filename, file_category, size, source_channel, source_message_id,
                        source_message_link, resolved_target_dir, recovery_note, existing_download_id
                    )
                )
                conn.commit()
                return existing_download_id

        def is_monitored_channel_message(message_obj) -> bool:
            """消息是否来自被监听的频道本身（区别于通过链接抓回来的外部消息）。"""
            return get_source_channel_id(message_obj) == channel_id

        async def notify_about_message(message_obj, text, reply_target=None):
            """把状态 / 失败通知发回去，且**绝不**发进别人的频道。

            通过链接抓回来的消息属于外部私密频道，直接 ``message.reply()`` 会往人家
            频道里发消息。这里的规则是：优先回复调用方指定的 reply_target（通常是
            被监听频道里那条含链接的消息），否则只有来源就是被监听频道时才直接回复，
            剩下的情况统一发到被监听频道。

            发送走 bounded_notify：断链/限流/风暴时返回 None（所有调用方都接受
            status_message 为 None），绝不挂死调用方。
            """
            if reply_target is not None:
                return await bounded_notify(reply_target.reply(text), "reply to link request")
            if is_monitored_channel_message(message_obj):
                return await bounded_notify(message_obj.reply(text), "reply in monitored channel")
            return await bounded_notify(client.send_message(peerChannel, text), "send to monitored channel")

        async def enqueue_download_message(message_obj, notice_template="{0} added to queue", target_dir_override=None, existing_download_id=None, silent=False, recovery_note=None, reply_target=None):
            is_photo = getattr(message_obj, 'photo', None) is not None
            is_document = getattr(message_obj, 'document', None) is not None
            if not (is_photo or is_document):
                raise ValueError("That message does not contain a downloadable file")

            filename = getFilename(message_obj)
            temp_path = build_safe_path(tempFolder, f"{filename}.{TELEGRAM_DAEMON_TEMP_SUFFIX}")
            root_path = build_safe_path(downloadFolder, filename)
            # 重投一条已有记录（Web 上点重试、重启恢复）时，那个 .tdd 很可能就是它自己
            # 上次没下完的半成品，不能当成"已存在"给忽略掉——那正是要接着下的东西。
            retrying_existing = resume_enabled and existing_download_id is not None
            if path.exists(temp_path) and duplicates == "ignore" and not retrying_existing:
                status_message = None if silent else await notify_about_message(
                    message_obj, "{0} already exists. Ignoring it.".format(filename), reply_target
                )
                logger.info(f"Ignoring duplicate file: {filename}")
                return {'queued': False, 'filename': filename, 'message': status_message}

            download_id = await persist_queued_download(
                message_obj,
                target_dir_override=target_dir_override,
                existing_download_id=existing_download_id,
                recovery_note=recovery_note
            )
            status_message = None if silent else await notify_about_message(
                message_obj, notice_template.format(filename), reply_target
            )
            queue_item = [message_obj, status_message, target_dir_override, download_id, time.time()]
            queue_type = await push_queue_item(queue_item)

            logger.info(f"Added file to queue: {filename}, type: {queue_type}")
            socketio.emit('new_task', {
                'filename': filename,
                'status': 'queued',
                'downloadTime': time.strftime('%Y-%m-%d %H:%M:%S'),
                'source_message_link': build_message_link(message_obj)
            })
            emit_status_update()
            return {'queued': True, 'filename': filename, 'message': status_message, 'download_id': download_id}

        async def retry_download_message(source_channel_id, source_message_id, target_dir_override=None, existing_download_id=None):
            message_obj = await client.get_messages(PeerChannel(source_channel_id), ids=source_message_id)
            if not message_obj:
                raise ValueError("Unable to locate the original Telegram message")
            # 如果传了 existing_download_id，复用旧记录（更新为 queued），避免历史里出现重复条目
            return await enqueue_download_message(
                message_obj,
                "{0} re-added to queue",
                target_dir_override=target_dir_override,
                existing_download_id=existing_download_id,
                recovery_note="Manual retry via web UI" if existing_download_id else None,
            )

        # ------------------------------------------------------------------
        # 消息链接下载：把频道里出现的 t.me 消息链接还原成真正的消息再入队
        # ------------------------------------------------------------------
        link_entity_cache = {}
        last_dialog_refresh = [0.0]
        DIALOG_REFRESH_COOLDOWN = 300  # 秒
        # 一条链接消息最多逐个回复多少个文件（超出部分静默入队，只在汇总里体现）
        LINK_STATUS_MESSAGE_LIMIT = 5

        async def refresh_dialog_cache(force=False):
            """拉一遍会话列表，把私密频道的 access_hash 灌进 Telethon 的实体缓存。

            ``t.me/c/<id>/<msg>`` 链接只给出频道内部 ID，没有 access_hash；只有当这个
            频道出现在账号的会话列表里、被 Telethon 记进 session 之后，``PeerChannel(id)``
            才能解析成功。冷启动 / 新加入的频道会命中这条路径。
            """
            now = time.time()
            if not force and (now - last_dialog_refresh[0]) < DIALOG_REFRESH_COOLDOWN:
                return False
            last_dialog_refresh[0] = now
            logger.info("Refreshing dialog cache to resolve private channel links")
            await client.get_dialogs()
            return True

        async def resolve_link_entity(link):
            """把一条已解析的链接映射成 Telethon 实体（频道对象）。"""
            cache_key = link.entity_key
            cached = link_entity_cache.get(cache_key)
            if cached is not None:
                return cached

            if link.kind == 'private':
                try:
                    entity = await client.get_entity(PeerChannel(link.channel_id))
                except (ValueError, TypeError):
                    # 实体不在缓存里：刷新会话列表后再试一次
                    await refresh_dialog_cache(force=True)
                    try:
                        entity = await client.get_entity(PeerChannel(link.channel_id))
                    except (ValueError, TypeError) as exc:
                        raise ValueError(
                            "当前账号没有这个私密频道的访问权限（需要先加入该频道）"
                        ) from exc
            elif link.kind == 'public':
                entity = await client.get_entity(link.username)
            else:
                invite = await client(CheckChatInviteRequest(link.invite_hash))
                entity = getattr(invite, 'chat', None)
                if entity is None:
                    if not link_auto_join_enabled:
                        raise ValueError(
                            "当前账号尚未加入该邀请链接对应的频道；"
                            "设置 TELEGRAM_DAEMON_LINK_AUTO_JOIN=1 可允许自动加入"
                        )
                    updates = await client(ImportChatInviteRequest(link.invite_hash))
                    chats = getattr(updates, 'chats', None) or []
                    if not chats:
                        raise ValueError("加入邀请链接对应的频道失败")
                    entity = chats[0]
                    logger.info("Joined chat via invite link: %s", getattr(entity, 'title', entity))

            link_entity_cache[cache_key] = entity
            return entity

        async def fetch_link_messages(link, budget):
            """按链接取回对应的消息对象列表（已按 budget 截断）。"""
            entity = await resolve_link_entity(link)

            wanted_ids = list(link.message_ids)[:max(budget, 0)]
            if not wanted_ids:
                return []

            fetched = await client.get_messages(entity, ids=wanted_ids)
            if fetched is None:
                return []
            if not isinstance(fetched, list):
                fetched = [fetched]
            messages = [m for m in fetched if m is not None]

            # 链接指向相册中的一条时，默认把整组一起下载；带 ?single 的链接尊重原意只下这条
            if link_album_enabled and not link.single and len(wanted_ids) == 1 and messages:
                base = messages[0]
                grouped_id = getattr(base, 'grouped_id', None)
                if grouped_id:
                    # 相册最多 10 条且 ID 连续，取前后各 9 条足够覆盖
                    neighborhood = list(range(max(1, base.id - 9), base.id + 10))
                    siblings = await client.get_messages(entity, ids=neighborhood)
                    group = [
                        m for m in (siblings or [])
                        if m is not None and getattr(m, 'grouped_id', None) == grouped_id
                    ]
                    if group:
                        group.sort(key=lambda m: m.id)
                        messages = group[:max(budget, 1)]
                        logger.info(
                            "Link points to an album (grouped_id=%s); expanded to %s messages",
                            grouped_id, len(messages)
                        )

            return messages

        async def handle_link_message(event, links):
            """处理"频道里收到一条含 Telegram 消息链接的文本"这件事。"""
            source_message = event.message
            status_message = None
            with contextlib.suppress(Exception):
                status_message = await source_message.reply(
                    f"🔗 收到 {len(links)} 个 Telegram 消息链接，正在解析…"
                )

            budget = link_max_messages
            queued_total = 0
            skipped_total = 0
            notes = []

            for link in links:
                label = link.describe()
                if budget <= 0:
                    notes.append(f"⏭ {label}：已达单条消息 {link_max_messages} 条上限，跳过")
                    continue
                try:
                    messages = await fetch_link_messages(link, budget)
                except Exception as resolve_error:
                    logger.warning("Failed to resolve link %s: %s", label, resolve_error, exc_info=True)
                    notes.append(f"❌ {label}：{resolve_error}")
                    continue

                if not messages:
                    notes.append(f"⚠️ {label}：消息不存在或已被删除")
                    continue

                queued_here = 0
                failed_here = 0
                truncated_here = False
                for message_obj in messages:
                    if budget <= 0:
                        truncated_here = True
                        break
                    has_media = (
                        getattr(message_obj, 'photo', None) is not None
                        or getattr(message_obj, 'document', None) is not None
                    )
                    if not has_media:
                        skipped_total += 1
                        continue
                    try:
                        result = await enqueue_download_message(
                            message_obj,
                            "{0} added to queue",
                            reply_target=source_message,
                            # 一条消息展开出一堆文件时不再逐个回复，避免触发 Telegram 限流；
                            # 后续文件的进度看 Web UI 或末尾的汇总即可。
                            silent=queued_total >= LINK_STATUS_MESSAGE_LIMIT,
                        )
                    except Exception as enqueue_error:
                        failed_here += 1
                        logger.error(
                            "Failed to enqueue linked message %s: %s",
                            getattr(message_obj, 'id', None), enqueue_error, exc_info=True
                        )
                        notes.append(f"❌ {label}：入队失败（{enqueue_error}）")
                        continue
                    if result.get('queued'):
                        queued_here += 1
                        queued_total += 1
                        budget -= 1
                    else:
                        skipped_total += 1

                if queued_here:
                    note = f"✅ {label}：{queued_here} 个文件已入队"
                    if truncated_here:
                        note += f"（已达 {link_max_messages} 条上限，其余未处理）"
                    notes.append(note)
                elif not failed_here:
                    notes.append(f"⚠️ {label}：消息里没有可下载的文件")

            summary_lines = [
                f"🔗 链接解析完成：入队 {queued_total} 个文件"
                + (f"，跳过 {skipped_total} 条" if skipped_total else "")
            ]
            summary_lines.extend(notes[:10])
            if len(notes) > 10:
                summary_lines.append(f"…另有 {len(notes) - 10} 条结果，详见日志")
            summary = "\n".join(summary_lines)

            logger.info(
                "Link request handled: links=%s queued=%s skipped=%s",
                len(links), queued_total, skipped_total
            )
            if status_message is not None:
                await bounded_notify(status_message.edit(summary), "edit link summary")
            elif queued_total == 0:
                await bounded_notify(source_message.reply(summary), "reply link summary")

        async def restore_pending_downloads():
            async with db_lock:
                cursor.execute(
                    '''
                    SELECT id, source_channel_id, source_message_id, target_dir, status, filename
                    FROM downloads
                    WHERE status IN ('queued', 'interrupted', 'downloading')
                      AND source_channel_id IS NOT NULL
                      AND source_message_id IS NOT NULL
                    ORDER BY id ASC
                    '''
                )
                pending_rows = cursor.fetchall()

            restored = 0
            for download_id, source_channel_id, source_message_id, target_dir, previous_status, previous_filename in pending_rows:
                try:
                    # 开了断点续传就保留半成品：重启前下到一半的那些字节，重新入队后
                    # 会被 worker 认出来接着下（靠 downloads.temp_path 认领）。
                    if not resume_enabled:
                        cleanup_temp_file_for_filename(previous_filename)
                    message_obj = await client.get_messages(PeerChannel(source_channel_id), ids=source_message_id)
                    if not message_obj:
                        async with db_lock:
                            cursor.execute(
                                '''
                                UPDATE downloads
                                SET status = 'failed', error_message = ?, end_time = CURRENT_TIMESTAMP
                                WHERE id = ?
                                ''',
                                ("Original Telegram message no longer exists", download_id)
                            )
                            conn.commit()
                        continue

                    recovery_note = f"Recovered after restart from {previous_status}"
                    await enqueue_download_message(
                        message_obj,
                        "{0} restored to queue after restart",
                        target_dir_override=target_dir,
                        existing_download_id=download_id,
                        silent=True,
                        recovery_note=recovery_note
                    )
                    restored += 1
                except Exception as restore_error:
                    logger.error(f"Failed to restore pending download {download_id}: {restore_error}", exc_info=True)

            if restored > 0:
                logger.info(f"Restored {restored} pending downloads after restart")

        async def monitor_queue_health():
            warned_queue_ids = set()
            pipeline_stalled_since = None
            while True:
                try:
                    await asyncio.sleep(30)
                    now = time.time()
                    stale_entries = []
                    async with queue_lock:
                        # 不变式：拿着 queue_lock 的时候，三条 asyncio.Queue 里的元素数
                        # 必须和跟踪列表 queue_items 一致。对不上就说明有任务被"取出但没人处理"
                        # ——页面上会永远显示排队中，而没有任何 worker 会碰它。
                        # 这个坑之前就踩过（见 pop_next_queue_item 的注释），
                        # 这里留一道明确的告警，别再让它悄无声息。
                        queued_total = (photo_queue.qsize() + video_queue.qsize()
                                        + other_queue.qsize())
                        if queued_total != len(queue_items):
                            logger.error(
                                "Queue bookkeeping mismatch: asyncio queues hold %d items but "
                                "the tracking list has %d (photo=%d, video=%d, other=%d). "
                                "Tasks may be stuck as 'queued' forever.",
                                queued_total, len(queue_items),
                                photo_queue.qsize(), video_queue.qsize(), other_queue.qsize())
                        for item in list(queue_items):
                            download_id = item[3] if len(item) > 3 else None
                            queued_at = item[4] if len(item) > 4 and isinstance(item[4], (int, float)) else None
                            if not download_id or not queued_at:
                                continue
                            queue_age = int(max(now - queued_at, 0))
                            if queue_age >= TELEGRAM_DAEMON_QUEUE_WARN_SECONDS:
                                stale_entries.append((str(download_id), getFilename(item[0]), queue_age))

                    # 流水线级停摆：队列非空但没有任何 worker 在消化，说明 worker 全部
                    # 冻住或退出了（2026-08-14 事故形态：全员挂死在无超时的通知 RPC 上）。
                    # 单任务的 stalled 告警只反映"排队久"，这里是全局判定，单独升级。
                    if queued_total > 0 and len(in_progress) == 0:
                        if pipeline_stalled_since is None:
                            pipeline_stalled_since = now
                        stalled_for = int(now - pipeline_stalled_since)
                        if stalled_for >= TELEGRAM_DAEMON_PIPELINE_STALL_CRITICAL_SECONDS:
                            logger.critical(
                                "Download pipeline stalled: %d queued / 0 active for %ds; "
                                "workers are likely frozen — restart the container to recover",
                                queued_total, stalled_for)
                    else:
                        pipeline_stalled_since = None

                    current_stale_ids = {entry[0] for entry in stale_entries}
                    warned_queue_ids.intersection_update(current_stale_ids)
                    for stale_id, stale_filename, queue_age in stale_entries:
                        if stale_id in warned_queue_ids:
                            continue
                        logger.warning(
                            "Queue task appears stalled id=%s filename=%s age=%ss active=%s queued=%s",
                            stale_id,
                            stale_filename,
                            queue_age,
                            len(in_progress),
                            len(queue_items)
                        )
                        warned_queue_ids.add(stale_id)
                except asyncio.CancelledError:
                    raise
                except Exception as monitor_error:
                    logger.error(f"Queue monitor error: {monitor_error}", exc_info=True)

        def schedule_retry(source_channel_id, source_message_id, target_dir_override=None, existing_download_id=None):
            future = asyncio.run_coroutine_threadsafe(
                retry_download_message(source_channel_id, source_message_id, target_dir_override, existing_download_id),
                main_loop
            )
            return future.result(timeout=60)

        async def cancel_download(download_id_int):
            """取消队列中的 / 进行中的下载任务。

            修复时序竞争：
            - worker 从 queue 取出 item 后，会先 pop 掉 active_queue_items_by_id（L1962），
              再做一堆 DB 写入，最后才把真正的 download_task 注册进 active_download_tasks（L2691）。
            - 在"已从队列取出 但 尚未注册下载任务"这段窗口里，用户点取消会两边都找不到，
              返回 404，体验很差。
            - 修复：如果 DB 里这条记录状态仍然是可取消状态（queued/downloading），就把
              download_id 登记进 cancelled_download_ids 并把状态写回 'cancelled'。
              worker 在注册下载任务前/后都会检查 cancelled_download_ids，能在那个窗口
              里让任务立刻 self-cancel。
            """
            download_id = int(download_id_int)
            download_id_str = str(download_id)
            found_kind = None

            # 先尝试取消队列里的（尚未被 worker 取走的）
            async with queue_lock:
                queued_item = active_queue_items_by_id.get(download_id_str)
                if queued_item is not None:
                    for bucket in (photo_queue_items, video_queue_items, other_queue_items):
                        if queued_item in bucket:
                            try:
                                bucket.remove(queued_item)
                            except ValueError:
                                pass
                    active_queue_items_by_id.pop(download_id_str, None)
                    rebuild_web_queue_items()
                    cancelled_download_ids.add(download_id_str)
                    async with db_lock:
                        cursor.execute(
                            '''UPDATE downloads SET status = 'cancelled', error_message = ?, end_time = CURRENT_TIMESTAMP WHERE id = ?''',
                            ("Cancelled from web UI before downloading", download_id)
                        )
                        conn.commit()
                    found_kind = 'queued'

            if found_kind is None:
                # 再尝试取消正在下载的（包括暂停中的）
                download_task = active_download_tasks.get(download_id_str)
                if download_task is not None and not download_task.done():
                    cancelled_download_ids.add(download_id_str)
                    # 如果任务处于暂停状态，先唤醒回调以便取消能正常传播
                    pause_evt = paused_download_ids.get(download_id_str)
                    if pause_evt is not None:
                        pause_evt.set()
                    download_task.cancel()
                    found_kind = 'downloading'

            if found_kind is None:
                # 窗口期兜底：既不在队列也没注册下载任务，但 DB 里可能还是
                # queued/downloading/paused —— 这是 dequeue 与 register_active_task 之间的缝隙。
                # 直接把 cancel 意图登记进 cancelled_download_ids；worker 一旦注册任务
                # 就会看到并立即 self-cancel。同时把 DB 标记为 cancelled，UI 立刻正确。
                async with db_lock:
                    cursor.execute(
                        'SELECT status FROM downloads WHERE id = ?', (download_id,)
                    )
                    row = cursor.fetchone()
                    current_status = row[0] if row else None
                if current_status in ('queued', 'downloading', 'paused'):
                    cancelled_download_ids.add(download_id_str)
                    async with db_lock:
                        cursor.execute(
                            '''UPDATE downloads SET status = 'cancelled', error_message = ?, end_time = CURRENT_TIMESTAMP WHERE id = ?''',
                            ("Cancelled from web UI (in flight)", download_id)
                        )
                        conn.commit()
                    found_kind = 'transitioning'
                else:
                    return {'found': False}

            emit_status_update()
            return {'found': True, 'state': found_kind, 'download_id': download_id}

        def schedule_cancel(download_id_int):
            future = asyncio.run_coroutine_threadsafe(cancel_download(download_id_int), main_loop)
            return future.result(timeout=30)

        async def pause_download(download_id_int):
            """暂停一个正在下载的任务。通过清除 threading.Event 阻塞进度回调。"""
            download_id = int(download_id_int)
            download_id_str = str(download_id)

            # 只能暂停正在下载的任务
            download_task = active_download_tasks.get(download_id_str)
            if download_task is None or download_task.done():
                return {'found': False}

            # 检查 DB 状态确认是 downloading
            async with db_lock:
                cursor.execute('SELECT status FROM downloads WHERE id = ?', (download_id,))
                row = cursor.fetchone()
            if not row or row[0] != 'downloading':
                return {'found': False}

            # 创建或清除 Event（cleared = 暂停状态）
            if download_id_str not in paused_download_ids:
                evt = threading.Event()
                evt.set()  # 默认可运行
                paused_download_ids[download_id_str] = evt
            evt = paused_download_ids[download_id_str]
            evt.clear()  # 暂停：回调将阻塞

            # 更新 DB 状态
            async with db_lock:
                cursor.execute(
                    'UPDATE downloads SET status = ?, error_message = NULL WHERE id = ?',
                    ('paused', download_id)
                )
                conn.commit()

            # 更新内存状态
            with sync_lock:
                entry = in_progress.get(download_id_str)
                if entry:
                    entry['paused'] = True
                    entry['speed_bps'] = 0

            emit_status_update()
            socketio.emit('download_progress', {
                'task_id': download_id,
                'status': 'paused',
                'speed_bps': 0,
            })
            return {'found': True, 'state': 'paused', 'download_id': download_id}

        async def resume_download(download_id_int):
            """恢复一个已暂停的任务。设置 threading.Event 解除回调阻塞。"""
            download_id = int(download_id_int)
            download_id_str = str(download_id)

            evt = paused_download_ids.get(download_id_str)
            if evt is None:
                return {'found': False}

            # 检查 DB 状态
            async with db_lock:
                cursor.execute('SELECT status FROM downloads WHERE id = ?', (download_id,))
                row = cursor.fetchone()
            if not row or row[0] != 'paused':
                return {'found': False}

            # 恢复：设置 Event，回调将继续执行
            evt.set()

            # 更新 DB 状态
            async with db_lock:
                cursor.execute(
                    'UPDATE downloads SET status = ?, error_message = NULL WHERE id = ?',
                    ('downloading', download_id)
                )
                conn.commit()

            # 更新内存状态
            with sync_lock:
                entry = in_progress.get(download_id_str)
                if entry:
                    entry.pop('paused', None)

            emit_status_update()
            socketio.emit('download_progress', {
                'task_id': download_id,
                'status': 'downloading',
            })
            return {'found': True, 'state': 'resumed', 'download_id': download_id}

        def schedule_pause(download_id_int):
            future = asyncio.run_coroutine_threadsafe(pause_download(download_id_int), main_loop)
            return future.result(timeout=30)

        def schedule_resume(download_id_int):
            future = asyncio.run_coroutine_threadsafe(resume_download(download_id_int), main_loop)
            return future.result(timeout=30)

        global web_retry_scheduler, web_cancel_scheduler, web_pause_scheduler, web_resume_scheduler
        web_retry_scheduler = schedule_retry
        web_cancel_scheduler = schedule_cancel
        web_pause_scheduler = schedule_pause
        web_resume_scheduler = schedule_resume
        await restore_pending_downloads()
        
        @client.on(events.NewMessage())
        async def handler(event):
            if event.to_id != peerChannel:
                return

            logger.debug(f"Received new message event: {event}")

            try:
                # 私密频道链接优先：文本里带 t.me 消息链接时，先把链接指向的消息抓回来。
                # 必须放在媒体分支之前——带链接的文本消息通常自带网页预览（MessageMediaWebPage），
                # 会被 event.photo / event.media 误判成"预览图可下载"或"不可下载的媒体"。
                # 只有真的解析出链接才走这条分支，其余消息的行为与之前完全一致。
                #
                # 这里刻意不看 event.out：号主往往就是用自己的账号把链接贴进频道的。
                # 守护进程自己的回复里只有 link.describe()（不含 t.me 链接），解析结果为空，
                # 因此不会自触发。
                if link_download_enabled:
                    links = extract_telegram_links(event.message, link_max_messages)
                    if links:
                        logger.info(
                            "Received %s Telegram message link(s): %s",
                            len(links), ", ".join(link.describe() for link in links)
                        )
                        await handle_link_message(event, links)
                        return

                # 检查是否是可下载的媒体消息
                # 使用 event.photo 和 event.document 快捷方式，更可靠
                is_photo = event.photo is not None
                is_document = event.document is not None

                if is_photo or is_document:
                    await enqueue_download_message(event.message)
                elif event.media:
                    # 有 media 但不是 photo 或 document
                    message=await event.reply("That is not downloadable. Try to send it as a file.")
                    logger.info(f"Received non-downloadable media: {type(event.media)}")
                # 检查是否是相册分组消息（grouped_id），这类消息没有 media 但也不应该当作命令
                elif hasattr(event.message, 'grouped_id') and event.message.grouped_id is not None:
                    # 相册分组消息，跳过处理
                    logger.debug(f"Skipping grouped message with grouped_id: {event.message.grouped_id}")
                    return
                # 只有当消息不是媒体消息也不是分组消息时，才检查是否是命令
                elif event.message and event.message.message:
                    # 忽略自己发送的消息（避免把回复消息当命令处理）
                    if event.out:
                        logger.debug(f"Ignoring outgoing message: {event.message.message[:50]}...")
                        return
                    
                    command = event.message.message
                    command = command.lower()
                    logger.info(f"Received command: {command}")
                    output = "Unknown command"

                    if command == "list":
                        try:
                            files = os.listdir(downloadFolder)
                            output = ""
                            for file in files:
                                file_path = os.path.join(downloadFolder, file)
                                if os.path.isfile(file_path):
                                    stat = os.stat(file_path)
                                    output += f"{stat.st_mode:10o} {stat.st_nlink:3} {stat.st_uid:5} {stat.st_gid:5} {stat.st_size:10} {time.strftime('%Y-%m-%d %H:%M', time.localtime(stat.st_mtime))} {file}\n"
                            logger.info(f"Command 'list' executed, found {len(files)} files")
                        except Exception as e:
                            output = f"Error listing files: {str(e)}"
                            logger.error(f"Error executing command 'list': {e}")
                    elif command == "status":
                        try:
                            output = "".join([
                                "{0}: {1} - {2}\n".format(
                                    key,
                                    value.get('filename', 'unknown'),
                                    value.get('progress', '')
                                )
                                for (key, value) in in_progress.items()
                            ])
                            if output: 
                                output = "Active downloads:\n\n" + output
                            else: 
                                output = "No active downloads"
                            logger.info(f"Command 'status' executed, found {len(in_progress)} active downloads")
                        except Exception as e:
                            output = f"Error checking status: {str(e)}"
                            logger.error(f"Error executing command 'status': {e}")
                    elif command == "clean":
                        try:
                            import glob
                            temp_files = glob.glob(os.path.join(tempFolder, f"*.{TELEGRAM_DAEMON_TEMP_SUFFIX}"))
                            output = f"Cleaning {tempFolder}\n"
                            for temp_file in temp_files:
                                os.remove(temp_file)
                                output += f"Removed: {os.path.basename(temp_file)}\n"
                            if not temp_files:
                                output += "No temporary files found.\n"
                            logger.info(f"Command 'clean' executed, removed {len(temp_files)} temporary files")
                        except Exception as e:
                            output = f"Error cleaning temporary files: {str(e)}"
                            logger.error(f"Error executing command 'clean': {e}")
                    elif command == "queue":
                        try:
                            files_in_queue = []
                            for item in queue_items:
                                files_in_queue.append(getFilename(item[0]))
                            output = "".join([ "{0}\n".format(filename) for filename in files_in_queue])
                            if output: 
                                output = "Files in queue:\n\n" + output
                            else: 
                                output = "Queue is empty"
                            logger.info(f"Command 'queue' executed, found {len(files_in_queue)} files in queue")
                        except Exception as e:
                            output = f"Error checking queue: {str(e)}"
                            logger.error(f"Error executing command 'queue': {e}")
                    else:
                        output = "Available commands: list, status, clean, queue"
                        if link_download_enabled:
                            output += "\n也可以直接发送 Telegram 消息链接（含私密频道 t.me/c/... 链接）自动下载。"
                        logger.info(f"Unknown command: {command}")

                    await log_reply(event, output)

            except (OSError, IOError, ValueError, TypeError) as e:
                    logger.error(f'Events handler error: {e}', exc_info=True)

        async def worker(worker_id):
            """动态Worker函数，空闲时自动从任意非空队列取任务"""
            while True:
                download_id = None
                filename = "unknown"
                worker_queue = None
                queue_items_list = None
                # 续传相关状态：失败处理里要用，所以在 try 之前先给个确定的初值
                temp_path = None
                resume_from = 0
                try:
                    element, worker_queue, queue_items_list, queue_type = await pop_next_queue_item()
                    message_obj=element[0]
                    message=element[1]
                    target_dir_override = element[2] if len(element) > 2 else None
                    download_id = element[3] if len(element) > 3 else None
                    # Update status after removing from queue
                    emit_status_update()

                    # 时序竞争兜底：用户可能在"已 dequeue 但 worker 尚未注册下载任务"
                    # 的窗口里点了取消，cancel_download 会把 id 加入 cancelled_download_ids
                    # 并把 DB 改成 cancelled。这里提前检查，避免白费力气 / 反把状态覆盖回 downloading。
                    if download_id is not None and str(download_id) in cancelled_download_ids:
                        logger.info(
                            f"Worker {worker_id} skip pre-cancelled task: id={download_id}, filename={getFilename(message_obj)}"
                        )
                        cancelled_download_ids.discard(str(download_id))
                        active_download_tasks.pop(str(download_id), None)
                        # DB 状态应该已经被 cancel_download 改成 cancelled；若仍是 queued，补一把
                        async with db_lock:
                            cursor.execute(
                                '''UPDATE downloads SET status = 'cancelled',
                                   error_message = COALESCE(error_message, ?),
                                   end_time = COALESCE(end_time, CURRENT_TIMESTAMP)
                                   WHERE id = ? AND status IN ('queued','downloading')''',
                                ("Cancelled by user", download_id)
                            )
                            conn.commit()
                        emit_status_update()
                        if worker_queue is not None:
                            worker_queue.task_done()
                        continue

                    filename=getFilename(message_obj)
                    fileName, fileExtension = os.path.splitext(filename)
                    tempfilename=fileName+"-"+getRandomId(8)+fileExtension

                    # Get file type category
                    file_category = getFileTypeCategory(filename)
                    logger.info(f"Worker {worker_id} processing file: {filename}, QueueType: {queue_type}, Category: {file_category}")
                    
                    # Create category directory with date subfolder
                    current_date = time.strftime('%Y-%m-%d')
                    category_folder = target_dir_override or os.path.join(downloadFolder, file_category, current_date)
                    if not os.path.exists(category_folder):
                        os.makedirs(category_folder)
                        logger.info(f"Created category folder: {category_folder}")

                    size = get_message_media_size(message_obj)
                    if getattr(message_obj, 'photo', None):
                       logger.info(f"Processing photo: {filename}, Estimated size: {size} bytes")
                    else: 
                       logger.info(f"Processing document: {filename}, Size: {size} bytes")

                    # 断点续传：这条记录上次写到哪个临时文件？如果那个半成品还在，
                    # 说明是自己上次没下完的东西，可以接着下——而不是当成"重名文件"
                    # 改名后从 0 重来（那正是老逻辑里大文件永远下不完的原因之一）。
                    own_partial_path = None
                    if resume_enabled and download_id is not None:
                        async with db_lock:
                            cursor.execute(
                                'SELECT temp_path FROM downloads WHERE id = ?', (download_id,))
                            partial_row = cursor.fetchone()
                        recorded_temp = partial_row[0] if partial_row else None
                        if recorded_temp and os.path.exists(recorded_temp):
                            try:
                                # 记录里的路径必须仍落在临时目录内，避免脏数据把文件写到别处
                                own_partial_path = ensure_existing_path_within(tempFolder, recorded_temp)
                            except (ValueError, OSError):
                                logger.warning(
                                    "Ignoring out-of-tree temp_path for id=%s: %s",
                                    download_id, recorded_temp)
                        if own_partial_path:
                            # 沿用上次那个文件名，否则临时文件名对不上，就找不到自己的半成品了
                            suffix = f".{TELEGRAM_DAEMON_TEMP_SUFFIX}"
                            recorded_name = os.path.basename(own_partial_path)
                            if recorded_name.endswith(suffix):
                                filename = recorded_name[: -len(suffix)]
                            else:
                                own_partial_path = None

                    # Check for duplicates in the category folder
                    in_progress_temp_path = build_safe_path(tempFolder, f"{filename}.{TELEGRAM_DAEMON_TEMP_SUFFIX}")
                    final_duplicate_path = build_safe_path(category_folder, filename)
                    # 自己的半成品不算"撞名的在途下载"，否则每次重试都会改名重下。
                    own_partial_here = (
                        own_partial_path is not None
                        and os.path.abspath(own_partial_path) == os.path.abspath(in_progress_temp_path)
                    )
                    foreign_temp_exists = path.exists(in_progress_temp_path) and not own_partial_here
                    if foreign_temp_exists or path.exists(final_duplicate_path):
                        should_rename_for_size_mismatch = foreign_temp_exists
                        should_rename_for_unknown_size = False
                        if path.exists(final_duplicate_path) and size > 0:
                            try:
                                existing_size = os.path.getsize(final_duplicate_path)
                                should_rename_for_size_mismatch = existing_size != size
                            except OSError:
                                logger.warning(f"Unable to read existing file size for duplicate check: {final_duplicate_path}", exc_info=True)
                                should_rename_for_unknown_size = True
                        elif path.exists(final_duplicate_path):
                            should_rename_for_unknown_size = True

                        if should_rename_for_size_mismatch:
                           filename = tempfilename
                           logger.info(f"Renamed file because an existing file with the same name has a different size: {filename}")
                        elif should_rename_for_unknown_size:
                           filename = tempfilename
                           logger.info(f"Renamed file because duplicate size could not be compared reliably: {filename}")
                        elif duplicates == "rename":
                           filename = tempfilename
                           logger.info(f"Renamed file to avoid duplicate: {filename}")
                        elif duplicates == "ignore":
                           logger.info(f"Ignoring duplicate file: {filename}")
                           if download_id:
                               async with db_lock:
                                   cursor.execute(
                                       '''
                                       UPDATE downloads
                                       SET status = 'ignored', error_message = ?, end_time = CURRENT_TIMESTAMP
                                       WHERE id = ?
                                       ''',
                                       ("Duplicate file ignored", download_id)
                                   )
                                   conn.commit()
                           worker_queue.task_done()
                           continue

                    # 上面任何一条改名分支都会让文件名变掉，半成品也就对不上了：放弃续传，从头下。
                    temp_path = build_safe_path(tempFolder, f"{filename}.{TELEGRAM_DAEMON_TEMP_SUFFIX}")
                    if own_partial_path and os.path.abspath(own_partial_path) != os.path.abspath(temp_path):
                        logger.info(
                            "Discarding resume point for id=%s: target file was renamed to %s",
                            download_id, filename)
                        # 改了名之后那份半成品再也没人会来续，顺手删掉别留垃圾
                        with contextlib.suppress(OSError):
                            os.remove(own_partial_path)
                        own_partial_path = None

                    resume_from = resumable_offset(own_partial_path, size) if own_partial_path else 0
                    if resume_from > 0:
                        logger.info(
                            "Resuming download id=%s from %d/%d bytes (%.1f%%): %s",
                            download_id, resume_from, size,
                            resume_from / size * 100 if size else 0.0, filename)
                    elif own_partial_path:
                        # 半成品存在但不可用（大小对不上/不足一个分块），清掉重下。
                        with contextlib.suppress(Exception):
                            os.remove(own_partial_path)

                    # 续传时进度条从断点起算，别让页面上的百分比倒退回 0。
                    initial_progress = (resume_from / size * 100) if (size and resume_from) else 0.0
                    effective_timeout = compute_download_timeout(size)

                    # Update queued record into downloading state
                    download_path = build_safe_path(category_folder, filename)
                    source_channel = get_source_channel_id(message_obj)
                    source_message_id = getattr(message_obj, 'id', None)
                    source_message_link = build_message_link(message_obj)
                    async with db_lock:
                        if download_id is None:
                            cursor.execute(
                                '''
                                INSERT INTO downloads (
                                    filename, file_type, status, size, progress, download_path,
                                    source_channel_id, source_message_id, source_message_link, target_dir,
                                    temp_path
                                )
                                VALUES (?, ?, 'downloading', ?, ?, ?, ?, ?, ?, ?, ?)
                                ''',
                                (
                                    filename, file_category, size, initial_progress, download_path, source_channel,
                                    source_message_id, source_message_link, target_dir_override, temp_path
                                )
                            )
                            download_id = cursor.lastrowid
                            # 新增一行 —— 让 total_tasks 缓存下一次读取刷新
                            invalidate_total_tasks_count()
                        else:
                            # start_time 重新打点：它和 end_time 成对显示在页面上，两者之差
                            # 必须是一段**真实连续**的下载时间。老逻辑只在 INSERT 时写一次，
                            # 于是 Web 上点重试时 start_time 还停在最初入队那一刻，一条隔天
                            # 重试的记录会显示成"跑了 28 小时"，实际只下了 91 分钟。
                            #
                            # 打点选在"worker 真正开始下载"这一刻，而不是入队那一刻，
                            # 所以排队等待的时间不计入耗时；每次尝试都重打，首次和重试
                            # 口径一致。续传的那次只覆盖最后一段，页面上另有"重试 N"
                            # 说明前面还有过几轮。
                            cursor.execute(
                                '''
                                UPDATE downloads
                                SET filename = ?, file_type = ?, status = 'downloading', size = ?, progress = ?,
                                    download_path = ?, source_channel_id = ?, source_message_id = ?,
                                    source_message_link = ?, target_dir = ?, temp_path = ?,
                                    start_time = CURRENT_TIMESTAMP, end_time = NULL
                                WHERE id = ?
                                ''',
                                (
                                    filename, file_category, size, initial_progress, download_path, source_channel,
                                    source_message_id, source_message_link, target_dir_override, temp_path,
                                    download_id
                                )
                            )
                        conn.commit()
                    logger.info(f"Inserted download record: ID={download_id}, Status=downloading")

                    await log_reply(
                        message,
                        "Downloading file {0} ({1} bytes) to {2}".format(filename, size, file_category)
                    )

                    # 进度回调是 **async** 的：Telethon 和本项目的分块下载器都会 await 可等待的
                    # 回调返回值。这一点对暂停很关键——早先用 threading.Event.wait() 同步阻塞，
                    # 会把整个 event loop 冻住，连超时看门狗自己都跑不了，暂停时长于是被算进
                    # 总耗时，恢复后往往立刻被判超时。改成 asyncio.sleep 后 loop 照常运转。
                    #
                    # 为了避免每个 tick 都写 SQLite + 广播 WS，按 **进度变化 >= 1% 或距上次更新
                    # >= 2s** 的节流策略做事。仅当真正推进时才触发 DB 写、WS 广播、Telegram 回复。
                    last_progress_time = [time.time()]
                    # 速度按“固定时间窗口”统计，而不是相邻两次回调之差：并行分块下载时回调会
                    # 在一个网络往返内突发触发多次（间隔≈0），用瞬时差会把速度放大几十上百倍。
                    # 只有距上次采样 >= SPEED_SAMPLE_INTERVAL 才重算速度，否则沿用上次结果。
                    last_speed_snapshot = [{'received': resume_from, 'timestamp': time.time(), 'speed_bps': 0.0}]
                    SPEED_SAMPLE_INTERVAL = 0.5  # seconds
                    # 节流快照：[上次写库/广播的 percentage, 上次写库/广播的时间戳]
                    last_persisted = [-1.0, 0.0]
                    PROGRESS_MIN_DELTA = 1.0   # %
                    PROGRESS_MIN_INTERVAL = 2.0  # seconds
                    async def download_callback(received, total):
                        nonlocal lastUpdate

                        # === 暂停支持 ===
                        # 任务被暂停时停在这里，直到恢复或被取消。
                        pause_evt = paused_download_ids.get(str(download_id))
                        if pause_evt is not None and not pause_evt.is_set():
                            logger.info(f"Download paused: {filename} (id={download_id})")
                            while not pause_evt.is_set():
                                # 每 0.5s 醒一次：如果下载任务已被取消就别再等了
                                if download_task is not None and download_task.done():
                                    logger.info(f"Paused download was cancelled: {filename}")
                                    return
                                await asyncio.sleep(0.5)
                            logger.info(f"Download resumed: {filename} (id={download_id})")

                        # total 可能在媒体元信息缺失时为 0；避免除零异常
                        percentage = math.trunc(received / total * 10000) / 100 if total else 0.0
                        progress_message = "{0} % ({1} / {2})".format(percentage, received, total)
                        last_progress_time[0] = time.time()

                        snapshot = last_speed_snapshot[0]
                        elapsed_seconds = last_progress_time[0] - snapshot['timestamp']
                        if elapsed_seconds >= SPEED_SAMPLE_INTERVAL:
                            bytes_delta = max(received - snapshot['received'], 0)
                            speed_bps = bytes_delta / elapsed_seconds
                            last_speed_snapshot[0] = {
                                'received': received,
                                'timestamp': last_progress_time[0],
                                'speed_bps': speed_bps,
                            }
                        else:
                            # 时间窗口未到，沿用上次的稳定速度，避免突发回调把瞬时速度算爆。
                            speed_bps = snapshot['speed_bps']

                        # in-memory 状态每次都更新——便宜、无锁竞争，/api/tasks 能读到最新速度
                        with sync_lock:
                            existing_entry = in_progress.get(str(download_id)) or {}
                            in_progress[str(download_id)] = {
                                'filename': filename,
                                'progress': progress_message,
                                'size': size,
                                'source_message_link': source_message_link,
                                'speed_bps': speed_bps,
                                # 首次出现时记录起始时间；后续刷新保持稳定
                                'started_at': existing_entry.get('started_at') or time.time(),
                                'download_id': download_id,
                            }
                            global web_in_progress
                            web_in_progress = in_progress

                            currentTime = time.time()
                            if (currentTime - lastUpdate) > updateFrequency:
                                # 对 Telegram 客户端的回复本来就已经有 updateFrequency 节流，保持原样
                                asyncio.create_task(log_reply(message, progress_message))
                                lastUpdate = currentTime

                        # 节流：只有在进度推进够多 或 距上次写入够久 时才写 DB + 广播 WS
                        now = last_progress_time[0]
                        should_persist = (
                            percentage >= 100.0 or
                            percentage - last_persisted[0] >= PROGRESS_MIN_DELTA or
                            (now - last_persisted[1]) >= PROGRESS_MIN_INTERVAL
                        )
                        if not should_persist:
                            return

                        last_persisted[0] = percentage
                        last_persisted[1] = now

                        if download_id:
                            try:
                                with sync_db_lock:
                                    cursor.execute(
                                        'UPDATE downloads SET progress = ? WHERE id = ?',
                                        (percentage, download_id),
                                    )
                                    conn.commit()
                            except Exception as db_exc:
                                # 写库失败不中断下载，只记录一次
                                logger.warning(f"Progress DB update failed for id={download_id}: {db_exc}")

                        progress_int = int(percentage)
                        if progress_int > 0 and progress_int % 10 == 0 and abs(percentage - progress_int) < 0.01:
                            logger.info(f"Download progress: {filename} - {progress_int}% ({received}/{total} bytes)")

                        socketio.emit('download_progress', {
                            'task_id': download_id,
                            'filename': filename,
                            'progress': percentage,
                            'received': received,
                            'total': total,
                            'status': 'downloading',
                            'speed_bps': speed_bps,
                        })

                        # 开始下载时刷新一下总览状态
                        if received > 0 and total and received < total * 0.01:
                            emit_status_update()

                    # 三层超时：开始超时（连不上/取不到）、无进度超时（真卡死）、
                    # 总超时（按体积折算，见 compute_download_timeout）。
                    download_started = [False]  # 使用列表让闭包能修改

                    async def check_start_callback(received, total):
                        if received > 0:
                            download_started[0] = True
                        await download_callback(received, total)

                    async def refresh_source_document():
                        """file_reference 过期时重新取回消息，换一份新的 document。

                        大文件下载可能跨几个小时，而 file_reference 的有效期比这短，
                        不刷新的话下到一半就会 FILE_REFERENCE_EXPIRED。
                        """
                        if source_channel is None or source_message_id is None:
                            return None
                        fresh = await client.get_messages(
                            PeerChannel(source_channel), ids=source_message_id)
                        if fresh is None:
                            return None
                        return get_parallel_location(fresh)

                    download_task = None
                    try:
                        # 创建下载任务（分块下载，出错时自动回退到 Telethon 原生下载）
                        download_task = asyncio.create_task(
                            download_media_dispatch(
                                message_obj,
                                temp_path,
                                check_start_callback,
                                resume_from=resume_from,
                                refresh_document=refresh_source_document,
                            )
                        )
                        # 注册到全局 active_download_tasks 供 /api/cancel 使用
                        if download_id is not None:
                            active_download_tasks[str(download_id)] = download_task
                            # 如果之前已标记取消（用户在调度前再次点了取消），立刻取消
                            if str(download_id) in cancelled_download_ids:
                                download_task.cancel()
                        
                        # 等待下载开始或超时
                        start_time = time.time()
                        while not download_started[0] and (time.time() - start_time) < start_timeout:
                            if download_task.done():
                                break
                            await asyncio.sleep(1)
                        
                        # 如果下载没有在 start_timeout 内开始，取消任务
                        if not download_started[0] and not download_task.done():
                            download_task.cancel()
                            try:
                                await download_task
                            except asyncio.CancelledError:
                                pass
                            raise asyncio.TimeoutError(f"Download did not start within {start_timeout} seconds")
                        
                        # 等待下载完成或总超时
                        paused_accumulated = 0.0
                        pause_check_time = None
                        while not download_task.done():
                            # 暂停期间不计入超时
                            evt = paused_download_ids.get(str(download_id))
                            if evt is not None and not evt.is_set():
                                if pause_check_time is None:
                                    pause_check_time = time.time()
                                await asyncio.sleep(1)
                                continue
                            else:
                                if pause_check_time is not None:
                                    paused_accumulated += time.time() - pause_check_time
                                    pause_check_time = None

                            elapsed = (time.time() - start_time) - paused_accumulated
                            if elapsed > effective_timeout:
                                raise asyncio.TimeoutError(
                                    f"Download exceeded {effective_timeout} seconds")
                            if download_started[0] and (time.time() - last_progress_time[0]) > no_progress_timeout:
                                raise asyncio.TimeoutError(f"No download progress for {no_progress_timeout} seconds")
                            await asyncio.sleep(1)

                        await download_task

                        # 完整性校验：续传是靠"临时文件已有多少字节"推断断点的，一旦
                        # 偏移量算错就会产出一个大小对得上、内容却错位的文件。这里在
                        # 搬运之前先核对大小，对不上就当失败处理（半成品会被清掉重下）。
                        if size and os.path.exists(temp_path):
                            downloaded_bytes = os.path.getsize(temp_path)
                            if downloaded_bytes != size:
                                # 这份半成品不可信，留着只会让下一轮从错误的断点继续，
                                # 直接删掉，让重试从头来过。
                                with contextlib.suppress(OSError):
                                    os.remove(temp_path)
                                raise IOError(
                                    f"Downloaded size mismatch for {filename}: "
                                    f"got {downloaded_bytes} bytes, expected {size}")

                        await set_progress(download_id, filename, message, 100, 100, size=size, source_message_link=source_message_link)
                        move(temp_path, download_path)
                        # 文件已经搬走，续传信息作废
                        if download_id is not None:
                            with contextlib.suppress(Exception):
                                async with db_lock:
                                    cursor.execute(
                                        'UPDATE downloads SET temp_path = NULL WHERE id = ?',
                                        (download_id,))
                                    conn.commit()
                    except (asyncio.TimeoutError, asyncio.CancelledError) as e:
                        cancelled = isinstance(e, asyncio.CancelledError)
                        if download_task is not None and not download_task.done():
                            download_task.cancel()
                            try:
                                await download_task
                            except asyncio.CancelledError:
                                pass
                        # 半成品**留着**：下一次重试从这里接着下，而不是从 0 重来。
                        # 用户主动取消的情况由后面的 was_cancelled 分支负责删除。
                        keep_partial = False
                        if resume_enabled and os.path.exists(temp_path):
                            with contextlib.suppress(OSError):
                                partial_bytes = os.path.getsize(temp_path)
                                if 0 < partial_bytes < (size or 0):
                                    keep_partial = True
                                    logger.info(
                                        "Keeping %d bytes of %s for the next attempt to resume from",
                                        partial_bytes, temp_path)
                        if not keep_partial:
                            with contextlib.suppress(OSError):
                                if os.path.exists(temp_path):
                                    os.remove(temp_path)
                        if cancelled:
                            raise asyncio.TimeoutError("Download was cancelled")
                        raise
                    await log_reply(message, "{0} ready in {1}".format(filename, file_category))
                    logger.info(f"Download completed: {filename} saved to {download_path}")

                    # 获取实际文件大小
                    actual_size = os.path.getsize(download_path)
                    
                    # 生成缩略图
                    thumbnail_path = generate_thumbnail(download_path, file_category)
                    
                    # Update download record as completed
                    async with db_lock:
                        cursor.execute('''
                        UPDATE downloads SET status = ?, progress = 100.0, size = ?, thumbnail_path = ?, end_time = CURRENT_TIMESTAMP WHERE id = ?
                        ''', ('completed', actual_size, thumbnail_path, download_id))
                        conn.commit()
                    logger.info(f"Updated download record: ID={download_id}, Status=completed, Size={actual_size}")

                    # 成功完成后清理取消注册表
                    if download_id is not None:
                        active_download_tasks.pop(str(download_id), None)
                        cancelled_download_ids.discard(str(download_id))
                        paused_download_ids.pop(str(download_id), None)

                    # Update status after download completes
                    emit_status_update()

                    worker_queue.task_done()
                except Exception as e:
                    # 捕获所有异常，确保任务不会永久卡住
                    error_msg = str(e)
                    logger.error(f"Download failed: {filename} - {error_msg}")
                    with contextlib.suppress(Exception):
                        with sync_lock:
                            in_progress.pop(str(download_id), None)
                            global web_in_progress
                            web_in_progress = in_progress

                    # 检查任务是否因为用户主动取消而失败；如果是，直接标记 cancelled，不进入重试逻辑
                    was_cancelled = False
                    if download_id is not None and str(download_id) in cancelled_download_ids:
                        was_cancelled = True
                        cancelled_download_ids.discard(str(download_id))
                    # 清理暂停状态（如果在暂停期间被取消）
                    paused_download_ids.pop(str(download_id), None)

                    if was_cancelled:
                        if download_id:
                            async with db_lock:
                                cursor.execute(
                                    '''
                                    UPDATE downloads SET status = ?, error_message = ?, temp_path = NULL, end_time = CURRENT_TIMESTAMP WHERE id = ?
                                    ''',
                                    ('cancelled', 'Cancelled by user', download_id)
                                )
                                conn.commit()
                            logger.info(f"Download cancelled by user: ID={download_id}, filename={filename}")
                        # 用户主动取消：半成品没有保留价值，删掉
                        with contextlib.suppress(Exception):
                            temp_file_path = build_safe_path(tempFolder, f"{filename}.{TELEGRAM_DAEMON_TEMP_SUFFIX}")
                            if os.path.exists(temp_file_path):
                                os.remove(temp_file_path)
                        # 清理任务注册表
                        if download_id is not None:
                            active_download_tasks.pop(str(download_id), None)
                            paused_download_ids.pop(str(download_id), None)
                        emit_status_update()
                        if worker_queue is not None:
                            worker_queue.task_done()
                        continue

                    # 获取当前重试次数
                    current_retry = 0
                    if download_id:
                        async with db_lock:
                            cursor.execute('SELECT retry_count FROM downloads WHERE id = ?', (download_id,))
                            result = cursor.fetchone()
                            if result:
                                current_retry = result[0] or 0

                    # 这一轮实际往前推进了多少字节？开了续传之后，"失败但下了 800MB" 和
                    # "失败且原地踏步" 是两回事：前者再来几轮就下完了，不该消耗重试次数，
                    # 否则大文件仍旧会在第 3 次重试后被判死刑。
                    # 门槛设成 MEANINGFUL_PROGRESS_BYTES，避免每轮只挪几个字节从而无限重试。
                    MEANINGFUL_PROGRESS_BYTES = 8 * 1024 * 1024
                    progressed_bytes = 0
                    if resume_enabled and temp_path:
                        with contextlib.suppress(OSError):
                            if os.path.exists(temp_path):
                                progressed_bytes = max(0, os.path.getsize(temp_path) - resume_from)
                    made_progress = progressed_bytes >= MEANINGFUL_PROGRESS_BYTES

                    # 检查是否可以重试
                    if made_progress or current_retry < max_retries:
                        # 有实质进展的这一轮不计入重试次数
                        new_retry = current_retry if made_progress else current_retry + 1
                        if made_progress:
                            retry_note = (
                                f"Resuming (+{progressed_bytes // (1024 * 1024)} MB this attempt): {error_msg}")
                        else:
                            retry_note = f"Retry {new_retry}: {error_msg}"
                        if download_id:
                            async with db_lock:
                                cursor.execute('''
                                UPDATE downloads SET status = 'queued', retry_count = ?, error_message = ? WHERE id = ?
                                ''', (new_retry, retry_note, download_id))
                                conn.commit()
                            logger.info(
                                f"Requeued {filename}: retry {new_retry}/{max_retries}, "
                                f"+{progressed_bytes} bytes this attempt")

                        # 重新加入队列；保留 5 元素结构，避免 monitor_queue_health / api_tasks 漏掉 queued_at
                        await asyncio.sleep(5)  # 等待5秒后重试
                        await push_queue_item([message_obj, message, target_dir_override, download_id, time.time()])

                        if made_progress:
                            await log_reply(
                                message,
                                f"⏳ 续传中（本轮 +{progressed_bytes // (1024 * 1024)} MB）: {filename}")
                        else:
                            await log_reply(message, f"⚠️ Retry {new_retry}/{max_retries}: {filename}")
                    else:
                        # 重试次数用完，标记为失败并通知
                        if download_id:
                            async with db_lock:
                                cursor.execute('''
                                UPDATE downloads SET status = ?, error_message = ?, retry_count = ?, end_time = CURRENT_TIMESTAMP WHERE id = ?
                                ''', ('failed', error_msg, current_retry, download_id))
                                conn.commit()
                            logger.info(f"Updated download record: ID={download_id}, Status=failed after {current_retry} retries")
                        
                        # 发送失败通知到 Telegram
                        if notify_failure:
                            failure_msg = f"❌ {filename} 下载失败（已重试{max_retries}次）\n原因: {error_msg[:200]}"
                            try:
                                # 回复原始文件消息，让用户直观看到失败的文件。
                                # 通过链接抓回来的外部消息不能直接回复（会发进别人的频道），
                                # notify_about_message 会把通知改发到被监听频道。
                                await notify_about_message(message_obj, failure_msg)
                            except Exception as reply_error:
                                logger.error(f'Error sending failure reply: {reply_error}')
                    
                    # Update status after download fails
                    emit_status_update()
                    if worker_queue is not None:
                        worker_queue.task_done()
                    # 失败/重试分支都清理 active_download_tasks；重试新创建任务时会重新注册
                    if download_id is not None:
                        active_download_tasks.pop(str(download_id), None)
                        paused_download_ids.pop(str(download_id), None)

        tasks = []
        loop = asyncio.get_running_loop()

        dynamic_worker_count = max(1, int(worker_count))
        logger.info(f"Worker分配：动态共享worker={dynamic_worker_count}，按队列积压自动取图/视频/其他任务")

        queue_monitor_task = loop.create_task(monitor_queue_health())
        tasks.append(queue_monitor_task)

        for i in range(dynamic_worker_count):
            task = loop.create_task(worker(i + 1))
            tasks.append(task)
        
        await sendHelloMessage(client, peerChannel)
        await client.run_until_disconnected()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    main_loop.run_until_complete(start(initial_auth_message))
    
    # Disconnect the client when done
    disconnect_client_and_loop(main_loop, client)
    logger.info("Telegram client disconnected")
except SingleInstanceLockError as e:
    logger.error(str(e))
    raise SystemExit(1)
except AuthKeyDuplicatedError:
    logger.error(
        "Telegram invalidated this session because the same auth key appeared from multiple IP addresses. Switching the web UI back to login mode.",
        exc_info=True,
    )
    relogin_message = handle_auth_key_duplicated_recovery()
    if 'main_loop' in locals():
        disconnect_client_and_loop(main_loop, locals().get('client'))
    handle_interrupted_tasks()

    main_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(main_loop)
    initial_auth_message = connect_client_with_recovery(main_loop) or relogin_message
    ensure_web_server_started()
    logger.info("Telegram client restarted in re-login mode after AUTH_KEY_DUPLICATED")
    main_loop.run_until_complete(start(initial_auth_message))
    disconnect_client_and_loop(main_loop, client)
    logger.info("Telegram client disconnected after re-login flow")
except Exception as e:
    logger.error(f"Critical error: {e}", exc_info=True)
    # Disconnect the client if an error occurs
    if 'main_loop' in locals():
        disconnect_client_and_loop(main_loop, locals().get('client'))
        logger.info("Telegram client disconnected due to error")
    raise
finally:
    releaseProcessLock()
