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
import threading
import sqlite3
import glob
from mimetypes import guess_extension
import socks
from flask import Flask, jsonify, render_template_string, request, send_file
from flask_socketio import SocketIO

from sessionManager import getSession, saveSession

from telethon import TelegramClient, events, __version__
from telethon.tl.types import PeerChannel, DocumentAttributeFilename, DocumentAttributeVideo
from telethon.errors import SessionPasswordNeededError
import logging

# Set up logging
logging.basicConfig(
    format='[%(levelname) 5s/%(asctime)s] %(name)s: %(message)s',
    level=logging.INFO  # Set to INFO level for more detailed logs
)
logger = logging.getLogger('telegram-download-daemon')

import multiprocessing
import argparse
import asyncio
import contextlib


TDD_VERSION="2.0"

TELEGRAM_DAEMON_API_ID = getenv("TELEGRAM_DAEMON_API_ID")
TELEGRAM_DAEMON_API_HASH = getenv("TELEGRAM_DAEMON_API_HASH")
TELEGRAM_DAEMON_CHANNEL = getenv("TELEGRAM_DAEMON_CHANNEL")

TELEGRAM_DAEMON_SESSION_PATH = getenv("TELEGRAM_DAEMON_SESSION_PATH")

TELEGRAM_DAEMON_DEST=getenv("TELEGRAM_DAEMON_DEST", "/telegram-downloads")
TELEGRAM_DAEMON_TEMP=getenv("TELEGRAM_DAEMON_TEMP", "")
TELEGRAM_DAEMON_DUPLICATES=getenv("TELEGRAM_DAEMON_DUPLICATES", "rename")

TELEGRAM_DAEMON_TEMP_SUFFIX="tdd"

TELEGRAM_DAEMON_WORKERS=getenv("TELEGRAM_DAEMON_WORKERS", multiprocessing.cpu_count())
TELEGRAM_DAEMON_PROXY_HOST=getenv("TELEGRAM_DAEMON_PROXY_HOST")
TELEGRAM_DAEMON_PROXY_PORT=getenv("TELEGRAM_DAEMON_PROXY_PORT")
TELEGRAM_DAEMON_PROXY_TYPE=getenv("TELEGRAM_DAEMON_PROXY_TYPE", "socks5")
TELEGRAM_DAEMON_PROXY_USERNAME=getenv("TELEGRAM_DAEMON_PROXY_USERNAME")
TELEGRAM_DAEMON_PROXY_PASSWORD=getenv("TELEGRAM_DAEMON_PROXY_PASSWORD")

# 可配置参数
TELEGRAM_DAEMON_DOWNLOAD_TIMEOUT=int(getenv("TELEGRAM_DAEMON_DOWNLOAD_TIMEOUT", "3600"))  # 下载超时，默认1小时
TELEGRAM_DAEMON_UPDATE_FREQUENCY=int(getenv("TELEGRAM_DAEMON_UPDATE_FREQUENCY", "10"))  # 进度更新频率，默认10秒
TELEGRAM_DAEMON_START_TIMEOUT=int(getenv("TELEGRAM_DAEMON_START_TIMEOUT", "120"))  # 开始下载超时，默认2分钟
TELEGRAM_DAEMON_NO_PROGRESS_TIMEOUT=int(getenv("TELEGRAM_DAEMON_NO_PROGRESS_TIMEOUT", "300"))  # 无进度超时，默认5分钟
TELEGRAM_DAEMON_MAX_RETRIES=int(getenv("TELEGRAM_DAEMON_MAX_RETRIES", "3"))  # 最大重试次数，默认3次
TELEGRAM_DAEMON_NOTIFY_FAILURE=bool(int(getenv("TELEGRAM_DAEMON_NOTIFY_FAILURE", "1")))  # 失败通知，默认开启

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
args = parser.parse_args()

api_id = args.api_id
api_hash = args.api_hash
channel_id = args.channel
downloadFolder = args.dest
tempFolder = args.temp
duplicates=args.duplicates
worker_count = args.workers
updateFrequency = TELEGRAM_DAEMON_UPDATE_FREQUENCY
download_timeout = TELEGRAM_DAEMON_DOWNLOAD_TIMEOUT
start_timeout = TELEGRAM_DAEMON_START_TIMEOUT
no_progress_timeout = TELEGRAM_DAEMON_NO_PROGRESS_TIMEOUT
max_retries = TELEGRAM_DAEMON_MAX_RETRIES
notify_failure = TELEGRAM_DAEMON_NOTIFY_FAILURE
lastUpdate = 0

if not tempFolder:
    tempFolder = downloadFolder
   
# Proxy configuration
connection = None
proxy = None
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
    
    # 根据是否有认证信息创建代理配置
    if args.proxy_username and args.proxy_password:
        proxy = (
            proxy_type_const,
            args.proxy_host,
            int(args.proxy_port),
            False,
            args.proxy_username,
            args.proxy_password
        )
        print(f"Using proxy: {proxy_type_str}://{args.proxy_username}:******@{args.proxy_host}:{args.proxy_port}")
    else:
        proxy = (
            proxy_type_const,
            args.proxy_host,
            int(args.proxy_port),
            False
        )
        print(f"Using proxy without auth: {proxy_type_str}://{args.proxy_host}:{args.proxy_port}")

# File Type Categorization Rules
FILE_TYPE_RULES = {
    'IGNORE': ['part', 'desktop'],
    'Music': ['mp3', 'aac', 'flac', 'ogg', 'wma', 'm4a', 'aiff', 'wav', 'amr'],
    'Videos': ['flv', 'ogv', 'avi', 'mp4', 'mpg', 'mpeg', '3gp', 'mkv', 'ts', 'webm', 'vob', 'wmv', 'srt'],
    'Pictures': ['png', 'jpeg', 'gif', 'jpg', 'bmp', 'svg', 'webp', 'psd', 'tiff'],
    'Archives': ['rar', 'zip', '7z', 'gz', 'bz2', 'tar', 'tgz', 'xz', 'iso', 'cpio'],
    'Documents': ['txt', 'pdf', 'doc', 'docx', 'odf', 'xls', 'xlsv', 'xlsx', 'ppt', 'pptx', 'ppsx', 'odp', 'odt', 'ods', 'md', 'json', 'csv'],
    'Books': ['mobi', 'epub', 'chm'],
    'DEBPackages': ['deb'],
    'Programs': ['exe', 'msi'],
    'RPMPackages': ['rpm'],
    'Mac': ['dmg', 'pkg'],
    'Linux': ['sh', 'rpm', 'deb'],
    'Android': ['apk']
}

# Function to get file type category
def getFileTypeCategory(filename):
    ext = filename.split('.')[-1].lower() if '.' in filename else ''
    
    # Check ignore list first
    if ext in FILE_TYPE_RULES['IGNORE']:
        return 'IGNORE'
    
    # Check each category
    for category, extensions in FILE_TYPE_RULES.items():
        if category != 'IGNORE' and ext in extensions:
            return category
    
    # Default to Other if no match
    return 'Other'

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
    """清理残留的临时文件"""
    try:
        temp_files = glob.glob(os.path.join(tempFolder, f"*.{TELEGRAM_DAEMON_TEMP_SUFFIX}"))
        for temp_file in temp_files:
            # 检查文件是否超过24小时（可能是残留文件）
            file_age = time.time() - os.path.getmtime(temp_file)
            if file_age > 86400:  # 24小时
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

# Initialize SocketIO
socketio = SocketIO(app, cors_allowed_origins="*")

# Global variables for Web Server
start_time = time.time()
web_client = None
web_in_progress = {}
web_queue_items = []
telegram_user_info = None
telegram_channel_info = None
web_retry_scheduler = None
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


def get_auth_send_code_remaining():
    remaining = auth_send_code_cooldown_until - time.time()
    if remaining <= 0:
        return 0
    return math.ceil(remaining)

# Function to emit status update event
def emit_status_update():
    try:
        # Get total historical tasks count
        total_tasks = 0
        try:
            result = db_execute_query('SELECT COUNT(*) FROM downloads', fetch=True)
            if result:
                total_tasks = result[0][0]
        except Exception as e:
            logger.error(f'Error getting total tasks count: {e}', exc_info=True)
        
        # Emit status update event
        socketio.emit('status_update', {
            'active_downloads': len(web_in_progress),
            'queue_size': len(web_queue_items),
            'total_tasks': total_tasks
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
        if isinstance(proxy, tuple):
            # Handle tuple format proxy
            proxy_info = {
                'type': proxy[0] if len(proxy) > 0 else 'socks5',
                'host': proxy[1] if len(proxy) > 1 else '',
                'port': proxy[2] if len(proxy) > 2 else '',
                'username': proxy[4] if len(proxy) > 4 else ''
            }
        else:
            # Handle dict format proxy
            proxy_info = {
                'type': proxy.get('proxy_type', 'socks5'),
                'host': proxy.get('addr', ''),
                'port': proxy.get('port', ''),
                'username': proxy.get('username', '')
            }
    
    # Get telegram user info (stored in a global variable that's updated when client starts)
    global telegram_user_info
    telegram_user = telegram_user_info
    auth_state = telegram_auth_state
    
    # Read template from file
    template_path = os.path.join(os.path.dirname(__file__), 'templates', 'index.html')
    with open(template_path, 'r') as f:
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
        
        # Get total historical tasks count
        total_tasks = 0
        try:
            local_conn = sqlite3.connect(DB_PATH, check_same_thread=False)
            local_cursor = local_conn.cursor()
            local_cursor.execute('SELECT COUNT(*) FROM downloads')
            total_tasks = local_cursor.fetchone()[0]
            local_cursor.close()
            local_conn.close()
        except Exception as e:
            logger.error(f'Error getting total tasks count: {e}', exc_info=True)
        
        return jsonify({
            'uptime': uptime,
            'active_downloads': len(web_in_progress),
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
        for filename, progress in web_in_progress.items():
            # Get file size from database for active downloads
            size = 0
            source_message_link = ""
            try:
                local_conn = sqlite3.connect(DB_PATH, check_same_thread=False)
                local_cursor = local_conn.cursor()
                local_cursor.execute(
                    'SELECT id, size, source_message_link FROM downloads WHERE filename = ? AND status = ? ORDER BY id DESC LIMIT 1',
                    (filename, 'downloading')
                )
                result = local_cursor.fetchone()
                if result:
                    size = result[1]
                    source_message_link = result[2] or ""
                local_cursor.close()
                local_conn.close()
            except Exception as e:
                logger.error(f'Error getting size for active download: {e}', exc_info=True)
            
            tasks.append({
                'filename': filename,
                'status': 'downloading',
                'progress': progress,
                'downloadTime': time.strftime('%Y-%m-%d %H:%M:%S'),
                'size': size,
                'source_message_link': source_message_link
            })
        
        # Add queued items
        for item in web_queue_items:
            event = item[0]
            filename = getFilename(event)
            # Get file size from event
            size = 0
            if hasattr(event.media, 'document'):
                size = event.media.document.size
            
            tasks.append({
                'filename': filename,
                'status': 'queued',
                'progress': 'Waiting for download',
                'downloadTime': None,
                'size': size,
                'source_message_link': build_message_link(event)
            })
        
        return jsonify({'tasks': tasks})
    except Exception as e:
        logger.error(f'API tasks error: {e}', exc_info=True)
        return jsonify({'error': 'Internal server error'}), 500

@app.route('/api/history')
def api_history():
    try:
        # Get pagination parameters
        page = request.args.get('page', 1, type=int)
        per_page = request.args.get('per_page', 10, type=int)
        
        # Get filter parameters
        filename = request.args.get('filename', None)
        file_type = request.args.get('file_type', None)
        status = request.args.get('status', None)
        
        # Calculate offset
        offset = (page - 1) * per_page
        
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
        
        # Get total count with filters
        count_query = f"SELECT COUNT(*) FROM downloads{where_clause}"
        local_cursor.execute(count_query, params)
        total = local_cursor.fetchone()[0]
        
        # Get historical downloads with filters
        select_query = f'''
        SELECT id, filename, file_type, status, size, progress, download_path, thumbnail_path, retry_count,
               source_channel_id, source_message_id, source_message_link, target_dir, start_time, end_time, error_message
        FROM downloads
        {where_clause}
        ORDER BY start_time DESC
        LIMIT ? OFFSET ?
        '''
        
        # Add pagination params
        query_params = params + [per_page, offset]
        local_cursor.execute(select_query, query_params)
        rows = local_cursor.fetchall()
        
        # Format response
        history = []
        for row in rows:
            history.append({
                'id': row[0],
                'filename': row[1],
                'file_type': row[2],
                'status': row[3],
                'size': row[4],
                'progress': row[5],
                'download_path': row[6],
                'thumbnail_path': row[7],
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
            'pages': math.ceil(total / per_page)
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
        local_cursor.execute('SELECT download_path FROM downloads WHERE id = ?', (actual_task_id,))
        result = local_cursor.fetchone()
        
        if not result:
            local_cursor.close()
            local_conn.close()
            return jsonify({'error': 'File not found in database'}), 404

        if not result[0]:
            local_cursor.close()
            local_conn.close()
            return jsonify({'error': 'File is not available for download yet'}), 409
        
        file_path = ensure_existing_path_within(downloadFolder, result[0])
        
        # Close the local connection
        local_cursor.close()
        local_conn.close()
        
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

        retry_result = web_retry_scheduler(int(source_channel_id), int(source_message_id), resolved_retry_dir)
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
        
        # Create a new connection for this request to ensure thread safety
        local_conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        local_cursor = local_conn.cursor()
        
        # Get file paths from database
        local_cursor.execute('SELECT download_path, thumbnail_path, filename FROM downloads WHERE id = ?', (actual_task_id,))
        result = local_cursor.fetchone()
        
        if not result:
            local_cursor.close()
            local_conn.close()
            return jsonify({'error': 'File not found in database'}), 404

        download_path, thumbnail_path, stored_filename = result

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

async def sendHelloMessage(client: TelegramClient, peerChannel: PeerChannel) -> None:
    entity = await client.get_entity(peerChannel)
    print(f"Telegram Download Daemon {TDD_VERSION} using Telethon {__version__}")
    print(f"  Simultaneous downloads: {worker_count}")
    await client.send_message(entity, f"Telegram Download Daemon {TDD_VERSION} using Telethon {__version__}")
    await client.send_message(entity, "Hi! Ready for your files!")
 

async def log_reply(message: events.NewMessage.Event, reply: str) -> None:
    print(reply)
    if message is not None:
        await message.edit(reply)

def getRandomId(length: int) -> str:
    chars = string.ascii_lowercase + string.digits
    return ''.join(random.choice(chars) for _ in range(length))


def sanitize_filename(filename: str) -> str:
    filename = (filename or "").replace("\\", "_").replace("/", "_")
    safe_chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-()[]{}!@#$%^&*+=,;:'\" "
    filename = "".join(c for c in filename if c in safe_chars).strip()

    if not filename or filename in {".", ".."}:
        return f"file_{getRandomId(8)}"

    name, ext = os.path.splitext(filename)
    if len(filename) > 255:
        filename = f"{name[:255-len(ext)]}{ext}"

    return filename


def build_safe_path(base_dir: str, *parts: str) -> str:
    base_dir_abs = os.path.abspath(base_dir)
    candidate = os.path.abspath(os.path.join(base_dir_abs, *parts))
    if os.path.commonpath([base_dir_abs, candidate]) != base_dir_abs:
        raise ValueError(f"Refusing to access path outside base directory: {candidate}")
    return candidate


def ensure_existing_path_within(base_dir: str, target_path: str) -> str:
    base_dir_abs = os.path.abspath(base_dir)
    candidate = os.path.abspath(target_path)
    if os.path.commonpath([base_dir_abs, candidate]) != base_dir_abs:
        raise ValueError(f"Refusing to access existing path outside base directory: {candidate}")
    return candidate


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


# 移除全局变量，将在 start 函数内部管理状态


try:
    logger.info(f"Starting Telegram Download Daemon v{TDD_VERSION}")
    logger.info(f"Using Telethon v{__version__}")
    logger.info(f"API ID: {api_id}, Channel ID: {channel_id}")
    logger.info(f"Download folder: {downloadFolder}, Temp folder: {tempFolder}")
    logger.info(f"Worker count: {worker_count}")
    logger.info(f"Download timeout: {download_timeout}s, Start timeout: {start_timeout}s, Update frequency: {updateFrequency}s, Max retries: {max_retries}, Notify failure: {notify_failure}")
    
    # 清理残留的临时文件
    cleanup_temp_files()
    
    # 处理中断的任务
    handle_interrupted_tasks()
    
    # Log proxy configuration
    if proxy:
        if isinstance(proxy, tuple):
            logger.info(f"Using proxy: {proxy[1]}:{proxy[2]} with {'authentication' if len(proxy) > 4 and proxy[4] else 'no authentication'}")
        else:
            logger.info(f"Using proxy: {proxy.get('addr')}:{proxy.get('port')} with {'authentication' if proxy.get('username') else 'no authentication'}")
    else:
        logger.info("No proxy configured")
    
    # Create client without interactive auth prompts
    main_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(main_loop)
    client = TelegramClient(getSession(), api_id, api_hash, proxy=proxy, loop=main_loop)
    main_loop.run_until_complete(client.connect())
    
    # Update web_client reference
    web_client = client
    logger.info("Telegram client initialized successfully")
    
    # Start Web Server in a separate thread
    web_server_thread = threading.Thread(target=run_web_server, daemon=True)
    web_server_thread.start()
    logger.info("Web server started on http://0.0.0.0:7373")

    async def start():
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
                rebuild_web_queue_items()
            return queue_type

        async def pop_next_queue_item():
            queue_getters = {
                asyncio.create_task(photo_queue.get()): (photo_queue, photo_queue_items, 'photo'),
                asyncio.create_task(video_queue.get()): (video_queue, video_queue_items, 'video'),
                asyncio.create_task(other_queue.get()): (other_queue, other_queue_items, 'other'),
            }

            done = set()
            pending = set()
            try:
                done, pending = await asyncio.wait(queue_getters.keys(), return_when=asyncio.FIRST_COMPLETED)
                completed_task = done.pop()
                queue_ref, queue_items_list_ref, queue_type = queue_getters[completed_task]
                element = completed_task.result()

                async with queue_lock:
                    if element in queue_items_list_ref:
                        queue_items_list_ref.remove(element)
                    rebuild_web_queue_items()

                return element, queue_ref, queue_items_list_ref, queue_type
            finally:
                for pending_task in pending:
                    pending_task.cancel()
                if pending:
                    await asyncio.gather(*pending, return_exceptions=True)
                for done_task in done:
                    if not done_task.cancelled():
                        with contextlib.suppress(Exception):
                            done_task.result()

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

        await refresh_auth_state()
        if not telegram_auth_state.get('authorized'):
            logger.info("Telegram client is waiting for web login")
            await auth_ready_event.wait()
        
        # 内部的 set_progress 函数，使用闭包访问状态
        async def set_progress(filename, message, received, total):
            nonlocal lastUpdate
            
            async with status_lock:
                global web_in_progress
                if received >= total:
                    try: 
                        in_progress.pop(filename)
                        web_in_progress = in_progress
                    except: 
                        pass
                    return
                
                percentage = math.trunc(received / total * 10000) / 100
                progress_message = "{0} % ({1} / {2})".format(percentage, received, total)
                in_progress[filename] = progress_message
                web_in_progress = in_progress

                currentTime = time.time()
                if (currentTime - lastUpdate) > updateFrequency:
                    await log_reply(message, progress_message)
                    lastUpdate = currentTime

        async def persist_queued_download(message_obj, target_dir_override=None, existing_download_id=None, recovery_note=None):
            filename = getFilename(message_obj)
            file_category = getFileTypeCategory(filename)
            size = 0 if getattr(message_obj, 'photo', None) else (message_obj.document.size if getattr(message_obj, 'document', None) else 0)
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

        async def enqueue_download_message(message_obj, notice_template="{0} added to queue", target_dir_override=None, existing_download_id=None, silent=False, recovery_note=None):
            is_photo = getattr(message_obj, 'photo', None) is not None
            is_document = getattr(message_obj, 'document', None) is not None
            if not (is_photo or is_document):
                raise ValueError("That message does not contain a downloadable file")

            filename = getFilename(message_obj)
            temp_path = build_safe_path(tempFolder, f"{filename}.{TELEGRAM_DAEMON_TEMP_SUFFIX}")
            root_path = build_safe_path(downloadFolder, filename)
            if (path.exists(temp_path) or path.exists(root_path)) and duplicates == "ignore":
                status_message = None if silent else await message_obj.reply("{0} already exists. Ignoring it.".format(filename))
                logger.info(f"Ignoring duplicate file: {filename}")
                return {'queued': False, 'filename': filename, 'message': status_message}

            download_id = await persist_queued_download(
                message_obj,
                target_dir_override=target_dir_override,
                existing_download_id=existing_download_id,
                recovery_note=recovery_note
            )
            status_message = None if silent else await message_obj.reply(notice_template.format(filename))
            queue_item = [message_obj, status_message, target_dir_override, download_id]
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

        async def retry_download_message(source_channel_id, source_message_id, target_dir_override=None):
            message_obj = await client.get_messages(PeerChannel(source_channel_id), ids=source_message_id)
            if not message_obj:
                raise ValueError("Unable to locate the original Telegram message")
            return await enqueue_download_message(message_obj, "{0} re-added to queue", target_dir_override)

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

        def schedule_retry(source_channel_id, source_message_id, target_dir_override=None):
            future = asyncio.run_coroutine_threadsafe(
                retry_download_message(source_channel_id, source_message_id, target_dir_override),
                main_loop
            )
            return future.result(timeout=60)

        global web_retry_scheduler
        web_retry_scheduler = schedule_retry
        await restore_pending_downloads()
        
        @client.on(events.NewMessage())
        async def handler(event):
            if event.to_id != peerChannel:
                return

            logger.debug(f"Received new message event: {event}")
            
            try:
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
                            output = "".join([ "{0}: {1}\n".format(key,value) for (key, value) in in_progress.items()])
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
                try:
                    element, worker_queue, queue_items_list, queue_type = await pop_next_queue_item()
                    message_obj=element[0]
                    message=element[1]
                    target_dir_override = element[2] if len(element) > 2 else None
                    download_id = element[3] if len(element) > 3 else None
                    # Update status after removing from queue
                    emit_status_update()

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

                    # Check for duplicates in the category folder
                    temp_duplicate_path = build_safe_path(tempFolder, f"{tempfilename}.{TELEGRAM_DAEMON_TEMP_SUFFIX}")
                    final_duplicate_path = build_safe_path(category_folder, filename)
                    if path.exists(temp_duplicate_path) or path.exists(final_duplicate_path):
                        if duplicates == "rename":
                           filename=tempfilename
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

                    if getattr(message_obj, 'photo', None):
                       size = 0
                       logger.info(f"Processing photo: {filename}")
                    else: 
                       size=message_obj.document.size
                       logger.info(f"Processing document: {filename}, Size: {size} bytes")

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
                                    source_channel_id, source_message_id, source_message_link, target_dir
                                )
                                VALUES (?, ?, 'downloading', ?, 0.0, ?, ?, ?, ?, ?)
                                ''',
                                (
                                    filename, file_category, size, download_path, source_channel,
                                    source_message_id, source_message_link, target_dir_override
                                )
                            )
                            download_id = cursor.lastrowid
                        else:
                            cursor.execute(
                                '''
                                UPDATE downloads
                                SET filename = ?, file_type = ?, status = 'downloading', size = ?, progress = 0.0,
                                    download_path = ?, source_channel_id = ?, source_message_id = ?,
                                    source_message_link = ?, target_dir = ?, end_time = NULL
                                WHERE id = ?
                                ''',
                                (
                                    filename, file_category, size, download_path, source_channel,
                                    source_message_id, source_message_link, target_dir_override, download_id
                                )
                            )
                        conn.commit()
                    logger.info(f"Inserted download record: ID={download_id}, Status=downloading")

                    await log_reply(
                        message,
                        "Downloading file {0} ({1} bytes) to {2}".format(filename, size, file_category)
                    )

                    # 进度回调函数不能是异步的，所以我们需要使用一个同步的包装器
                    last_progress_time = [time.time()]
                    def download_callback(received, total):
                        # 由于回调是同步的，我们不能直接await异步函数
                        # 但我们可以记录进度，然后在合适的时候更新
                        nonlocal lastUpdate
                        percentage = math.trunc(received / total * 10000) / 100
                        progress_message = "{0} % ({1} / {2})".format(percentage, received, total)
                        last_progress_time[0] = time.time()
                        
                        with sync_lock:
                            in_progress[filename] = progress_message
                            # 确保全局变量同步
                            global web_in_progress
                            web_in_progress = in_progress
                            
                            currentTime = time.time()
                            if (currentTime - lastUpdate) > updateFrequency:
                                # 我们不能在这里await，所以我们需要使用loop.create_task
                                asyncio.create_task(log_reply(message, progress_message))
                                lastUpdate = currentTime
                        
                        # Update progress in database
                        if download_id:
                            with sync_db_lock:
                                cursor.execute('''
                                UPDATE downloads SET progress = ? WHERE id = ?
                                ''', (percentage, download_id))
                                conn.commit()
                        
                        # 每10%记录一次进度（使用整数判断避免浮点精度问题）
                        progress_int = int(percentage)
                        if progress_int > 0 and progress_int % 10 == 0 and abs(percentage - progress_int) < 0.01:
                            logger.info(f"Download progress: {filename} - {progress_int}% ({received}/{total} bytes)")
                        
                        # Send WebSocket notification for download progress
                        socketio.emit('download_progress', {
                            'filename': filename,
                            'progress': percentage,
                            'received': received,
                            'total': total,
                            'status': 'downloading'
                        })
                        
                        # Update status when download starts
                        if received > 0 and received < total * 0.01:  # 开始下载时
                            emit_status_update()

                    # 添加超时处理，防止下载卡住
                    # 使用两层超时：开始超时 + 总下载超时
                    download_started = [False]  # 使用列表让闭包能修改
                    
                    def check_start_callback(received, total):
                        if received > 0:
                            download_started[0] = True
                        download_callback(received, total)
                    
                    download_task = None
                    try:
                        # 创建下载任务
                        download_task = asyncio.create_task(
                            client.download_media(
                                message_obj,
                                build_safe_path(tempFolder, f"{filename}.{TELEGRAM_DAEMON_TEMP_SUFFIX}"),
                                progress_callback = check_start_callback
                            )
                        )
                        
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
                        while not download_task.done():
                            elapsed = time.time() - start_time
                            if elapsed > download_timeout:
                                raise asyncio.TimeoutError(f"Download exceeded {download_timeout} seconds")
                            if download_started[0] and (time.time() - last_progress_time[0]) > no_progress_timeout:
                                raise asyncio.TimeoutError(f"No download progress for {no_progress_timeout} seconds")
                            await asyncio.sleep(1)

                        await download_task
                        
                        await set_progress(filename, message, 100, 100)
                        move(build_safe_path(tempFolder, f"{filename}.{TELEGRAM_DAEMON_TEMP_SUFFIX}"), download_path)
                    except asyncio.TimeoutError as e:
                        if download_task is not None and not download_task.done():
                            download_task.cancel()
                            try:
                                await download_task
                            except asyncio.CancelledError:
                                pass
                        # 清理临时文件
                        temp_file_path = build_safe_path(tempFolder, f"{filename}.{TELEGRAM_DAEMON_TEMP_SUFFIX}")
                        if os.path.exists(temp_file_path):
                            os.remove(temp_file_path)
                        raise
                    except asyncio.CancelledError:
                        # 清理临时文件
                        temp_file_path = build_safe_path(tempFolder, f"{filename}.{TELEGRAM_DAEMON_TEMP_SUFFIX}")
                        if os.path.exists(temp_file_path):
                            os.remove(temp_file_path)
                        raise asyncio.TimeoutError("Download was cancelled")
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

                    # Update status after download completes
                    emit_status_update()
                    
                    worker_queue.task_done()
                except Exception as e:
                    # 捕获所有异常，确保任务不会永久卡住
                    error_msg = str(e)
                    logger.error(f"Download failed: {filename} - {error_msg}")
                    
                    # 获取当前重试次数
                    current_retry = 0
                    if download_id:
                        async with db_lock:
                            cursor.execute('SELECT retry_count FROM downloads WHERE id = ?', (download_id,))
                            result = cursor.fetchone()
                            if result:
                                current_retry = result[0] or 0
                    
                    # 检查是否可以重试
                    if current_retry < max_retries:
                        # 更新重试次数，状态改回 queued
                        new_retry = current_retry + 1
                        if download_id:
                            async with db_lock:
                                cursor.execute('''
                                UPDATE downloads SET status = 'queued', retry_count = ?, error_message = ? WHERE id = ?
                                ''', (new_retry, f"Retry {new_retry}: {error_msg}", download_id))
                                conn.commit()
                            logger.info(f"Retry {new_retry}/{max_retries} for: {filename}")
                        
                        # 重新加入队列
                        await asyncio.sleep(5)  # 等待5秒后重试
                        await push_queue_item([message_obj, message, target_dir_override, download_id])
                        
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
                            failure_msg = f"❌ 下载失败（已重试{max_retries}次）\n原因: {error_msg[:200]}"
                            try:
                                # 回复原始文件消息，让用户直观看到失败的文件
                                await message_obj.reply(failure_msg)
                            except Exception as reply_error:
                                logger.error(f'Error sending failure reply: {reply_error}')
                    
                    # Update status after download fails
                    emit_status_update()
                    if worker_queue is not None:
                        worker_queue.task_done()
        
        tasks = []
        loop = asyncio.get_running_loop()

        dynamic_worker_count = max(1, int(worker_count))
        logger.info(f"Worker分配：动态共享worker={dynamic_worker_count}，按队列积压自动取图/视频/其他任务")

        for i in range(dynamic_worker_count):
            task = loop.create_task(worker(i + 1))
            tasks.append(task)
        
        await sendHelloMessage(client, peerChannel)
        await client.run_until_disconnected()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    main_loop.run_until_complete(start())
    
    # Disconnect the client when done
    main_loop.run_until_complete(client.disconnect())
    main_loop.close()
    logger.info("Telegram client disconnected")
except Exception as e:
    logger.error(f"Critical error: {e}", exc_info=True)
    # Disconnect the client if an error occurs
    if 'main_loop' in locals() and 'client' in locals() and client:
        try:
            if not main_loop.is_closed():
                main_loop.run_until_complete(client.disconnect())
                main_loop.close()
        except Exception:
            pass
        logger.info("Telegram client disconnected due to error")
    raise
