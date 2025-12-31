#!/usr/bin/env python3
# Telegram Download Daemon
# Author: Alfonso E.M. <alfonso@el-magnifico.org>
# You need to install telethon (and cryptg to speed up downloads)

from os import getenv, path
from shutil import move
import subprocess
import math
import time
import random
import string
import os.path
import threading
import sqlite3
from typing import Dict, List, Any, Optional, Tuple
from mimetypes import guess_extension
import socks
from flask import Flask, jsonify, render_template_string, request, send_file
from flask_socketio import SocketIO

from sessionManager import getSession, saveSession

from telethon import TelegramClient, events, __version__
from telethon.tl.types import PeerChannel, DocumentAttributeFilename, DocumentAttributeVideo
from telethon.network.connection import ConnectionTcpMTProxyRandomizedIntermediate
from telethon.network.connection import ConnectionTcpFull
from telethon.network.connection import ConnectionTcpObfuscated
from telethon.sessions import StringSession
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

parser = argparse.ArgumentParser(
    description="Script to download files from a Telegram Channel.")
parser.add_argument(
    "--proxy-host",
    required=TELEGRAM_DAEMON_PROXY_HOST == None,
    type=str,
    default=TELEGRAM_DAEMON_PROXY_HOST,
    help=
    'Proxy host to use for Telegram connection (default is TELEGRAM_DAEMON_PROXY_HOST env var)'
)
parser.add_argument(
    "--proxy-port",
    required=TELEGRAM_DAEMON_PROXY_PORT == None,
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
updateFrequency = 10
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
    
# 建议修改后的代码片段
if args.proxy_username and args.proxy_password:
    proxy = (
        socks.SOCKS5,
        args.proxy_host,
        int(args.proxy_port),
        False,
        args.proxy_username,
        args.proxy_password
    )
    print(f"Using proxy: socks5://{args.proxy_username}:******@{args.proxy_host}:{args.proxy_port}")
else:
    proxy = (
        socks.SOCKS5,
        args.proxy_host,
        int(args.proxy_port),
        False
    )
    print(f"Using proxy without auth: socks5://{args.proxy_host}:{args.proxy_port}")

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
        start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        end_time TIMESTAMP,
        error_message TEXT
    )
    ''')
    conn.commit()
    logger.info("Downloads table created or already exists")
except Exception as e:
    logger.error(f"Database initialization error: {e}")
    raise

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

# Function to emit status update event
def emit_status_update():
    try:
        # Calculate uptime (not needed for status update)
        
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
    
    # Read template from file
    template_path = os.path.join(os.path.dirname(__file__), 'templates', 'index.html')
    with open(template_path, 'r') as f:
        template_content = f.read()
    
    return render_template_string(template_content, version=TDD_VERSION, proxy=proxy_info, telegram_user=telegram_user)

@app.route('/api/status')
def api_status():
    try:
        global start_time, web_in_progress, web_queue_items
        
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
            'total_tasks': total_tasks
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
            try:
                local_conn = sqlite3.connect(DB_PATH, check_same_thread=False)
                local_cursor = local_conn.cursor()
                local_cursor.execute('SELECT size FROM downloads WHERE filename = ? AND status = ?', (filename, 'downloading'))
                result = local_cursor.fetchone()
                if result:
                    size = result[0]
                local_cursor.close()
                local_conn.close()
            except Exception as e:
                logger.error(f'Error getting size for active download: {e}', exc_info=True)
            
            tasks.append({
                'filename': filename,
                'status': 'downloading',
                'progress': progress,
                'downloadTime': time.strftime('%Y-%m-%d %H:%M:%S'),
                'size': size
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
                'size': size
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
        SELECT id, filename, file_type, status, size, progress, download_path, start_time, end_time, error_message
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
                'start_time': row[7],
                'end_time': row[8],
                'error_message': row[9]
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
        
        file_path = result[0]
        
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

@app.route('/api/delete', methods=['DELETE'])
def api_delete():
    try:
        # Get parameters
        task_id = request.args.get('task_id', type=str)
        filename = request.args.get('filename', type=str)
        
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
        
        file_path = result[0]
        
        # Delete file from disk if it exists
        if os.path.exists(file_path):
            os.remove(file_path)
            logger.info(f'Deleted file: {file_path}')
        
        # Delete record from database
        local_cursor.execute('DELETE FROM downloads WHERE id = ?', (actual_task_id,))
        local_conn.commit()
        logger.info(f'Deleted download record: {actual_task_id}')
        
        # Close the local connection
        local_cursor.close()
        local_conn.close()
        
        return jsonify({'success': True, 'message': 'File deleted successfully'})
    except Exception as e:
        logger.error(f'API delete error: {e}', exc_info=True)
        return jsonify({'error': 'Internal server error'}), 500

@app.route('/api/rename', methods=['POST'])
def api_rename():
    try:
        # Get parameters
        task_id = request.args.get('task_id', type=str)
        filename = request.args.get('filename', type=str)
        new_filename = request.args.get('new_filename', type=str)
        
        if not task_id or not filename or not new_filename:
            return jsonify({'error': 'Missing task_id, filename, or new_filename parameter'}), 400
        
        # Extract actual task id from task_id string (e.g., "history-123" -> "123")
        actual_task_id = task_id.split('-')[-1]
        
        # Create a new connection for this request to ensure thread safety
        local_conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        local_cursor = local_conn.cursor()
        
        # Get file path from database
        local_cursor.execute('SELECT download_path, file_type FROM downloads WHERE id = ?', (actual_task_id,))
        result = local_cursor.fetchone()
        
        if not result:
            local_cursor.close()
            local_conn.close()
            return jsonify({'error': 'File not found in database'}), 404
        
        old_file_path = result[0]
        file_type = result[1]
        
        # Check if file exists
        if not os.path.exists(old_file_path):
            local_cursor.close()
            local_conn.close()
            return jsonify({'error': 'File not found on disk'}), 404
        
        # Get directory path and extension
        dir_path = os.path.dirname(old_file_path)
        old_extension = os.path.splitext(old_file_path)[1]
        
        # Create new file path with same extension
        new_file_path = os.path.join(dir_path, new_filename)
        
        # Rename file on disk
        os.rename(old_file_path, new_file_path)
        logger.info(f'Renamed file: {old_file_path} -> {new_file_path}')
        
        # Update filename in database
        local_cursor.execute('UPDATE downloads SET filename = ? WHERE id = ?', (new_filename, actual_task_id))
        local_conn.commit()
        logger.info(f'Updated download record filename: {actual_task_id} -> {new_filename}')
        
        # Close the local connection
        local_cursor.close()
        local_conn.close()
        
        return jsonify({'success': True, 'message': 'File renamed successfully'})
    except Exception as e:
        logger.error(f'API rename error: {e}', exc_info=True)
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
    await message.edit(reply)

def getRandomId(length: int) -> str:
    chars = string.ascii_lowercase + string.digits
    return ''.join(random.choice(chars) for _ in range(length))
 

def getFilename(event: events.NewMessage.Event) -> str:
    mediaFileName = "unknown"

    if hasattr(event.media, 'photo'):
        mediaFileName = f"{event.media.photo.id}.jpeg"
    elif hasattr(event.media, 'document'):
        # 优先使用文件名属性
        for attribute in event.media.document.attributes:
            if isinstance(attribute, DocumentAttributeFilename): 
                mediaFileName = attribute.file_name
                break      
        # 如果没有文件名属性，尝试使用其他方式
        if mediaFileName == "unknown":
            if event.original_update.message.message != '': 
                mediaFileName = event.original_update.message.message
            else:    
                mediaFileName = str(event.media.document.id)
            # 添加适当的扩展名
            extension = guess_extension(event.media.document.mime_type)
            if extension:
                mediaFileName += extension
    
    # 确保文件名安全，只允许字母、数字和常见的安全字符
    # 移除所有不安全的字符
    safe_chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-()[]{}!@#$%^&*+=,;:'\" \\/"
    mediaFileName = "".join(c for c in mediaFileName if c in safe_chars)
    
    # 确保文件名不为空
    if not mediaFileName or mediaFileName == ".":
        mediaFileName = f"file_{getRandomId(8)}"
    
    # 确保文件名不超过255个字符（常见的文件系统限制）
    if len(mediaFileName) > 255:
        name, ext = os.path.splitext(mediaFileName)
        mediaFileName = f"{name[:255-len(ext)]}{ext}"
      
    return mediaFileName


# 移除全局变量，将在 start 函数内部管理状态


try:
    logger.info(f"Starting Telegram Download Daemon v{TDD_VERSION}")
    logger.info(f"Using Telethon v{__version__}")
    logger.info(f"API ID: {api_id}, Channel ID: {channel_id}")
    logger.info(f"Download folder: {downloadFolder}, Temp folder: {tempFolder}")
    logger.info(f"Worker count: {worker_count}")
    
    # Log proxy configuration
    if proxy:
        if isinstance(proxy, tuple):
            logger.info(f"Using proxy: {proxy[1]}:{proxy[2]} with {'authentication' if len(proxy) > 4 and proxy[4] else 'no authentication'}")
        else:
            logger.info(f"Using proxy: {proxy.get('addr')}:{proxy.get('port')} with {'authentication' if proxy.get('username') else 'no authentication'}")
    else:
        logger.info("No proxy configured")
    
    # Create and start client without with statement
    client = TelegramClient(getSession(), api_id, api_hash, proxy=proxy)
    client.start()
    
    # Save session
    saveSession(client.session)
    logger.info("Telegram client session saved")
    
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
        
        # Link web variables to local variables
        global web_in_progress, web_queue_items, telegram_user_info
        web_in_progress = in_progress
        web_queue_items = queue_items
        
        # Get telegram user info in the main loop
        try:
            me = await client.get_me()
            telegram_user_info = {
                'username': me.username,
                'first_name': me.first_name,
                'last_name': me.last_name or ''
            }
            logger.info(f"Telegram user: {me.username} ({me.first_name} {me.last_name})")
        except Exception as e:
            logger.error(f"Failed to get telegram user info: {e}")
            telegram_user_info = None
        
        # 内部的 set_progress 函数，使用闭包访问状态
        async def set_progress(filename, message, received, total):
            nonlocal lastUpdate
            
            async with status_lock:
                if received >= total:
                    try: 
                        in_progress.pop(filename)
                    except: 
                        pass
                    return
                
                percentage = math.trunc(received / total * 10000) / 100
                progress_message = "{0} % ({1} / {2})".format(percentage, received, total)
                in_progress[filename] = progress_message

                currentTime = time.time()
                if (currentTime - lastUpdate) > updateFrequency:
                    await log_reply(message, progress_message)
                    lastUpdate = currentTime
        
        @client.on(events.NewMessage())
        async def handler(event):
            if event.to_id != peerChannel:
                return

            logger.debug(f"Received new message event: {event}")
            
            try:
                # 先检查是否是媒体消息，因为媒体消息可能同时包含文本（比如分享多个媒体时）
                if event.media:
                    if hasattr(event.media, 'document') or hasattr(event.media,'photo'):
                        filename=getFilename(event)
                        if ( path.exists("{0}/{1}.{2}".format(tempFolder,filename,TELEGRAM_DAEMON_TEMP_SUFFIX)) or path.exists("{0}/{1}".format(downloadFolder,filename)) ) and duplicates == "ignore":
                            message=await event.reply("{0} already exists. Ignoring it.".format(filename))
                            logger.info(f"Ignoring duplicate file: {filename}")
                        else:
                            message=await event.reply("{0} added to queue".format(filename))
                            queue_item = [event, message]
                            
                            # 根据文件类型决定放入哪个队列
                            is_photo = hasattr(event.media, 'photo')
                            is_video = False
                            if not is_photo and hasattr(event.media, 'document'):
                                for attribute in event.media.document.attributes:
                                    if isinstance(attribute, DocumentAttributeVideo):
                                        is_video = True
                                        break
                            
                            async with queue_lock:
                                if is_photo:
                                    await photo_queue.put(queue_item)
                                    photo_queue_items.append(queue_item)
                                    queue_items = photo_queue_items + video_queue_items + other_queue_items
                                elif is_video:
                                    await video_queue.put(queue_item)
                                    video_queue_items.append(queue_item)
                                    queue_items = photo_queue_items + video_queue_items + other_queue_items
                                else:
                                    await other_queue.put(queue_item)
                                    other_queue_items.append(queue_item)
                                    queue_items = photo_queue_items + video_queue_items + other_queue_items
                            
                            logger.info(f"Added file to queue: {filename}, type: {'photo' if is_photo else 'video' if is_video else 'other'}")
                            
                            # Send WebSocket notifications
                            socketio.emit('new_task', {
                                'filename': filename,
                                'status': 'queued',
                                'downloadTime': time.strftime('%Y-%m-%d %H:%M:%S')
                            })
                            # Update status
                            emit_status_update()
                    else:
                        message=await event.reply("That is not downloadable. Try to send it as a file.")
                        logger.info(f"Received non-downloadable media: {type(event.media)}")
                # 只有当消息不是媒体消息时，才检查是否是命令
                elif event.message:
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

        async def worker(worker_queue, queue_items_list):
            """Worker函数，处理特定类型的队列"""
            while True:
                download_id = None
                try:
                    element = await worker_queue.get()
                    # 从队列跟踪列表中移除元素
                    async with queue_lock:
                        if element in queue_items_list:
                            queue_items_list.remove(element)
                        # 更新合并后的队列列表
                        nonlocal queue_items
                        queue_items = photo_queue_items + video_queue_items + other_queue_items
                    event=element[0]
                    message=element[1]
                    # Update status after removing from queue
                    emit_status_update()

                    filename=getFilename(event)
                    fileName, fileExtension = os.path.splitext(filename)
                    tempfilename=fileName+"-"+getRandomId(8)+fileExtension

                    # Get file type category
                    file_category = getFileTypeCategory(filename)
                    logger.info(f"Processing file: {filename}, Category: {file_category}")
                    
                    # Create category directory if it doesn't exist
                    category_folder = os.path.join(downloadFolder, file_category)
                    if not os.path.exists(category_folder):
                        os.makedirs(category_folder)
                        logger.info(f"Created category folder: {category_folder}")

                    # Check for duplicates in the category folder
                    if path.exists("{0}/{1}.{2}".format(tempFolder,tempfilename,TELEGRAM_DAEMON_TEMP_SUFFIX)) or path.exists("{0}/{1}".format(category_folder,filename)):
                        if duplicates == "rename":
                           filename=tempfilename
                           logger.info(f"Renamed file to avoid duplicate: {filename}")
                        elif duplicates == "ignore":
                           logger.info(f"Ignoring duplicate file: {filename}")
                           queue.task_done()
                           continue

                    if hasattr(event.media, 'photo'):
                       size = 0
                       logger.info(f"Processing photo: {filename}")
                    else: 
                       size=event.media.document.size
                       logger.info(f"Processing document: {filename}, Size: {size} bytes")

                    # Insert download record into database
                    download_path = os.path.join(category_folder, filename)
                    async with db_lock:
                        cursor.execute('''
                        INSERT INTO downloads (filename, file_type, status, size, progress, download_path)
                        VALUES (?, ?, ?, ?, ?, ?)
                        ''', (filename, file_category, 'downloading', size, 0.0, download_path))
                        conn.commit()
                        download_id = cursor.lastrowid
                    logger.info(f"Inserted download record: ID={download_id}, Status=downloading")

                    await log_reply(
                        message,
                        "Downloading file {0} ({1} bytes) to {2}".format(filename, size, file_category)
                    )

                    # 进度回调函数不能是异步的，所以我们需要使用一个同步的包装器
                    def download_callback(received, total):
                        # 由于回调是同步的，我们不能直接await异步函数
                        # 但我们可以记录进度，然后在合适的时候更新
                        nonlocal lastUpdate, download_id
                        percentage = math.trunc(received / total * 10000) / 100
                        progress_message = "{0} % ({1} / {2})".format(percentage, received, total)
                        
                        with sync_lock:
                            in_progress[filename] = progress_message
                            
                            currentTime = time.time()
                            if (currentTime - lastUpdate) > updateFrequency:
                                # 我们不能在这里await，所以我们需要使用loop.create_task
                                asyncio.create_task(log_reply(message, progress_message))
                                lastUpdate = currentTime
                        
                        # Update progress in database
                        with sync_db_lock:
                            cursor.execute('''
                            UPDATE downloads SET progress = ? WHERE id = ?
                            ''', (percentage, download_id))
                            conn.commit()
                        if percentage % 10 == 0:  # Log every 10% progress
                            logger.info(f"Download progress: {filename} - {percentage}% ({received}/{total} bytes)")
                        
                        # Send WebSocket notification for download progress
                        socketio.emit('download_progress', {
                            'filename': filename,
                            'progress': percentage,
                            'received': received,
                            'total': total,
                            'status': 'downloading'
                        })
                        
                        # Update status when download starts
                        if percentage == 0.0:
                            emit_status_update()

                    # 添加超时处理，防止下载卡住
                    try:
                        await asyncio.wait_for(
                            client.download_media(
                                event.message, 
                                "{0}/{1}.{2}".format(tempFolder, filename, TELEGRAM_DAEMON_TEMP_SUFFIX), 
                                progress_callback = download_callback
                            ),
                            timeout=3600  # 1小时超时
                        )
                        await set_progress(filename, message, 100, 100)
                        move("{0}/{1}.{2}".format(tempFolder, filename, TELEGRAM_DAEMON_TEMP_SUFFIX), download_path)
                    except asyncio.TimeoutError:
                        # 清理临时文件
                        temp_file_path = "{0}/{1}.{2}".format(tempFolder, filename, TELEGRAM_DAEMON_TEMP_SUFFIX)
                        if os.path.exists(temp_file_path):
                            os.remove(temp_file_path)
                        raise
                    await log_reply(message, "{0} ready in {1}".format(filename, file_category))
                    logger.info(f"Download completed: {filename} saved to {download_path}")

                    # Update download record as completed
                    async with db_lock:
                        cursor.execute('''
                        UPDATE downloads SET status = ?, progress = 100.0, end_time = CURRENT_TIMESTAMP WHERE id = ?
                        ''', ('completed', download_id))
                        conn.commit()
                    logger.info(f"Updated download record: ID={download_id}, Status=completed")

                    # Update status after download completes
                    emit_status_update()
                    
                    worker_queue.task_done()
                except (OSError, IOError, ValueError, TypeError, asyncio.TimeoutError) as e:
                    try: 
                        error_msg = str(e)
                        await log_reply(message, f"Error: {error_msg}") # If it failed, inform the user about it.
                        logger.error(f"Download failed: {filename} - {error_msg}")
                        
                        # Update download record as failed
                        if download_id:
                            async with db_lock:
                                cursor.execute('''
                                UPDATE downloads SET status = ?, error_message = ?, end_time = CURRENT_TIMESTAMP WHERE id = ?
                                ''', ('failed', error_msg, download_id))
                                conn.commit()
                            logger.info(f"Updated download record: ID={download_id}, Status=failed")
                    except Exception as reply_error:
                        logger.error(f'Error sending reply: {reply_error}')
                    logger.error(f'Queue worker error: {e}', exc_info=True)
                    # Update status after download fails
                    emit_status_update()
                    worker_queue.task_done()
        
        tasks = []
        loop = asyncio.get_event_loop()
        
        # 根据用户要求分配worker：至少1个图片worker，至少1个其他类型worker，剩余的为视频worker
        if worker_count < 3:
            # 如果worker数不足3，每个类型至少1个
            photo_workers = 1
            other_workers = 1
            video_workers = max(0, worker_count - 2)
        else:
            # 至少1个图片worker，至少1个其他类型worker，剩余的为视频worker
            photo_workers = 1
            other_workers = 1
            video_workers = worker_count - 2
        
        logger.info(f"Worker分配：图片={photo_workers}, 视频={video_workers}, 其他={other_workers}")
        
        # 创建图片worker
        for i in range(photo_workers):
            task = loop.create_task(worker(photo_queue, photo_queue_items))
            tasks.append(task)
        
        # 创建视频worker
        for i in range(video_workers):
            task = loop.create_task(worker(video_queue, video_queue_items))
            tasks.append(task)
        
        # 创建其他类型worker
        for i in range(other_workers):
            task = loop.create_task(worker(other_queue, other_queue_items))
            tasks.append(task)
        
        await sendHelloMessage(client, peerChannel)
        await client.run_until_disconnected()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    client.loop.run_until_complete(start())
    
    # Disconnect the client when done
    client.disconnect()
    logger.info("Telegram client disconnected")
except Exception as e:
    logger.error(f"Critical error: {e}", exc_info=True)
    # Disconnect the client if an error occurs
    if client:
        client.disconnect()
        logger.info("Telegram client disconnected due to error")
    raise
