"""tdd_utils — 纯工具函数，单独抽出来方便测试。

注意：这里只能放"真正纯粹"、不依赖全局状态的函数。依赖数据库 / Telegram 客户端 /
asyncio loop 的代码请保留在 telegram-download-daemon.py 中。
"""

from __future__ import annotations

import math
import os
import random
import string


# ---------------------------------------------------------------------------
# 文件类型归类规则
# ---------------------------------------------------------------------------
# 下载完成后按扩展名把文件归类到不同子目录。规则以小写扩展名匹配。
FILE_TYPE_RULES = {
    'IGNORE': ['part', 'desktop'],
    'Music': ['mp3', 'aac', 'flac', 'ogg', 'wma', 'm4a', 'aiff', 'wav', 'amr'],
    'Videos': ['flv', 'ogv', 'avi', 'mp4', 'mpg', 'mpeg', '3gp', 'mkv', 'ts', 'webm', 'vob', 'wmv', 'srt'],
    'Pictures': ['png', 'jpeg', 'jpg', 'gif', 'bmp', 'svg', 'webp', 'psd', 'tiff', 'heic', 'heif'],
    'Archives': ['rar', 'zip', '7z', 'gz', 'bz2', 'tar', 'tgz', 'xz', 'iso', 'cpio',
                 'zst', 'lz', 'lzma',
                 'tar.gz', 'tar.bz2', 'tar.xz', 'tar.zst', 'tar.lz', 'tar.lzma'],
    'Documents': ['txt', 'pdf', 'doc', 'docx', 'odf', 'xls', 'xlsv', 'xlsx', 'ppt', 'pptx', 'ppsx', 'odp', 'odt', 'ods', 'md', 'json', 'csv'],
    'Books': ['mobi', 'epub', 'chm'],
    'DEBPackages': ['deb'],
    'Programs': ['exe', 'msi'],
    'RPMPackages': ['rpm'],
    'Mac': ['dmg', 'pkg'],
    'Linux': ['sh', 'rpm', 'deb'],
    'Android': ['apk'],
}

# 已知的复合扩展名（按长度优先匹配），用于正确识别 .tar.gz 这类形式
_COMPOUND_EXTENSIONS = {
    'tar.gz', 'tar.bz2', 'tar.xz', 'tar.zst', 'tar.lz', 'tar.lzma',
}
_ARCHIVE_COMPOUND = {'gz', 'bz2', 'xz', 'zst', 'lz', 'lzma'}


def getFileTypeCategory(filename: str) -> str:
    """根据扩展名判断文件类别，能正确处理 .tar.gz 这类复合扩展名。

    匹配不到任何已知类别时返回 ``'Other'``；命中忽略列表返回 ``'IGNORE'``。
    """
    name = (filename or "").lower()
    parts = name.split('.') if '.' in name else [name, '']

    # 先尝试复合扩展名（最后两段）
    if len(parts) >= 3:
        compound = f"{parts[-2]}.{parts[-1]}"
        if compound in _COMPOUND_EXTENSIONS:
            ext = compound
        elif parts[-2] == 'tar' and parts[-1] in _ARCHIVE_COMPOUND:
            ext = compound
        else:
            ext = parts[-1]
    else:
        ext = parts[-1] if len(parts) > 1 else ''

    # 先查忽略列表
    if ext in FILE_TYPE_RULES['IGNORE']:
        return 'IGNORE'

    # 再逐类别匹配
    for category, extensions in FILE_TYPE_RULES.items():
        if category != 'IGNORE' and ext in extensions:
            return category

    return 'Other'


def normalize_pagination(page, per_page, default_per_page: int = 10,
                         max_per_page: int = 100):
    """把外部传入的分页参数归一化为安全可用的 ``(page, per_page, offset)``。

    防御点：
    - ``page`` 至少为 1，非法/缺失回退到 1；
    - ``per_page`` 落在 ``[1, max_per_page]`` 区间，避免 ``0``（除零错误）
      或负数（SQLite 下 ``LIMIT -1`` 会返回全表）造成的崩溃 / DoS；
    - 非整数（含 ``None`` / 字符串）回退到默认值。
    """
    def _to_int(value, fallback):
        try:
            return int(value)
        except (TypeError, ValueError):
            return fallback

    page = _to_int(page, 1)
    per_page = _to_int(per_page, default_per_page)

    if page < 1:
        page = 1
    if per_page < 1:
        per_page = default_per_page
    if per_page > max_per_page:
        per_page = max_per_page

    offset = (page - 1) * per_page
    return page, per_page, offset


def compute_total_pages(total: int, per_page: int) -> int:
    """根据总条数与每页大小计算总页数，安全处理 ``per_page <= 0`` 的边界。"""
    if per_page <= 0:
        return 0
    return math.ceil(max(total, 0) / per_page)


# Windows 保留名（不含大小写），禁止直接作为文件名
WINDOWS_RESERVED_NAMES = {
    "CON", "PRN", "AUX", "NUL",
    *(f"COM{i}" for i in range(1, 10)),
    *(f"LPT{i}" for i in range(1, 10)),
}


def getRandomId(length: int) -> str:
    chars = string.ascii_lowercase + string.digits
    return "".join(random.choice(chars) for _ in range(length))


def sanitize_filename(filename: str) -> str:
    """把用户提供的文件名规整成**仅文件名**、无路径穿越、无控制字符的形式。

    - 去掉路径分隔符与盘符冒号，阻止 ``../foo``、``C:\\evil`` 之类的穿越尝试；
    - 去掉控制字符（含 DEL）；
    - 过滤掉几个会触发 Windows 路径解析的特殊字符 (<, >, |, ?, *)；
    - 拒绝 Windows 保留名（CON / PRN / COM1 ...）；
    - 去掉结尾的 "." / 空格（Windows 会自动去掉，容易和已有文件冲突）；
    - 空值 / 只剩 "." / ".." 时回退成随机名；
    - 总长度按 UTF-8 字节数截到 255。
    """
    original = filename or ""
    # 先把所有路径分隔符换成 "_"，免得 "../" 被吃成 ".."
    cleaned = original.replace("\\", "_").replace("/", "_")

    # 丢掉控制字符（0-31 与 127）和 Windows 禁用字符
    forbidden = {"<", ">", ":", "\"", "|", "?", "*"}
    buf = []
    for ch in cleaned:
        code = ord(ch)
        if code < 32 or code == 127:
            continue
        if ch in forbidden:
            continue
        buf.append(ch)
    cleaned = "".join(buf).strip()

    # 去掉结尾的 "." 和空格
    cleaned = cleaned.rstrip(" .")

    # 仍然以 "." / ".." 作为全部内容？拒绝。
    if cleaned in ("", ".", ".."):
        return f"file_{getRandomId(8)}"

    # 拒绝 Windows 保留名（含扩展名的也要看主名部分）
    stem, ext = os.path.splitext(cleaned)
    if stem.upper() in WINDOWS_RESERVED_NAMES:
        cleaned = f"file_{getRandomId(4)}{ext}"

    # 截断到 255 字节（用 UTF-8 字节数避免中文名被腰斩成乱码）
    encoded = cleaned.encode("utf-8")
    if len(encoded) > 255:
        name, ext = os.path.splitext(cleaned)
        ext_bytes = ext.encode("utf-8")
        name_bytes = name.encode("utf-8")[: max(0, 255 - len(ext_bytes))]
        # 防止在多字节字符中间切断
        cleaned = name_bytes.decode("utf-8", errors="ignore") + ext

    return cleaned


def build_safe_path(base_dir: str, *parts: str) -> str:
    """拼路径并确保落在 ``base_dir`` 之内，否则抛 ValueError。"""
    base_dir_abs = os.path.abspath(base_dir)
    candidate = os.path.abspath(os.path.join(base_dir_abs, *parts))
    if os.path.commonpath([base_dir_abs, candidate]) != base_dir_abs:
        raise ValueError(f"Refusing to access path outside base directory: {candidate}")
    return candidate


def ensure_existing_path_within(base_dir: str, target_path: str) -> str:
    """校验一个已存在的绝对路径确实在 ``base_dir`` 之内。"""
    base_dir_abs = os.path.abspath(base_dir)
    candidate = os.path.abspath(target_path)
    if os.path.commonpath([base_dir_abs, candidate]) != base_dir_abs:
        raise ValueError(f"Refusing to access existing path outside base directory: {candidate}")
    return candidate
