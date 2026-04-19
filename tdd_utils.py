"""tdd_utils — 纯工具函数，单独抽出来方便测试。

注意：这里只能放"真正纯粹"、不依赖全局状态的函数。依赖数据库 / Telegram 客户端 /
asyncio loop 的代码请保留在 telegram-download-daemon.py 中。
"""

from __future__ import annotations

import os
import random
import string


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
