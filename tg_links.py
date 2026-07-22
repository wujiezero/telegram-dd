"""tg_links —— Telegram 消息链接解析（纯字符串逻辑，方便单元测试）。

守护进程监听的频道里经常出现"别人转来的一条私密频道链接"，本模块负责把这些
链接从文本里抠出来并解析成 (目标会话, 消息 ID) 结构，真正的抓取交给
telegram-download-daemon.py 里的 Telethon 客户端完成。

覆盖的链接形态::

    https://t.me/c/1234567890/456          私密频道（c = 内部数字 ID）
    https://t.me/c/1234567890/12/456       私密频道 + 话题（forum topic）
    https://t.me/c/1234567890/456-460      私密频道 + 连续消息区间
    https://t.me/channelname/456           公开频道
    https://t.me/s/channelname/456         公开频道的网页预览形态
    https://t.me/+AbCdEfGh/456             邀请链接（未加入时需要先 join）
    https://t.me/joinchat/AbCdEfGh/456     旧版邀请链接
    tg://resolve?domain=channelname&post=456
    tg://privatepost?channel=1234567890&post=456

链接后面允许带 ``?single`` / ``?thread=12`` / ``?comment=34`` 等参数，
Telegram 客户端"复制链接"时会自动附加。
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterable, Sequence
from urllib.parse import parse_qs, unquote, urlparse

# 官方与常见镜像域名
TELEGRAM_HOSTS = {"t.me", "telegram.me", "telegram.dog", "telesco.pe"}

# t.me 下这些一级路径不是频道用户名，不能当频道解析
RESERVED_PATHS = {
    "joinchat", "addstickers", "addemoji", "addtheme", "setlanguage", "share",
    "proxy", "socks", "login", "confirmphone", "bg", "iv", "addlist", "contact",
    "invoice", "giftcode", "boost", "premium", "call", "nft", "blog", "faq",
    "apps", "privacy", "tos", "auth", "wallet",
}

# 单条消息链接里一次最多展开多少条（区间链接的保护上限）
DEFAULT_MAX_RANGE = 100

_LINK_RE = re.compile(
    r"(?:https?://)?(?:www\.)?(?:t\.me|telegram\.me|telegram\.dog|telesco\.pe)/[^\s<>\"'`　]+"
    r"|tg://[^\s<>\"'`　]+",
    re.IGNORECASE,
)

# 链接被中英文标点包住时把尾巴削掉（"看这个 https://t.me/c/1/2。" 之类）
_TRAILING_JUNK = ".,;:!?'\"`)]}>，。；：！？）】》」』、…"

_USERNAME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]{3,31}$")
_MESSAGE_SPEC_RE = re.compile(r"^(\d{1,12})(?:[-~](\d{1,12}))?$")


@dataclass(frozen=True)
class TelegramMessageLink:
    """一条已解析的 Telegram 消息链接。"""

    url: str
    kind: str                       # 'private' | 'public' | 'invite'
    channel_id: int | None = None   # kind == 'private' 时有值（内部数字 ID）
    username: str | None = None     # kind == 'public' 时有值
    invite_hash: str | None = None  # kind == 'invite' 时有值
    message_ids: tuple[int, ...] = ()
    topic_id: int | None = None
    single: bool = False            # 链接带 ?single，表示"只要这一条，不展开相册"
    comment_id: int | None = None

    @property
    def entity_key(self) -> tuple:
        """用于缓存已解析实体的 key。"""
        if self.kind == "private":
            return ("channel", self.channel_id)
        if self.kind == "public":
            return ("username", (self.username or "").lower())
        return ("invite", self.invite_hash)

    @property
    def dedup_key(self) -> tuple:
        return (self.entity_key, self.message_ids, self.topic_id, self.single)

    def describe(self) -> str:
        """给日志 / Telegram 回复用的短描述。

        故意**不**拼成 t.me 链接：守护进程的回复也会回到被监听的频道里，
        如果回复内容里带真链接，下一轮 NewMessage 事件会把自己的回复再当成
        任务解析一遍，形成自触发循环。
        """
        if self.kind == "private":
            target = f"私密频道 {self.channel_id}"
        elif self.kind == "public":
            target = f"@{self.username}"
        else:
            target = f"邀请链接 {(self.invite_hash or '')[:8]}…"

        if not self.message_ids:
            return target
        if len(self.message_ids) == 1:
            return f"{target} 消息 {self.message_ids[0]}"
        return f"{target} 消息 {self.message_ids[0]}~{self.message_ids[-1]}"


def _strip_trailing_junk(url: str) -> str:
    trimmed = url.rstrip(_TRAILING_JUNK)
    # 括号成对时不要误删："(https://t.me/c/1/2)" 削尾，"t.me/c/1/2)" 也削尾，
    # 但 "t.me/foo_(bar)" 这种带成对括号的路径要保留。
    if trimmed.count("(") > trimmed.count(")") and url[len(trimmed):].startswith(")"):
        trimmed = url[: len(trimmed) + 1]
    return trimmed


def _parse_message_spec(spec: str, max_range: int) -> tuple[int, ...]:
    """把 ``456`` / ``456-460`` 解析成消息 ID 元组。"""
    match = _MESSAGE_SPEC_RE.match(spec)
    if not match:
        return ()

    start = int(match.group(1))
    if start <= 0:
        return ()
    if match.group(2) is None:
        return (start,)

    end = int(match.group(2))
    if end < start:
        start, end = end, start
    if start <= 0:
        return ()
    return tuple(range(start, min(end, start + max_range - 1) + 1))


def _first_int(values: Sequence[str] | None) -> int | None:
    if not values:
        return None
    try:
        parsed = int(values[0])
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def _safe_urlparse(url: str):
    """``urlparse`` 对畸形的 IPv6 字面量（``https://[abc``）会抛 ValueError。"""
    try:
        parsed = urlparse(url)
        parsed.netloc  # netloc 是惰性解析的，这里主动摸一下把异常提前引爆
    except ValueError:
        return None
    return parsed


def _parse_tg_scheme(url: str, max_range: int) -> TelegramMessageLink | None:
    parsed = _safe_urlparse(url)
    if parsed is None:
        return None
    action = (parsed.netloc or parsed.path.lstrip("/")).lower()
    query = parse_qs(parsed.query, keep_blank_values=True)

    post_id = _first_int(query.get("post")) or _first_int(query.get("message_id"))
    message_ids = (post_id,) if post_id else ()
    thread_id = _first_int(query.get("thread")) or _first_int(query.get("topic"))
    comment_id = _first_int(query.get("comment"))
    single = "single" in query

    if action == "privatepost":
        channel_raw = _first_int(query.get("channel"))
        if not channel_raw:
            return None
        return TelegramMessageLink(
            url=url, kind="private", channel_id=channel_raw, message_ids=message_ids,
            topic_id=thread_id, single=single, comment_id=comment_id,
        )

    if action == "resolve":
        domain_values = query.get("domain")
        domain = domain_values[0] if domain_values else ""
        if not _USERNAME_RE.match(domain):
            return None
        return TelegramMessageLink(
            url=url, kind="public", username=domain, message_ids=message_ids,
            topic_id=thread_id, single=single, comment_id=comment_id,
        )

    if action in ("join", "invite"):
        invite_values = query.get("invite")
        invite_hash = invite_values[0] if invite_values else ""
        if not invite_hash:
            return None
        return TelegramMessageLink(
            url=url, kind="invite", invite_hash=invite_hash, message_ids=message_ids,
            topic_id=thread_id, single=single, comment_id=comment_id,
        )

    return None


def parse_link(url: str, max_range: int = DEFAULT_MAX_RANGE) -> TelegramMessageLink | None:
    """解析单条链接；不是 Telegram 消息链接时返回 ``None``。"""
    if not url:
        return None

    url = _strip_trailing_junk(url.strip())
    if not url:
        return None

    if url.lower().startswith("tg://"):
        return _parse_tg_scheme(url, max_range)

    normalized = url if re.match(r"^https?://", url, re.IGNORECASE) else f"https://{url}"
    parsed = _safe_urlparse(normalized)
    if parsed is None:
        return None

    host = parsed.netloc.lower().split("@")[-1].split(":")[0]
    if host.startswith("www."):
        host = host[4:]
    if host not in TELEGRAM_HOSTS:
        return None

    segments = [unquote(seg) for seg in parsed.path.split("/") if seg]
    if not segments:
        return None

    # t.me/s/<channel>/<id> 是频道的网页预览形态，去掉 "s" 后与普通链接同构
    if segments[0].lower() == "s":
        segments = segments[1:]
        if not segments:
            return None

    query = parse_qs(parsed.query, keep_blank_values=True)
    single = "single" in query
    comment_id = _first_int(query.get("comment"))
    query_thread = _first_int(query.get("thread")) or _first_int(query.get("topic"))

    head = segments[0]
    head_lower = head.lower()

    if head_lower == "c":
        if len(segments) < 2 or not segments[1].isdigit():
            return None
        channel_id = int(segments[1])
        if channel_id <= 0:
            return None
        kind, username, invite_hash = "private", None, None
        rest = segments[2:]
    elif head_lower == "joinchat":
        if len(segments) < 2 or not segments[1]:
            return None
        kind, channel_id, username, invite_hash = "invite", None, None, segments[1]
        rest = segments[2:]
    elif head.startswith("+"):
        invite_hash = head[1:]
        if not invite_hash:
            return None
        kind, channel_id, username = "invite", None, None
        rest = segments[1:]
    elif head_lower in RESERVED_PATHS:
        return None
    elif _USERNAME_RE.match(head):
        kind, channel_id, username, invite_hash = "public", None, head, None
        rest = segments[1:]
    else:
        return None

    topic_id = query_thread
    message_ids: tuple[int, ...] = ()
    if len(rest) == 1:
        message_ids = _parse_message_spec(rest[0], max_range)
    elif len(rest) >= 2:
        # /<topic>/<message>：话题（forum topic）里的消息，最后一段才是消息 ID
        if rest[0].isdigit():
            topic_id = int(rest[0])
        message_ids = _parse_message_spec(rest[1], max_range)

    return TelegramMessageLink(
        url=url, kind=kind, channel_id=channel_id, username=username,
        invite_hash=invite_hash, message_ids=message_ids, topic_id=topic_id,
        single=single, comment_id=comment_id,
    )


def utf16_slice(text: str, offset: int, length: int) -> str:
    """按 UTF-16 码元的 offset/length 从文本里切片。

    Telegram 的 ``MessageEntity.offset`` / ``.length`` 以 UTF-16 码元计数，正文里
    只要有 emoji（代理对）或部分 CJK 扩展字符，直接用 Python 的字符下标就会错位。
    """
    if not text or length <= 0 or offset < 0:
        return ""
    encoded = text.encode("utf-16-le")
    start = offset * 2
    end = start + length * 2
    if start >= len(encoded):
        return ""
    return encoded[start:end].decode("utf-16-le", errors="ignore")


def iter_raw_links(text: str) -> Iterable[str]:
    """从一段文本里扫出所有疑似 Telegram 链接的子串。"""
    if not text:
        return []
    return (match.group(0) for match in _LINK_RE.finditer(text))


def parse_telegram_links(
    text: str,
    extra_urls: Iterable[str] | None = None,
    max_range: int = DEFAULT_MAX_RANGE,
    require_message_id: bool = True,
) -> list[TelegramMessageLink]:
    """从文本（可选再加上消息实体里的 URL）中解析出所有 Telegram 消息链接。

    ``extra_urls`` 用来接住 ``MessageEntityTextUrl`` 这类"文字是中文、真链接
    藏在实体里"的情况。结果按出现顺序去重。
    """
    candidates: list[str] = list(iter_raw_links(text or ""))
    for extra in extra_urls or []:
        if extra:
            candidates.append(extra)

    results: list[TelegramMessageLink] = []
    seen: set[tuple] = set()
    for candidate in candidates:
        link = parse_link(candidate, max_range=max_range)
        if link is None:
            continue
        if require_message_id and not link.message_ids:
            continue
        if link.dedup_key in seen:
            continue
        seen.add(link.dedup_key)
        results.append(link)
    return results


def has_telegram_link(text: str) -> bool:
    """快速判断：这段文本里有没有可下载的 Telegram 消息链接。"""
    return bool(parse_telegram_links(text))
