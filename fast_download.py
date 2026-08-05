"""fast_download — 单文件多连接并行分块下载，支持从任意偏移量续传。

Telethon 默认的 ``client.download_media`` 是**单连接、串行**逐块拉取：发一个
``GetFileRequest``，等它回来，再发下一个。任何时刻链路上只有一个 in-flight 请求，
吞吐被网络往返延迟（RTT）卡死；更关键的是 Telegram 的下载限速是**按连接**计的，
单连接就是天花板，Premium 放开的高速档也吃不到。

本模块实现 "FastTelethon" 式并行下载：同时建立多条到文件所在 DC 的 MTProto 连接，
各自负责一组分块，再按序号顺序写回文件。

与早期实现（以及公开的 FastTelethon）的区别，也是这次重写的重点：

* **没有轮次栅栏（barrier）**。早先的写法是每轮给 N 条连接各派一块、等整轮回收完
  才发下一轮，最慢的那条连接会拖住所有人，每轮尾部还白白浪费一个 RTT。现在每条
  连接是一个独立协程，自己一直往前跑，互不等待。
* **背压靠每条连接自己的有界队列**，而不是全局栅栏。消费者按 ``part % N`` 轮流取，
  取到的天然就是严格递增的字节序，可以直接顺序写文件；某条连接跑太快时只会阻塞
  它自己，其余连接照常拉取。
* **消费者做磁盘写和进度回调期间，其它连接仍在拉数据**。这是早先实现最大的隐性
  损失：``yield`` 之后要等调用方写盘 + 回调（含 SQLite 写、WS 广播）走完，全部
  连接都停着，并行度越高这段串行占比越大，能把并行收益吃干净。

另外补上了长时间下载必须处理的两件事：``file_reference`` 过期时回调宿主重新取回
消息，以及 flood-wait / 超时的有限重试。

实现参考 Lonami / painor 的公开 FastTelethon 方案，仅保留下载路径。依赖 Telethon 1.x
的若干内部 API（已在 1.36~1.42 验证）。
"""

from __future__ import annotations

import asyncio
import contextlib
import inspect
import logging
import math
from typing import AsyncGenerator, BinaryIO, Callable, List, Optional

from telethon import TelegramClient, errors, utils
from telethon.crypto import AuthKey
from telethon.network import MTProtoSender
from telethon.tl import types
from telethon.tl.alltlobjects import LAYER
from telethon.tl.functions import InvokeWithLayerRequest
from telethon.tl.functions.auth import (
    ExportAuthorizationRequest,
    ImportAuthorizationRequest,
)
from telethon.tl.functions.upload import GetFileRequest

log = logging.getLogger("telegram-download-daemon.fast_download")

# Telegram 对 upload.getFile 的约束：offset 和 limit 都必须是 4KB 的整数倍，
# limit 不超过 512KB，且单次请求不能跨越 1MB 边界。只要 part_size 取 512KB 的
# 约数、offset 取 part_size 的整数倍，这些条件自动满足。
MIN_CHUNK_SIZE = 4 * 1024
MAX_CHUNK_SIZE = 512 * 1024

# 每条连接允许"已取回但还没被消费"的分块数。乘上连接数和分块大小就是内存上限，
# 默认 8 连接 × 4 × 512KB ≈ 16MB。
DEFAULT_QUEUE_DEPTH = 4

_DONE = object()


class CdnRedirectNeeded(Exception):
    """服务端要求改走 CDN。本模块不实现 CDN 路径，交回 Telethon 原生下载处理。"""


def align_down(offset: int, part_size: int) -> int:
    """把 ``offset`` 向下对齐到 ``part_size`` 的整数倍。

    续传时必须这么做：Telegram 只接受对齐的 offset，把已下载的尾巴截掉一点重下，
    远比整个文件重下便宜。
    """
    if part_size <= 0:
        return 0
    return (offset // part_size) * part_size


def choose_part_size(file_size: int) -> int:
    """选择分块大小，并保证它是 4KB 的整数倍且不超过 512KB。"""
    part_size = int(utils.get_appropriated_part_size(max(file_size, 1)) * 1024)
    part_size -= part_size % MIN_CHUNK_SIZE
    return max(MIN_CHUNK_SIZE, min(part_size, MAX_CHUNK_SIZE))


class _ChunkSource:
    """一条连接 + 它负责的那组分块。

    序号为 ``first_index`` 的连接负责 ``first_index, first_index+N, first_index+2N …``
    号分块。它是个独立协程，取到一块就塞进自己的有界队列，队列满了才停下来等消费者，
    **不会**因为别的连接慢而停。
    """

    def __init__(self, transferrer: "ParallelTransferrer", sender: MTProtoSender,
                 first_index: int, part_size: int, stride: int, part_count: int,
                 depth: int) -> None:
        self._transferrer = transferrer
        self._sender = sender
        self._offset = transferrer.start_offset + first_index * part_size
        self._part_size = part_size
        self._stride = stride
        self._remaining = part_count
        self.queue: asyncio.Queue = asyncio.Queue(maxsize=max(1, depth))
        self.task: Optional[asyncio.Future] = None

    def start(self) -> None:
        self.task = asyncio.ensure_future(self._run())

    async def _run(self) -> None:
        try:
            while self._remaining > 0:
                data = await self._transferrer.request_chunk(
                    self._sender, self._offset, self._part_size)
                await self.queue.put(data)
                self._remaining -= 1
                self._offset += self._stride
                if len(data) < self._part_size:
                    # 短读只可能出现在文件末尾，后面没有数据了。
                    break
            await self.queue.put(_DONE)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001 —— 交给消费者原样抛出
            # 队列满时这里会等；消费者按序轮流取，一定会把本队列排空，所以不会死锁。
            with contextlib.suppress(asyncio.CancelledError):
                await self.queue.put(exc)

    async def close(self) -> None:
        if self.task is not None and not self.task.done():
            self.task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self.task
        self.task = None

    def disconnect(self):
        return self._sender.disconnect()


class ParallelTransferrer:
    """管理一组到目标 DC 的并行连接，按分块序号取回文件的 ``[start_offset, size)`` 区间。"""

    def __init__(self, client: TelegramClient, dc_id: Optional[int] = None,
                 refresh_document: Optional[Callable] = None,
                 max_chunk_retries: int = 5,
                 flood_sleep_threshold: int = 60) -> None:
        self.client = client
        self.dc_id = dc_id or client.session.dc_id
        # 如果目标 DC 就是当前会话所在 DC，可直接复用 auth_key，省去 export/import 授权一步。
        self.auth_key: Optional[AuthKey] = (
            None if dc_id and client.session.dc_id != dc_id else client.session.auth_key
        )
        self.start_offset = 0
        self.sources: List[_ChunkSource] = []
        self._location = None
        self._refresh_document = refresh_document
        self._refresh_lock = asyncio.Lock()
        self._refresh_generation = 0
        self._max_chunk_retries = max(1, max_chunk_retries)
        self._flood_sleep_threshold = flood_sleep_threshold

    # ------------------------------------------------------------------
    # 连接建立
    # ------------------------------------------------------------------

    @staticmethod
    def _get_connection_count(file_size: int, max_count: int,
                              full_size: int = 100 * 1024 * 1024) -> int:
        """按文件大小线性分配连接数：100MB 及以上用满 ``max_count``，更小的按比例缩减。"""
        if file_size > full_size:
            return max_count
        return max(1, math.ceil((file_size / full_size) * max_count))

    async def _create_sender(self) -> MTProtoSender:
        dc = await self.client._get_dc(self.dc_id)
        sender = MTProtoSender(self.auth_key, loggers=self.client._log)
        await sender.connect(self.client._connection(
            dc.ip_address, dc.port, dc.id,
            loggers=self.client._log,
            proxy=self.client._proxy,
        ))
        if not self.auth_key:
            log.debug("Exporting auth to DC %s for parallel download", self.dc_id)
            auth = await self.client(ExportAuthorizationRequest(self.dc_id))
            self.client._init_request.query = ImportAuthorizationRequest(
                id=auth.id, bytes=auth.bytes)
            req = InvokeWithLayerRequest(LAYER, self.client._init_request)
            await sender.send(req)
            self.auth_key = sender.auth_key
        return sender

    # ------------------------------------------------------------------
    # 分块请求（含 file_reference 过期 / flood-wait / 超时处理）
    # ------------------------------------------------------------------

    async def _refresh_location(self, seen_generation: int) -> None:
        """``file_reference`` 过期时重新取回消息换一份新的 location。

        多条连接会几乎同时撞上过期，用 generation 号保证只真正刷新一次。
        """
        async with self._refresh_lock:
            if seen_generation != self._refresh_generation:
                return  # 别的连接已经刷过了
            if self._refresh_document is None:
                raise RuntimeError(
                    "File reference expired and no refresh callback was provided")
            log.info("File reference expired mid-download; refetching source message")
            document = self._refresh_document()
            if inspect.isawaitable(document):
                document = await document
            if document is None:
                raise RuntimeError("Could not refresh the expired file reference")
            _, self._location = utils.get_input_location(document)
            self._refresh_generation += 1

    async def request_chunk(self, sender: MTProtoSender, offset: int, limit: int) -> bytes:
        last_error: Optional[BaseException] = None
        for attempt in range(self._max_chunk_retries):
            generation = self._refresh_generation
            try:
                result = await self.client._call(
                    sender, GetFileRequest(self._location, offset=offset, limit=limit))
                if isinstance(result, types.upload.FileCdnRedirect):
                    raise CdnRedirectNeeded(
                        "Telegram asked for a CDN redirect; falling back to the native downloader")
                return result.bytes
            except (errors.FileReferenceExpiredError, errors.FilerefUpgradeNeededError) as exc:
                last_error = exc
                await self._refresh_location(generation)
            except errors.FloodWaitError as exc:
                last_error = exc
                if exc.seconds > self._flood_sleep_threshold:
                    raise
                log.warning("Flood wait of %ds while downloading offset %d", exc.seconds, offset)
                await asyncio.sleep(exc.seconds + 1)
            except errors.TimedOutError as exc:
                last_error = exc
                log.info("Timeout on chunk at offset %d, retrying (%d/%d)",
                         offset, attempt + 1, self._max_chunk_retries)
                await asyncio.sleep(1)
        raise last_error if last_error is not None else RuntimeError(
            f"Failed to fetch chunk at offset {offset}")

    # ------------------------------------------------------------------
    # 下载
    # ------------------------------------------------------------------

    async def init_download(self, location, file_size: int, start_offset: int = 0,
                            connection_count: Optional[int] = None,
                            part_size_kb: Optional[float] = None,
                            queue_depth: int = DEFAULT_QUEUE_DEPTH) -> int:
        self._location = location
        remaining = max(0, file_size - start_offset)
        connection_count = connection_count or self._get_connection_count(
            remaining, max_count=8)
        if part_size_kb:
            part_size = int(part_size_kb * 1024)
            part_size -= part_size % MIN_CHUNK_SIZE
            part_size = max(MIN_CHUNK_SIZE, min(part_size, MAX_CHUNK_SIZE))
        else:
            part_size = choose_part_size(file_size)

        # Telegram 只接受对齐的 offset，调用方应当已经把断点截齐；这里再兜一次底。
        self.start_offset = align_down(start_offset, part_size)
        remaining = max(0, file_size - self.start_offset)
        part_count = math.ceil(remaining / part_size) if remaining else 0
        connection_count = max(1, min(connection_count, part_count or 1))

        log.debug(
            "Parallel download: size=%d, from=%d, connections=%d, part_size=%d, parts=%d",
            file_size, self.start_offset, connection_count, part_size, part_count,
        )

        minimum, remainder = divmod(part_count, connection_count)

        def next_part_count() -> int:
            nonlocal remainder
            if remainder > 0:
                remainder -= 1
                return minimum + 1
            return minimum

        # 第一条 sender 串行建立：跨 DC 时它负责把 auth_key export/import 出来，
        # 后续连接直接复用，否则并发导出会互相踩 client._init_request。
        senders = [await self._create_sender()]
        if connection_count > 1:
            senders.extend(await asyncio.gather(
                *[self._create_sender() for _ in range(1, connection_count)]))

        stride = connection_count * part_size
        self.sources = [
            _ChunkSource(self, sender, index, part_size, stride,
                         next_part_count(), queue_depth)
            for index, sender in enumerate(senders)
        ]
        return part_count

    async def download(self, part_count: int) -> AsyncGenerator[bytes, None]:
        if not self.sources:
            return
        for source in self.sources:
            source.start()

        source_count = len(self.sources)
        part = 0
        while part < part_count:
            # 序号 k 的分块一定由第 k % N 条连接负责，所以按这个顺序取回来的
            # 就是严格递增的字节序，可以直接顺序写文件。
            item = await self.sources[part % source_count].queue.get()
            if item is _DONE:
                break
            if isinstance(item, BaseException):
                raise item
            yield item
            part += 1

    async def finish(self) -> None:
        if not self.sources:
            return
        sources, self.sources = self.sources, []
        for source in sources:
            await source.close()
        await asyncio.gather(
            *[source.disconnect() for source in sources], return_exceptions=True)


def get_parallel_location(message):
    """从消息里取出可并行下载的 Document（带 size），否则返回 None。

    照片 / 缩略图等体积小、且 location 结构不同，统一走默认下载路径，故返回 None。
    """
    document = getattr(message, "document", None)
    if document is not None and getattr(document, "size", 0):
        return document
    media = getattr(message, "media", None)
    if media is not None:
        inner = getattr(media, "document", None)
        if inner is not None and getattr(inner, "size", 0):
            return inner
    return None


async def download_file(client: TelegramClient, document, out: BinaryIO,
                        progress_callback=None,
                        connection_count: Optional[int] = None,
                        start_offset: int = 0,
                        refresh_document: Optional[Callable] = None) -> int:
    """把 ``document`` 的 ``[start_offset, size)`` 区间并行下载并写入 ``out``。

    ``out`` 必须已经定位到 ``start_offset``（续传时用 ``'r+b'`` 打开并 seek，或用
    ``'ab'`` 追加）。返回**文件总字节数**（含续传起点之前的部分）。

    ``progress_callback(received, total)`` 里的 ``received`` 同样是累计值，
    兼容同步 / 异步两种形式。``refresh_document`` 在 file_reference 过期时被调用，
    应当返回一份新的 document（可以是协程）。
    """
    size = int(document.size)
    dc_id, input_location = utils.get_input_location(document)
    transferrer = ParallelTransferrer(client, dc_id, refresh_document=refresh_document)
    part_count = await transferrer.init_download(
        input_location, size, start_offset=start_offset,
        connection_count=connection_count)

    received = transferrer.start_offset
    try:
        async for chunk in transferrer.download(part_count):
            out.write(chunk)
            received += len(chunk)
            if progress_callback:
                result = progress_callback(min(received, size), size)
                if inspect.isawaitable(result):
                    await result
    finally:
        await transferrer.finish()
    return received
