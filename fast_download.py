"""fast_download — 单文件多连接并行分块下载。

Telethon 默认的 ``client.download_media`` 是**单连接、串行**逐块拉取，吞吐被
网络往返延迟（RTT）卡死，往往连服务端给的限速档都跑不满。对 Telegram Premium
账号而言，服务端虽然放开了更高的下载限速档，但单连接根本吃不下这部分红利。

本模块实现经典的 "FastTelethon" 并行下载：同时建立多条到文件所在 DC 的
MTProto 连接，并发请求不同 offset 的分块，再按顺序写回文件。实测可比默认实现
快数倍，从而真正利用上高级订阅的高速下载权益。

实现参考 Lonami / painor 的公开 FastTelethon 方案，仅保留下载路径，并针对本项目
做了精简与健壮性处理。依赖 Telethon 1.x 的若干内部 API（已在 1.36~1.42 验证）。
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import math
from typing import AsyncGenerator, BinaryIO, List, Optional

from telethon import TelegramClient, utils
from telethon.crypto import AuthKey
from telethon.network import MTProtoSender
from telethon.tl.alltlobjects import LAYER
from telethon.tl.functions import InvokeWithLayerRequest
from telethon.tl.functions.auth import (
    ExportAuthorizationRequest,
    ImportAuthorizationRequest,
)
from telethon.tl.functions.upload import GetFileRequest

log = logging.getLogger("telegram-download-daemon.fast_download")


class _DownloadSender:
    """单条连接上的分块拉取器：从 ``offset`` 开始，每次步进 ``stride``，共 ``count`` 块。"""

    def __init__(self, client: TelegramClient, sender: MTProtoSender, file,
                 offset: int, limit: int, stride: int, count: int) -> None:
        self.client = client
        self.sender = sender
        self.request = GetFileRequest(file, offset=offset, limit=limit)
        self.stride = stride
        self.remaining = count

    async def next(self) -> Optional[bytes]:
        if not self.remaining:
            return None
        result = await self.client._call(self.sender, self.request)
        self.remaining -= 1
        self.request.offset += self.stride
        return result.bytes

    def disconnect(self):
        return self.sender.disconnect()


class ParallelTransferrer:
    """管理一组到目标 DC 的并行连接，按 round-robin 拉取分块。"""

    def __init__(self, client: TelegramClient, dc_id: Optional[int] = None) -> None:
        self.client = client
        self.dc_id = dc_id or client.session.dc_id
        # 如果目标 DC 就是当前会话所在 DC，可直接复用 auth_key，省去 export/import 授权一步。
        self.auth_key: Optional[AuthKey] = (
            None if dc_id and client.session.dc_id != dc_id else client.session.auth_key
        )
        self.senders: Optional[List[_DownloadSender]] = None

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

    async def _create_download_sender(self, file, index: int, part_size: int,
                                      stride: int, part_count: int) -> _DownloadSender:
        return _DownloadSender(
            self.client, await self._create_sender(), file,
            index * part_size, part_size, stride, part_count,
        )

    async def _init_download(self, connections: int, file, part_count: int,
                             part_size: int) -> None:
        minimum, remainder = divmod(part_count, connections)

        def get_part_count() -> int:
            nonlocal remainder
            if remainder > 0:
                remainder -= 1
                return minimum + 1
            return minimum

        # 第一条 sender 复用主连接的授权信息，其余并发建立。
        self.senders = [
            await self._create_download_sender(
                file, 0, part_size, connections * part_size, get_part_count()),
            *await asyncio.gather(*[
                self._create_download_sender(
                    file, i, part_size, connections * part_size, get_part_count())
                for i in range(1, connections)
            ]),
        ]

    async def init_download(self, file, file_size: int,
                            connection_count: Optional[int] = None,
                            part_size_kb: Optional[float] = None) -> int:
        connection_count = connection_count or self._get_connection_count(
            file_size, max_count=8)
        part_size = int((part_size_kb or utils.get_appropriated_part_size(file_size)) * 1024)
        part_count = math.ceil(file_size / part_size)
        log.debug(
            "Parallel download: size=%d, connections=%d, part_size=%d, parts=%d",
            file_size, connection_count, part_size, part_count,
        )
        await self._init_download(connection_count, file, part_count, part_size)
        return part_count

    async def download(self, part_count: int) -> AsyncGenerator[bytes, None]:
        assert self.senders is not None
        part = 0
        while part < part_count:
            # 每一轮给每条连接派一个分块任务，再按 sender 顺序回收，
            # 由于 sender0 覆盖 offset 0/N、sender1 覆盖 1/N+1……round-robin
            # 回收即可得到严格递增的字节序，可直接顺序写文件。
            tasks = [asyncio.ensure_future(s.next()) for s in self.senders]
            for task in tasks:
                data = await task
                if not data:
                    break
                yield data
                part += 1

    async def finish(self) -> None:
        if not self.senders:
            return
        await asyncio.gather(
            *[s.disconnect() for s in self.senders], return_exceptions=True)
        self.senders = None


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
                        connection_count: Optional[int] = None) -> int:
    """把 ``document`` 并行下载并写入已打开的二进制文件对象 ``out``。

    返回写入的总字节数。``progress_callback(received, total)`` 兼容同步/异步两种形式。
    """
    size = int(document.size)
    dc_id, input_location = utils.get_input_location(document)
    transferrer = ParallelTransferrer(client, dc_id)
    part_count = await transferrer.init_download(
        input_location, size, connection_count=connection_count)
    received = 0
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
