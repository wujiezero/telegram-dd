"""``download_media_dispatch`` 的单元测试——续传接线最容易出错的地方。

同样用 AST 把函数从主文件里摘出来单独执行（主文件不可 import，理由见
``tests/test_link_extraction.py``）。分块下载器和 Telethon 的原生下载都换成替身，
断言的是"在什么情况下走哪条路、传了什么偏移量、临时文件被怎么处理"。

运行：
    cd telegram-download-deamon
    python -m pytest tests/test_download_dispatch.py -v
"""

import ast
import asyncio
import contextlib
import logging
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from fast_download import get_parallel_location  # noqa: E402

DAEMON_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "telegram-download-daemon.py")
)


def _load_dispatch(**globals_override):
    with open(DAEMON_PATH, "r", encoding="utf-8") as handle:
        tree = ast.parse(handle.read(), filename=DAEMON_PATH)

    picked = [
        node for node in tree.body
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "download_media_dispatch"
    ]
    if not picked:
        raise AssertionError("未能在守护进程源码里找到 download_media_dispatch")

    namespace = {
        "os": os,
        "asyncio": asyncio,
        "contextlib": contextlib,
        "logger": logging.getLogger("test"),
        "get_parallel_location": get_parallel_location,
        "parallel_connections": 4,
        "parallel_min_size_bytes": 10 * 1024 * 1024,
    }
    namespace.update(globals_override)
    module = ast.Module(body=picked, type_ignores=[])
    exec(compile(module, DAEMON_PATH, "exec"), namespace)  # noqa: S102
    return namespace


class FakeDocument:
    def __init__(self, size):
        self.size = size


class FakeMessage:
    """带 document 的消息替身；photo=True 时用来模拟没有 document 的媒体。"""

    def __init__(self, size=None):
        self.document = FakeDocument(size) if size is not None else None
        self.media = None


class Recorder:
    """记录分块下载器 / 原生下载器分别被怎么调用。"""

    def __init__(self, chunked_error=None, written=b""):
        self.chunked_calls = []
        self.native_calls = []
        self.chunked_error = chunked_error
        self.written = written

    async def fast_download_file(self, client, document, out, progress_callback=None,
                                 connection_count=None, start_offset=0,
                                 refresh_document=None):
        self.chunked_calls.append({
            "document": document,
            "connection_count": connection_count,
            "start_offset": start_offset,
            "file_position": out.tell(),
            "file_mode": out.mode,
            "refresh_document": refresh_document,
        })
        if self.chunked_error:
            raise self.chunked_error
        out.write(self.written)
        return start_offset + len(self.written)

    def as_client(self):
        recorder = self

        class FakeClient:
            async def download_media(self, message_obj, temp_path, progress_callback=None):
                recorder.native_calls.append({
                    "temp_path": temp_path,
                    "existed_on_entry": os.path.exists(temp_path),
                })
                with open(temp_path, "wb") as fh:
                    fh.write(b"native")
                return temp_path

        return FakeClient()


class DispatchTestCase(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)
        self.temp_path = os.path.join(self.tmpdir.name, "movie.mkv.tdd")

    def build(self, recorder, **overrides):
        ns = _load_dispatch(
            fast_download_file=recorder.fast_download_file,
            client=recorder.as_client(),
            **overrides,
        )
        return ns["download_media_dispatch"]

    def write_partial(self, nbytes):
        with open(self.temp_path, "wb") as fh:
            fh.write(b"x" * nbytes)


class RoutingTests(DispatchTestCase):
    def test_large_document_uses_the_chunked_downloader(self):
        rec = Recorder()
        dispatch = self.build(rec)
        asyncio.run(dispatch(FakeMessage(size=50 * 1024**2), self.temp_path, None))
        self.assertEqual(len(rec.chunked_calls), 1)
        self.assertEqual(rec.native_calls, [])
        self.assertEqual(rec.chunked_calls[0]["connection_count"], 4)
        self.assertEqual(rec.chunked_calls[0]["start_offset"], 0)

    def test_small_document_uses_the_native_downloader(self):
        # 小文件并行收益低，走原生路径
        rec = Recorder()
        dispatch = self.build(rec)
        asyncio.run(dispatch(FakeMessage(size=1024), self.temp_path, None))
        self.assertEqual(rec.chunked_calls, [])
        self.assertEqual(len(rec.native_calls), 1)

    def test_media_without_document_uses_the_native_downloader(self):
        # 照片没有 document，location 结构也不同，只能走原生下载
        rec = Recorder()
        dispatch = self.build(rec)
        asyncio.run(dispatch(FakeMessage(size=None), self.temp_path, None))
        self.assertEqual(rec.chunked_calls, [])
        self.assertEqual(len(rec.native_calls), 1)

    def test_single_connection_still_uses_native_when_not_resuming(self):
        rec = Recorder()
        dispatch = self.build(rec, parallel_connections=1)
        asyncio.run(dispatch(FakeMessage(size=50 * 1024**2), self.temp_path, None))
        self.assertEqual(rec.chunked_calls, [])
        self.assertEqual(len(rec.native_calls), 1)


class ResumeTests(DispatchTestCase):
    def test_resume_seeks_and_passes_the_offset(self):
        rec = Recorder(written=b"tail")
        dispatch = self.build(rec)
        self.write_partial(8192)
        asyncio.run(dispatch(FakeMessage(size=50 * 1024**2), self.temp_path, None,
                             resume_from=4096))
        call = rec.chunked_calls[0]
        self.assertEqual(call["start_offset"], 4096)
        # 必须以可读写模式打开并定位到断点，而不是 'wb'（那会把已下载的部分清零）
        self.assertEqual(call["file_position"], 4096)
        self.assertIn("+", call["file_mode"])

    def test_resume_truncates_the_unaligned_tail(self):
        """断点之后的残留字节必须截掉，否则续写会把文件拼错位。"""
        rec = Recorder(written=b"tail")
        dispatch = self.build(rec)
        self.write_partial(8192)
        asyncio.run(dispatch(FakeMessage(size=50 * 1024**2), self.temp_path, None,
                             resume_from=4096))
        self.assertEqual(os.path.getsize(self.temp_path), 4096 + len(b"tail"))

    def test_resume_forces_the_chunked_path_even_for_small_files(self):
        # 只有分块下载器支持任意起始偏移，所以有断点时不看体积阈值
        rec = Recorder(written=b"tail")
        dispatch = self.build(rec, parallel_connections=1)
        self.write_partial(8192)
        asyncio.run(dispatch(FakeMessage(size=1024 * 1024), self.temp_path, None,
                             resume_from=4096))
        self.assertEqual(len(rec.chunked_calls), 1)
        self.assertEqual(rec.native_calls, [])

    def test_refresh_callback_is_forwarded(self):
        # file_reference 会在长时间下载中过期，回调必须传到下载器里
        rec = Recorder()
        dispatch = self.build(rec)

        async def refresher():
            return None

        asyncio.run(dispatch(FakeMessage(size=50 * 1024**2), self.temp_path, None,
                             refresh_document=refresher))
        self.assertIs(rec.chunked_calls[0]["refresh_document"], refresher)


class FallbackTests(DispatchTestCase):
    def test_chunked_failure_falls_back_and_clears_the_partial(self):
        """原生下载器不支持续传，半成品留着会被当成完整文件，必须先清掉。"""
        rec = Recorder(chunked_error=RuntimeError("cdn redirect"))
        dispatch = self.build(rec)
        self.write_partial(8192)
        asyncio.run(dispatch(FakeMessage(size=50 * 1024**2), self.temp_path, None,
                             resume_from=4096))
        self.assertEqual(len(rec.chunked_calls), 1)
        self.assertEqual(len(rec.native_calls), 1)
        self.assertFalse(rec.native_calls[0]["existed_on_entry"],
                         "the stale partial should have been removed before falling back")

    def test_cancellation_is_not_swallowed(self):
        """取消必须原样往上抛，不能被当成"并行失败"降级成原生下载重来一遍。"""
        rec = Recorder(chunked_error=asyncio.CancelledError())
        dispatch = self.build(rec)
        with self.assertRaises(asyncio.CancelledError):
            asyncio.run(dispatch(FakeMessage(size=50 * 1024**2), self.temp_path, None))
        self.assertEqual(rec.native_calls, [])


if __name__ == "__main__":
    unittest.main()
