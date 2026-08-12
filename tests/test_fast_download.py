"""fast_download 的纯逻辑测试。

用一个假的 client 顶替 Telethon：``_call`` 直接按 offset/limit 从内存里的"文件"
切片返回，``_get_dc`` / ``_connection`` 之类建连接的部分被 monkeypatch 掉。这样
可以在不联网的情况下验证分块调度本身：字节序、续传、错误传播、以及"某条连接慢
不会拖住其它连接"这条本次重写的核心性质。
"""

import asyncio
import io
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import fast_download
from fast_download import ParallelTransferrer, align_down, choose_part_size


class FakeResult:
    def __init__(self, data):
        self.bytes = data


class FakeSender:
    """假连接。``delay`` 用来模拟这条链路比别人慢。"""

    def __init__(self, index, delay=0.0):
        self.index = index
        self.delay = delay
        self.calls = []
        self.disconnected = False

    async def disconnect(self):
        self.disconnected = True


class FakeClient:
    def __init__(self, payload, delays=None, fail_at=None):
        self.payload = payload
        self.delays = delays or {}
        self.fail_at = fail_at
        self.session = type("S", (), {"dc_id": 2, "auth_key": object()})()
        self._log = {}
        self._proxy = None
        self.senders = []
        # 每一刻有多少个请求同时在飞——用来证明并行是真的并行
        self.in_flight = 0
        self.max_in_flight = 0

    async def _call(self, sender, request):
        offset, limit = request.offset, request.limit
        if self.fail_at is not None and offset == self.fail_at:
            raise RuntimeError(f"boom at {offset}")

        self.in_flight += 1
        self.max_in_flight = max(self.max_in_flight, self.in_flight)
        try:
            delay = self.delays.get(sender.index, 0.0)
            if delay:
                await asyncio.sleep(delay)
            sender.calls.append(offset)
            return FakeResult(self.payload[offset:offset + limit])
        finally:
            self.in_flight -= 1


def make_transferrer(client, connection_count):
    """建一个 ParallelTransferrer，并把"建连接"换成产出 FakeSender。"""
    transferrer = ParallelTransferrer(client, dc_id=None)

    created = {"n": 0}

    async def fake_create_sender():
        sender = FakeSender(created["n"])
        created["n"] += 1
        client.senders.append(sender)
        return sender

    transferrer._create_sender = fake_create_sender
    return transferrer


async def run_download(payload, connection_count, part_size_kb=4, start_offset=0,
                       delays=None, fail_at=None):
    client = FakeClient(payload, delays=delays, fail_at=fail_at)
    transferrer = make_transferrer(client, connection_count)
    part_count = await transferrer.init_download(
        object(), len(payload), start_offset=start_offset,
        connection_count=connection_count, part_size_kb=part_size_kb)
    out = io.BytesIO()
    try:
        async for chunk in transferrer.download(part_count):
            out.write(chunk)
    finally:
        await transferrer.finish()
    return out.getvalue(), client, transferrer


class AlignmentTests(unittest.TestCase):
    def test_align_down_snaps_to_part_boundary(self):
        # Telegram 只接受对齐的 offset，尾巴上不足一块的部分必须丢掉重下
        self.assertEqual(align_down(0, 4096), 0)
        self.assertEqual(align_down(4095, 4096), 0)
        self.assertEqual(align_down(4096, 4096), 4096)
        self.assertEqual(align_down(10000, 4096), 8192)

    def test_align_down_tolerates_zero_part_size(self):
        self.assertEqual(align_down(1234, 0), 0)

    def test_choose_part_size_is_valid_for_telegram(self):
        for size in (1, 1024, 10 * 1024**2, 100 * 1024**2, 3 * 1024**3):
            part = choose_part_size(size)
            self.assertEqual(part % fast_download.MIN_CHUNK_SIZE, 0,
                             f"part size {part} for {size} is not a 4KB multiple")
            self.assertLessEqual(part, fast_download.MAX_CHUNK_SIZE)
            self.assertGreater(part, 0)


class ParallelDownloadTests(unittest.TestCase):
    def test_bytes_come_out_in_order(self):
        payload = bytes(range(256)) * 400  # 102400 bytes
        data, _, _ = asyncio.run(run_download(payload, connection_count=4))
        self.assertEqual(data, payload)

    def test_single_connection_matches_payload(self):
        payload = os.urandom(20000)
        data, _, _ = asyncio.run(run_download(payload, connection_count=1))
        self.assertEqual(data, payload)

    def test_last_partial_chunk_is_kept(self):
        # 长度刻意不是分块大小的整数倍，最后一块是短读
        payload = os.urandom(4096 * 5 + 137)
        data, _, _ = asyncio.run(run_download(payload, connection_count=3, part_size_kb=4))
        self.assertEqual(data, payload)

    def test_resume_downloads_only_the_tail(self):
        payload = os.urandom(4096 * 10)
        start = 4096 * 4
        data, client, transferrer = asyncio.run(
            run_download(payload, connection_count=3, part_size_kb=4, start_offset=start))
        self.assertEqual(data, payload[start:])
        self.assertEqual(transferrer.start_offset, start)
        # 断点之前的字节一次都不该再请求
        requested = [off for sender in client.senders for off in sender.calls]
        self.assertTrue(all(off >= start for off in requested), requested)

    def test_unaligned_resume_offset_is_snapped_down(self):
        payload = os.urandom(4096 * 6)
        transferrer_offset = asyncio.run(self._start_offset_for(payload, 4096 * 3 + 500))
        self.assertEqual(transferrer_offset, 4096 * 3)

    async def _start_offset_for(self, payload, requested_offset):
        client = FakeClient(payload)
        transferrer = make_transferrer(client, 2)
        await transferrer.init_download(
            object(), len(payload), start_offset=requested_offset,
            connection_count=2, part_size_kb=4)
        await transferrer.finish()
        return transferrer.start_offset

    def test_work_is_spread_across_all_connections(self):
        payload = os.urandom(4096 * 16)
        _, client, _ = asyncio.run(
            run_download(payload, connection_count=4, part_size_kb=4))
        self.assertEqual(len(client.senders), 4)
        for sender in client.senders:
            self.assertTrue(sender.calls, f"connection {sender.index} fetched nothing")

    def test_requests_overlap_in_flight(self):
        """并行的实质：同一时刻链路上有多个请求在飞。

        重写前的实现是"每轮派 N 块 → 等整轮回收 → 再派下一轮"，轮次之间存在栅栏；
        这条断言保证现在不再有那个栅栏。
        """
        payload = os.urandom(4096 * 40)
        _, client, _ = asyncio.run(
            run_download(payload, connection_count=4, part_size_kb=4,
                         delays={i: 0.01 for i in range(4)}))
        self.assertGreater(client.max_in_flight, 1)
        self.assertLessEqual(client.max_in_flight, 4)

    def test_slow_connection_does_not_stall_the_others(self):
        """一条慢连接只拖慢它自己，其余连接继续往前跑。

        0 号连接每块慢 30ms，其余接近 0。这条断言正是重写前后的分界线：**任何**
        按轮次派活的实现里，每条连接的请求数最多只差 1（大家每轮各领一块），
        所以差距 >= 2 就说明栅栏确实没有了。实测这里是 2 vs 6。
        """
        payload = os.urandom(4096 * 60)

        async def scenario():
            client = FakeClient(payload, delays={0: 0.03})
            transferrer = make_transferrer(client, 3)
            part_count = await transferrer.init_download(
                object(), len(payload), connection_count=3, part_size_kb=4)
            # 只消费前几块：慢的 0 号还堵着的时候，快的连接应该已经把自己的队列填满了
            gen = transferrer.download(part_count)
            collected = 0
            async for _ in gen:
                collected += 1
                if collected >= 4:
                    break
            await gen.aclose()
            fast_calls = max(len(client.senders[1].calls), len(client.senders[2].calls))
            slow_calls = len(client.senders[0].calls)
            await transferrer.finish()
            return fast_calls, slow_calls

        fast_calls, slow_calls = asyncio.run(scenario())
        self.assertGreaterEqual(
            fast_calls - slow_calls, 2,
            f"fast connection ran only {fast_calls} vs slow {slow_calls}; "
            "a gap < 2 means the connections are still advancing in lockstep")

    def test_error_propagates_to_the_consumer(self):
        payload = os.urandom(4096 * 12)
        with self.assertRaises(RuntimeError):
            asyncio.run(run_download(payload, connection_count=3, part_size_kb=4,
                                     fail_at=4096 * 5))

    def test_finish_disconnects_every_sender(self):
        payload = os.urandom(4096 * 8)
        _, client, _ = asyncio.run(run_download(payload, connection_count=3, part_size_kb=4))
        self.assertTrue(client.senders)
        for sender in client.senders:
            self.assertTrue(sender.disconnected, f"connection {sender.index} was left open")

    def test_connection_count_never_exceeds_part_count(self):
        # 小文件只有 2 块，开 8 条连接毫无意义，还白白建连接
        payload = os.urandom(4096 * 2)
        _, client, _ = asyncio.run(run_download(payload, connection_count=8, part_size_kb=4))
        self.assertLessEqual(len(client.senders), 2)


if __name__ == "__main__":
    unittest.main()


class TransientConnectionErrorTests(unittest.TestCase):
    """瞬时断连不能判整个文件死刑。

    Telethon 的 ``client._call`` 只对 ServerError / RpcCallFail / TimedOut 这几类
    做内部重试，**连接层的错误（ConnectionError / OSError）会原样抛出来**。
    ``request_chunk`` 以前只接住 file_reference 过期、flood-wait、TimedOut 三种，
    于是一次代理抖动就会让 _ChunkSource 把异常塞进队列、download() 原样抛出，
    整个多 GB 的文件直接失败。下载耗时越长越必然撞上——这就是"大文件总是失败"。
    """

    class FlakySender:
        def __init__(self):
            self.connected = True
            self.reconnects = 0

        def is_connected(self):
            return self.connected

        async def connect(self, _connection):
            self.connected = True
            self.reconnects += 1

        async def disconnect(self):
            self.connected = False

    def setUp(self):
        # 退避里的 sleep 对测试没有意义，跳过它，别让单测跑几十秒
        self._real_sleep = asyncio.sleep

        async def no_sleep(_delay, result=None):
            return await self._real_sleep(0, result)

        fast_download.asyncio.sleep = no_sleep

    def tearDown(self):
        fast_download.asyncio.sleep = self._real_sleep

    def _make(self, exc_factory, fail_times):
        """造一个前 fail_times 次抛指定异常、之后正常返回的 transferrer。"""
        state = {"calls": 0}
        payload = b"x" * 32

        class C:
            session = type("S", (), {"dc_id": 2, "auth_key": object()})()
            _log = {}
            _proxy = None

            async def _call(self, sender, request):
                state["calls"] += 1
                if state["calls"] <= fail_times:
                    sender.connected = False        # 模拟连接被打断
                    raise exc_factory()
                return FakeResult(payload[request.offset:request.offset + request.limit])

            async def _get_dc(self, dc_id):
                return type("DC", (), {"ip_address": "1.2.3.4", "port": 443, "id": 2})()

            def _connection(self, *a, **kw):
                return object()

        client = C()
        transferrer = ParallelTransferrer(client, dc_id=None, max_chunk_retries=5)
        transferrer._location = object()
        return transferrer, state

    def test_connection_error_is_retried_and_sender_reconnected(self):
        transferrer, state = self._make(lambda: ConnectionError("connection reset"), fail_times=2)
        sender = self.FlakySender()

        async def scenario():
            return await transferrer.request_chunk(sender, 0, 16)

        data = asyncio.run(scenario())
        self.assertEqual(len(data), 16, "重试成功后应该拿到正常数据")
        self.assertEqual(state["calls"], 3, "前两次断连应该被重试掉")
        self.assertEqual(sender.reconnects, 2, "每次断连后都应该先重连再重试")
        self.assertTrue(sender.is_connected(), "重试前应该先把断掉的连接重连上")

    def test_os_error_is_retried(self):
        transferrer, state = self._make(lambda: OSError("proxy went away"), fail_times=1)
        sender = self.FlakySender()
        data = asyncio.run(transferrer.request_chunk(sender, 0, 16))
        self.assertEqual(len(data), 16)
        self.assertEqual(state["calls"], 2)

    def test_gives_up_after_max_retries(self):
        """一直连不上还是要失败，不能无限重试把任务挂死。"""
        transferrer, _state = self._make(lambda: ConnectionError("down"), fail_times=99)
        sender = self.FlakySender()
        with self.assertRaises(ConnectionError):
            asyncio.run(transferrer.request_chunk(sender, 0, 16))
