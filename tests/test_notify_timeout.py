"""bounded_notify / log_reply 的超时保险丝测试。

2026-08-14 生产事故：主连接陷入重连风暴时 Telethon 请求无限挂起且不抛错，
两个 worker 全部冻死在重试通知的 message.edit() 上，队列永远 "Waiting for
download"。这里守住三条性质：

1. 通知 RPC 挂起会被超时砍掉——bounded_notify 在超时后返回 None，不挂死调用方；
2. RPC 抛出的异常被吞掉（通知丢了就丢了，流水线继续走）；
3. 外部取消（CancelledError）仍然向上传播，暂停/取消机制不受影响。

主文件不可 import（见 test_link_extraction.py 的说明），用 AST 把
``bounded_notify`` / ``log_reply`` 两个 async 函数摘出来单独执行。
"""

import ast
import asyncio
import logging
import os
import time
import unittest
from types import SimpleNamespace

DAEMON_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "telegram-download-daemon.py")
)
WANTED = ("bounded_notify", "log_reply")

# 测试里把超时压到 50ms，让"挂起被砍"跑得飞快
TEST_TIMEOUT = 0.05


def _load_functions():
    with open(DAEMON_PATH, "r", encoding="utf-8") as handle:
        tree = ast.parse(handle.read(), filename=DAEMON_PATH)

    picked = [
        node for node in tree.body
        if isinstance(node, ast.AsyncFunctionDef) and node.name in WANTED
    ]
    missing = set(WANTED) - {node.name for node in picked}
    if missing:
        raise AssertionError(f"未能在守护进程源码里找到函数：{sorted(missing)}")

    namespace = {
        "asyncio": asyncio,
        "logger": logging.getLogger("test-bounded-notify"),
        "TELEGRAM_DAEMON_NOTIFY_RPC_TIMEOUT": TEST_TIMEOUT,
        # log_reply 的签名注解在 def 时求值，需要一个 events 替身
        "events": SimpleNamespace(NewMessage=SimpleNamespace(Event=object)),
    }
    module = ast.Module(body=picked, type_ignores=[])
    exec(compile(module, DAEMON_PATH, "exec"), namespace)  # noqa: S102
    return namespace


NAMESPACE = _load_functions()
bounded_notify = NAMESPACE["bounded_notify"]
log_reply = NAMESPACE["log_reply"]


async def _hang_forever():
    await asyncio.sleep(30)


async def _return_value():
    return "rpc-result"


async def _raise_error():
    raise RuntimeError("boom")


class HangingMessage:
    """edit() 永不返回的 Telethon Message 替身——重连风暴下的真实形态。"""

    def edit(self, text):
        return _hang_forever()


class FastMessage:
    """edit() 正常完成的替身。"""

    def __init__(self):
        self.edited = []

    async def edit(self, text):
        self.edited.append(text)
        return "edited-ok"


class BoundedNotifyTest(unittest.TestCase):

    def test_success_returns_rpc_result(self):
        result = asyncio.run(bounded_notify(_return_value(), "test"))
        self.assertEqual(result, "rpc-result")

    def test_hanging_rpc_is_cut_and_returns_none(self):
        started = time.monotonic()
        result = asyncio.run(bounded_notify(_hang_forever(), "test"))
        elapsed = time.monotonic() - started
        self.assertIsNone(result)
        # 挂起的 RPC 必须在超时（50ms）附近被砍掉，而不是等它 30 秒
        self.assertLess(elapsed, 2.0)

    def test_rpc_exception_is_swallowed(self):
        result = asyncio.run(bounded_notify(_raise_error(), "test"))
        self.assertIsNone(result)

    def test_external_cancellation_propagates(self):
        async def scenario():
            task = asyncio.ensure_future(bounded_notify(_hang_forever(), "test"))
            await asyncio.sleep(0.01)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task

        asyncio.run(scenario())


class LogReplyTest(unittest.TestCase):

    def test_none_message_only_prints(self):
        asyncio.run(log_reply(None, "hello"))

    def test_normal_edit_goes_through(self):
        message = FastMessage()
        asyncio.run(log_reply(message, "hello"))
        self.assertEqual(message.edited, ["hello"])

    def test_hanging_edit_does_not_freeze_caller(self):
        started = time.monotonic()
        asyncio.run(log_reply(HangingMessage(), "hello"))
        elapsed = time.monotonic() - started
        self.assertLess(elapsed, 2.0)


if __name__ == "__main__":
    unittest.main()
