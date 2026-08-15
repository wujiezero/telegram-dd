"""切换代理前打断在跑的下载（interrupt_downloads_for_proxy_switch）。

2026-08-15 实测发现：切代理时只断主连接是不彻底的。fast_download 给每个文件另开
独立的 MTProtoSender，代理是建连接时从 client._proxy 读的，client.disconnect()
带不走它们——切换瞬间恰好没报错的任务会继续用**旧代理**下到文件结束（当时切换
4 分钟后仍有 2 条连接挂在旧代理上）。

这里守住三条性质：

1. 所有在跑的下载任务都被 cancel；
2. **绝不往 cancelled_download_ids 里写**——那是"用户主动取消"的语义，worker 见到
   会删半成品且不再重试；这里要的是保留 .tdd + 重新入队 + 断点续传；
3. 已经结束的任务不重复 cancel，没有在跑的任务时是干净的空操作。

该函数是 start() 里的闭包，用 AST 摘出来单独编译（见 test_link_handler.py）。
"""

import ast
import asyncio
import os
import unittest

DAEMON_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "telegram-download-daemon.py")
)


class FakeLogger:
    def __init__(self):
        self.messages = []

    def info(self, msg, *args):
        self.messages.append(msg % args if args else msg)

    def warning(self, *a, **k):
        pass


def _load_interrupter(namespace):
    with open(DAEMON_PATH, "r", encoding="utf-8") as handle:
        tree = ast.parse(handle.read(), filename=DAEMON_PATH)

    target = None
    for node in ast.walk(tree):
        if (isinstance(node, ast.AsyncFunctionDef)
                and node.name == "interrupt_downloads_for_proxy_switch"):
            target = node
            break
    if target is None:
        raise AssertionError("未能在守护进程源码里找到 interrupt_downloads_for_proxy_switch")

    module = ast.Module(body=[target], type_ignores=[])
    ast.fix_missing_locations(module)
    exec(compile(module, DAEMON_PATH, "exec"), namespace)  # noqa: S102
    return namespace["interrupt_downloads_for_proxy_switch"]


class InterruptDownloadsTest(unittest.IsolatedAsyncioTestCase):

    def _build(self, tasks):
        self.logger = FakeLogger()
        self.cancelled_ids = set()
        namespace = {
            "asyncio": asyncio,
            "contextlib": __import__("contextlib"),
            "logger": self.logger,
            "active_download_tasks": tasks,
            "cancelled_download_ids": self.cancelled_ids,
        }
        return _load_interrupter(namespace)

    async def test_cancels_every_running_download(self):
        async def forever():
            await asyncio.sleep(300)

        running = {str(i): asyncio.ensure_future(forever()) for i in range(3)}
        interrupt = self._build(running)

        count = await interrupt()

        self.assertEqual(count, 3)
        for task in running.values():
            self.assertTrue(task.cancelled() or task.done())

    async def test_never_marks_them_as_user_cancelled(self):
        """最关键的一条：走了用户取消的语义就会删半成品、不再重试。"""
        async def forever():
            await asyncio.sleep(300)

        running = {"42": asyncio.ensure_future(forever())}
        interrupt = self._build(running)

        await interrupt()

        self.assertEqual(self.cancelled_ids, set(),
                         "打断下载不能写 cancelled_download_ids，否则半成品会被删且不再重试")

    async def test_partial_downloads_can_still_unwind_their_cleanup(self):
        """被 cancel 的任务有机会跑完自己的清理（对应 transferrer.finish 断开并行 sender）。

        注意必须先让任务真正开始跑再打断：一个还没被调度过的任务被 cancel 时，
        协程根本没进入 try，清理逻辑自然不会执行。生产里下载任务早就在跑了，
        所以这里要还原"已经在下载"的状态，否则测的是一个不存在的场景。
        """
        cleaned = []

        async def with_cleanup():
            try:
                await asyncio.sleep(300)
            except asyncio.CancelledError:
                cleaned.append("finally ran")
                raise

        running = {"7": asyncio.ensure_future(with_cleanup())}
        await asyncio.sleep(0)  # 让任务真正进入 try
        interrupt = self._build(running)

        await interrupt()

        self.assertEqual(cleaned, ["finally ran"])

    async def test_finished_tasks_are_not_counted(self):
        async def done_immediately():
            return "done"

        finished = asyncio.ensure_future(done_immediately())
        await finished

        interrupt = self._build({"1": finished})
        self.assertEqual(await interrupt(), 0)

    async def test_no_active_downloads_is_a_noop(self):
        interrupt = self._build({})
        self.assertEqual(await interrupt(), 0)
        self.assertEqual(self.logger.messages, [])

    async def test_none_entries_are_tolerated(self):
        interrupt = self._build({"1": None})
        self.assertEqual(await interrupt(), 0)


if __name__ == "__main__":
    unittest.main()
