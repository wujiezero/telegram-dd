"""守护进程 handle_link_message 编排逻辑的单元测试。

该函数是 ``start()`` 里的闭包，无法直接 import；这里同样用 AST 把它从真实源码里
摘出来单独编译，再把它依赖的 client / 入队函数换成替身，专测"预算、汇总、异常"
这套编排逻辑。

运行：
    cd telegram-download-deamon
    python -m pytest tests/test_link_handler.py -v
"""

import ast
import contextlib
import os
import sys
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from tg_links import parse_link  # noqa: E402

DAEMON_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "telegram-download-daemon.py")
)


def _load_handler(namespace):
    """把 handle_link_message 编译进给定的命名空间（闭包变量退化成全局查找）。"""
    with open(DAEMON_PATH, "r", encoding="utf-8") as handle:
        tree = ast.parse(handle.read(), filename=DAEMON_PATH)

    target = None
    for node in ast.walk(tree):
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "handle_link_message":
            target = node
            break
    if target is None:
        raise AssertionError("未能在守护进程源码里找到 handle_link_message")

    # 摘出来单独编译时缩进层级会变，ast 不关心缩进，但要清掉行号偏移带来的告警
    module = ast.Module(body=[target], type_ignores=[])
    ast.fix_missing_locations(module)
    exec(compile(module, DAEMON_PATH, "exec"), namespace)  # noqa: S102
    return namespace["handle_link_message"]


class FakeLogger:
    def __init__(self):
        self.records = []

    def _log(self, level, msg, *args, **kwargs):
        self.records.append((level, msg % args if args else msg))

    def info(self, msg, *a, **k):
        self._log("info", msg, *a, **k)

    def warning(self, msg, *a, **k):
        self._log("warning", msg, *a, **k)

    def error(self, msg, *a, **k):
        self._log("error", msg, *a, **k)


class FakeStatusMessage:
    def __init__(self, text):
        self.text = text
        self.edits = []

    async def edit(self, text):
        self.text = text
        self.edits.append(text)


class FakeSourceMessage:
    def __init__(self):
        self.replies = []

    async def reply(self, text):
        status = FakeStatusMessage(text)
        self.replies.append(status)
        return status


class FakeEvent:
    def __init__(self):
        self.message = FakeSourceMessage()


class FakeMedia:
    """带媒体的消息替身（photo 非空即视为可下载）。"""

    def __init__(self, message_id, has_media=True):
        self.id = message_id
        self.photo = object() if has_media else None
        self.document = None


async def _passthrough_notify(rpc_coro, what):
    """bounded_notify 的透传替身：测试里不关心超时，只要行为等价于直接 await。"""
    return await rpc_coro


class HandleLinkMessageTests(unittest.IsolatedAsyncioTestCase):
    def _build(self, fetch, enqueue, max_messages=50, status_limit=5):
        self.logger = FakeLogger()
        namespace = {
            "contextlib": contextlib,
            "logger": self.logger,
            "link_max_messages": max_messages,
            "LINK_STATUS_MESSAGE_LIMIT": status_limit,
            "fetch_link_messages": fetch,
            "enqueue_download_message": enqueue,
            "bounded_notify": _passthrough_notify,
        }
        return _load_handler(namespace)

    async def test_single_link_queues_media(self):
        enqueued = []

        async def fetch(link, budget):
            return [FakeMedia(456)]

        async def enqueue(message_obj, template, reply_target=None, silent=False, **kwargs):
            enqueued.append((message_obj.id, silent))
            return {"queued": True, "filename": f"{message_obj.id}.mp4"}

        handler = self._build(fetch, enqueue)
        event = FakeEvent()
        await handler(event, [parse_link("https://t.me/c/123/456")])

        self.assertEqual(enqueued, [(456, False)])
        summary = event.message.replies[0].text
        self.assertIn("入队 1 个文件", summary)
        self.assertIn("私密频道 123 消息 456", summary)

    async def test_messages_without_media_are_skipped(self):
        async def fetch(link, budget):
            return [FakeMedia(1, has_media=False), FakeMedia(2, has_media=False)]

        async def enqueue(*a, **k):
            raise AssertionError("没有媒体的消息不应该入队")

        handler = self._build(fetch, enqueue)
        event = FakeEvent()
        await handler(event, [parse_link("https://t.me/c/1/1-2")])

        summary = event.message.replies[0].text
        self.assertIn("入队 0 个文件", summary)
        self.assertIn("跳过 2 条", summary)
        self.assertIn("没有可下载的文件", summary)

    async def test_budget_limits_total_files(self):
        enqueued = []

        async def fetch(link, budget):
            # 故意多返回一些，验证 budget 在内层循环里也会刹车
            return [FakeMedia(i) for i in range(1, 11)]

        async def enqueue(message_obj, template, reply_target=None, silent=False, **kwargs):
            enqueued.append(message_obj.id)
            return {"queued": True, "filename": "x.mp4"}

        handler = self._build(fetch, enqueue, max_messages=3)
        event = FakeEvent()
        await handler(event, [parse_link("https://t.me/c/1/1-10")])

        self.assertEqual(len(enqueued), 3)
        summary = event.message.replies[0].text
        self.assertIn("入队 3 个文件", summary)
        self.assertIn("上限", summary)

    async def test_status_message_limit_switches_to_silent(self):
        silent_flags = []

        async def fetch(link, budget):
            return [FakeMedia(i) for i in range(1, 9)]

        async def enqueue(message_obj, template, reply_target=None, silent=False, **kwargs):
            silent_flags.append(silent)
            return {"queued": True, "filename": "x.mp4"}

        handler = self._build(fetch, enqueue, status_limit=2)
        event = FakeEvent()
        await handler(event, [parse_link("https://t.me/c/1/1-8")])

        # 前 2 个逐条回复，之后静默入队
        self.assertEqual(silent_flags[:2], [False, False])
        self.assertTrue(all(silent_flags[2:]))

    async def test_resolve_failure_is_reported_not_raised(self):
        async def fetch(link, budget):
            raise ValueError("当前账号没有这个私密频道的访问权限（需要先加入该频道）")

        async def enqueue(*a, **k):
            raise AssertionError("解析失败时不应该入队")

        handler = self._build(fetch, enqueue)
        event = FakeEvent()
        await handler(event, [parse_link("https://t.me/c/123/456")])

        summary = event.message.replies[0].text
        self.assertIn("没有这个私密频道的访问权限", summary)
        self.assertIn("入队 0 个文件", summary)

    async def test_missing_message_is_reported(self):
        async def fetch(link, budget):
            return []

        async def enqueue(*a, **k):
            raise AssertionError("没有消息时不应该入队")

        handler = self._build(fetch, enqueue)
        event = FakeEvent()
        await handler(event, [parse_link("https://t.me/c/123/456")])

        self.assertIn("消息不存在或已被删除", event.message.replies[0].text)

    async def test_multiple_links_each_get_a_note(self):
        async def fetch(link, budget):
            if link.channel_id == 1:
                raise ValueError("boom")
            return [FakeMedia(7)]

        async def enqueue(message_obj, template, reply_target=None, silent=False, **kwargs):
            return {"queued": True, "filename": "x.mp4"}

        handler = self._build(fetch, enqueue)
        event = FakeEvent()
        await handler(
            event,
            [parse_link("https://t.me/c/1/1"), parse_link("https://t.me/c/2/7")],
        )

        summary = event.message.replies[0].text
        self.assertIn("私密频道 1 消息 1", summary)
        self.assertIn("私密频道 2 消息 7", summary)
        self.assertIn("入队 1 个文件", summary)

    async def test_summary_never_contains_a_parsable_link(self):
        """汇总回复会再次进入 NewMessage 事件，绝不能带上可解析的 t.me 链接。"""
        from tg_links import parse_telegram_links

        async def fetch(link, budget):
            return [FakeMedia(456)]

        async def enqueue(message_obj, template, reply_target=None, silent=False, **kwargs):
            return {"queued": True, "filename": "x.mp4"}

        handler = self._build(fetch, enqueue)
        event = FakeEvent()
        await handler(event, [parse_link("https://t.me/c/1234567890/456?single")])

        for reply in event.message.replies:
            for text in [reply.text] + reply.edits:
                self.assertEqual(parse_telegram_links(text), [], text)

    async def test_reply_failure_does_not_break_handling(self):
        """频道禁言 / 限流导致回复失败时，入队流程仍要跑完。"""
        enqueued = []

        async def fetch(link, budget):
            return [FakeMedia(1)]

        async def enqueue(message_obj, template, reply_target=None, silent=False, **kwargs):
            enqueued.append(message_obj.id)
            return {"queued": True, "filename": "x.mp4"}

        handler = self._build(fetch, enqueue)

        class ExplodingMessage(FakeSourceMessage):
            async def reply(self, text):
                raise RuntimeError("CHAT_WRITE_FORBIDDEN")

        event = FakeEvent()
        event.message = ExplodingMessage()
        await handler(event, [parse_link("https://t.me/c/1/1")])

        self.assertEqual(enqueued, [1])


if __name__ == "__main__":
    unittest.main()
