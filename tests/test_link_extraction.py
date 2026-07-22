"""telegram-download-daemon.py 里链接抽取函数的单元测试。

守护进程主文件在 import 时就会解析命令行参数、初始化数据库并连接 Telegram，
没法直接 import。这里用 AST 把 ``extract_entity_urls`` / ``extract_telegram_links``
两个纯函数从源码里摘出来单独执行——测的仍然是真实源码，不是副本。

运行：
    cd telegram-download-deamon
    python -m pytest tests/test_link_extraction.py -v
"""

import ast
import os
import sys
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from telethon.tl.types import MessageEntityTextUrl, MessageEntityUrl  # noqa: E402

from tg_links import parse_telegram_links, utf16_slice  # noqa: E402

DAEMON_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "telegram-download-daemon.py")
)
WANTED = ("get_message_object", "extract_entity_urls", "extract_telegram_links")


def _load_functions():
    with open(DAEMON_PATH, "r", encoding="utf-8") as handle:
        tree = ast.parse(handle.read(), filename=DAEMON_PATH)

    picked = [
        node for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name in WANTED
    ]
    missing = set(WANTED) - {node.name for node in picked}
    if missing:
        raise AssertionError(f"未能在守护进程源码里找到函数：{sorted(missing)}")

    namespace = {
        "MessageEntityTextUrl": MessageEntityTextUrl,
        "MessageEntityUrl": MessageEntityUrl,
        "parse_telegram_links": parse_telegram_links,
        "utf16_slice": utf16_slice,
    }
    module = ast.Module(body=picked, type_ignores=[])
    exec(compile(module, DAEMON_PATH, "exec"), namespace)  # noqa: S102
    return namespace


DAEMON = _load_functions()


class FakeMessage:
    """够用的 Telethon Message 替身：只有链接抽取用到的两个属性。"""

    def __init__(self, text, entities=None):
        self.message = text
        self.entities = entities or []


class ExtractEntityUrlsTests(unittest.TestCase):
    def test_text_url_entity(self):
        message = FakeMessage(
            "点这里下载",
            [MessageEntityTextUrl(offset=0, length=5, url="https://t.me/c/123/456")],
        )
        self.assertEqual(
            DAEMON["extract_entity_urls"](message), ["https://t.me/c/123/456"]
        )

    def test_url_entity_offsets_are_utf16(self):
        text = "🎬 https://t.me/c/123/456"
        message = FakeMessage(text, [MessageEntityUrl(offset=3, length=22)])
        self.assertEqual(
            DAEMON["extract_entity_urls"](message), ["https://t.me/c/123/456"]
        )

    def test_no_entities(self):
        self.assertEqual(DAEMON["extract_entity_urls"](FakeMessage("hello")), [])


class ExtractTelegramLinksTests(unittest.TestCase):
    def test_plain_text_link(self):
        links = DAEMON["extract_telegram_links"](
            FakeMessage("帮我下 https://t.me/c/1234567890/456"), 50
        )
        self.assertEqual(len(links), 1)
        self.assertEqual(links[0].channel_id, 1234567890)
        self.assertEqual(links[0].message_ids, (456,))

    def test_hidden_link_in_entity_only(self):
        """正文完全没有链接文本，真链接藏在 text_url 实体里。"""
        message = FakeMessage(
            "这个视频",
            [MessageEntityTextUrl(offset=0, length=4, url="https://t.me/c/99/1")],
        )
        links = DAEMON["extract_telegram_links"](message, 50)
        self.assertEqual(len(links), 1)
        self.assertEqual(links[0].channel_id, 99)

    def test_text_and_entity_are_deduplicated(self):
        url = "https://t.me/c/1/2"
        message = FakeMessage(url, [MessageEntityUrl(offset=0, length=len(url))])
        links = DAEMON["extract_telegram_links"](message, 50)
        self.assertEqual(len(links), 1)

    def test_max_messages_caps_range_links(self):
        links = DAEMON["extract_telegram_links"](
            FakeMessage("https://t.me/c/1/100-999"), 3
        )
        self.assertEqual(links[0].message_ids, (100, 101, 102))

    def test_commands_are_not_links(self):
        for command in ("list", "status", "clean", "queue"):
            self.assertEqual(DAEMON["extract_telegram_links"](FakeMessage(command), 50), [])

    def test_empty_message(self):
        self.assertEqual(DAEMON["extract_telegram_links"](FakeMessage(""), 50), [])
        self.assertEqual(DAEMON["extract_telegram_links"](FakeMessage(None), 50), [])


if __name__ == "__main__":
    unittest.main()
