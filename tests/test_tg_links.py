"""tg_links 链接解析的单元测试。

运行：
    cd telegram-download-deamon
    python -m pytest tests/test_tg_links.py -v
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from tg_links import (  # noqa: E402
    has_telegram_link,
    parse_link,
    parse_telegram_links,
    utf16_slice,
)


class ParsePrivateLinkTests(unittest.TestCase):
    def test_basic_private_link(self):
        link = parse_link("https://t.me/c/1234567890/456")
        self.assertIsNotNone(link)
        self.assertEqual(link.kind, "private")
        self.assertEqual(link.channel_id, 1234567890)
        self.assertEqual(link.message_ids, (456,))
        self.assertIsNone(link.topic_id)
        self.assertFalse(link.single)

    def test_private_link_without_scheme(self):
        link = parse_link("t.me/c/1234567890/456")
        self.assertEqual(link.kind, "private")
        self.assertEqual(link.channel_id, 1234567890)
        self.assertEqual(link.message_ids, (456,))

    def test_private_link_with_topic(self):
        link = parse_link("https://t.me/c/1234567890/12/456")
        self.assertEqual(link.topic_id, 12)
        self.assertEqual(link.message_ids, (456,))

    def test_private_link_with_single_flag(self):
        link = parse_link("https://t.me/c/1234567890/456?single")
        self.assertTrue(link.single)
        self.assertEqual(link.message_ids, (456,))

    def test_private_link_range(self):
        link = parse_link("https://t.me/c/1234567890/456-460")
        self.assertEqual(link.message_ids, (456, 457, 458, 459, 460))

    def test_range_is_capped(self):
        link = parse_link("https://t.me/c/1/1-10000", max_range=5)
        self.assertEqual(link.message_ids, (1, 2, 3, 4, 5))

    def test_reversed_range_is_normalized(self):
        link = parse_link("https://t.me/c/1/10-8")
        self.assertEqual(link.message_ids, (8, 9, 10))

    def test_private_link_without_message_id(self):
        link = parse_link("https://t.me/c/1234567890")
        self.assertEqual(link.message_ids, ())

    def test_bad_channel_id(self):
        self.assertIsNone(parse_link("https://t.me/c/notanumber/456"))


class ParsePublicLinkTests(unittest.TestCase):
    def test_basic_public_link(self):
        link = parse_link("https://t.me/durov/123")
        self.assertEqual(link.kind, "public")
        self.assertEqual(link.username, "durov")
        self.assertEqual(link.message_ids, (123,))

    def test_web_preview_form(self):
        link = parse_link("https://t.me/s/durov/123")
        self.assertEqual(link.kind, "public")
        self.assertEqual(link.username, "durov")
        self.assertEqual(link.message_ids, (123,))

    def test_public_topic_link(self):
        link = parse_link("https://t.me/somegroup/25/900")
        self.assertEqual(link.topic_id, 25)
        self.assertEqual(link.message_ids, (900,))

    def test_comment_query(self):
        link = parse_link("https://t.me/durov/123?comment=456")
        self.assertEqual(link.comment_id, 456)
        self.assertEqual(link.message_ids, (123,))

    def test_telegram_me_alias(self):
        link = parse_link("https://telegram.me/durov/123")
        self.assertEqual(link.username, "durov")

    def test_reserved_paths_are_rejected(self):
        for url in [
            "https://t.me/addstickers/foo",
            "https://t.me/proxy?server=1",
            "https://t.me/share/url?url=x",
            "https://t.me/setlanguage/foo",
        ]:
            self.assertIsNone(parse_link(url), url)

    def test_non_telegram_host_rejected(self):
        self.assertIsNone(parse_link("https://example.com/c/123/456"))
        self.assertIsNone(parse_link("https://nott.me/c/123/456"))

    def test_too_short_username_rejected(self):
        self.assertIsNone(parse_link("https://t.me/ab/1"))


class ParseInviteLinkTests(unittest.TestCase):
    def test_plus_invite(self):
        link = parse_link("https://t.me/+AbCdEfGhIj/789")
        self.assertEqual(link.kind, "invite")
        self.assertEqual(link.invite_hash, "AbCdEfGhIj")
        self.assertEqual(link.message_ids, (789,))

    def test_joinchat_invite(self):
        link = parse_link("https://t.me/joinchat/AbCdEfGhIj")
        self.assertEqual(link.kind, "invite")
        self.assertEqual(link.invite_hash, "AbCdEfGhIj")
        self.assertEqual(link.message_ids, ())


class ParseTgSchemeTests(unittest.TestCase):
    def test_privatepost(self):
        link = parse_link("tg://privatepost?channel=1234567890&post=456")
        self.assertEqual(link.kind, "private")
        self.assertEqual(link.channel_id, 1234567890)
        self.assertEqual(link.message_ids, (456,))

    def test_resolve(self):
        link = parse_link("tg://resolve?domain=durov&post=456")
        self.assertEqual(link.kind, "public")
        self.assertEqual(link.username, "durov")
        self.assertEqual(link.message_ids, (456,))

    def test_unknown_action(self):
        self.assertIsNone(parse_link("tg://settings"))


class ExtractFromTextTests(unittest.TestCase):
    def test_extract_from_sentence(self):
        text = "帮我下这个 https://t.me/c/1234567890/456 谢谢"
        links = parse_telegram_links(text)
        self.assertEqual(len(links), 1)
        self.assertEqual(links[0].channel_id, 1234567890)

    def test_trailing_chinese_punctuation_trimmed(self):
        links = parse_telegram_links("看这个：https://t.me/c/1/2。")
        self.assertEqual(len(links), 1)
        self.assertEqual(links[0].message_ids, (2,))

    def test_multiple_links_deduplicated(self):
        text = (
            "https://t.me/c/1/2\n"
            "https://t.me/c/1/2\n"
            "https://t.me/c/1/3"
        )
        links = parse_telegram_links(text)
        self.assertEqual([l.message_ids for l in links], [(2,), (3,)])

    def test_links_without_message_id_are_skipped_by_default(self):
        self.assertEqual(parse_telegram_links("https://t.me/durov"), [])
        self.assertEqual(len(parse_telegram_links("https://t.me/durov", require_message_id=False)), 1)

    def test_extra_urls_from_entities(self):
        links = parse_telegram_links("点这里", extra_urls=["https://t.me/c/99/1"])
        self.assertEqual(len(links), 1)
        self.assertEqual(links[0].channel_id, 99)

    def test_malformed_urls_do_not_raise(self):
        """畸形 URL 只应该被忽略，不能把整条消息的处理带崩。"""
        for text in [
            "tg://[abc",
            "https://t.me/[::1/1",
            "t.me/",
            "t.me///",
            "tg://",
            "https://t.me/c//",
            "https://t.me/c/1/",
        ]:
            self.assertEqual(parse_telegram_links(text), [], text)

    def test_plain_text_without_links(self):
        self.assertEqual(parse_telegram_links("status"), [])
        self.assertFalse(has_telegram_link("list"))
        self.assertTrue(has_telegram_link("t.me/c/1/2"))

    def test_daemon_reply_text_does_not_self_trigger(self):
        """守护进程自己的回复里只有 describe()，不能被再次解析成任务。"""
        link = parse_link("https://t.me/c/1234567890/456")
        reply = f"✅ 已加入队列：{link.describe()}"
        self.assertEqual(parse_telegram_links(reply), [])


class Utf16SliceTests(unittest.TestCase):
    """Telegram 消息实体的 offset/length 以 UTF-16 码元计。"""

    def test_ascii_slice(self):
        self.assertEqual(utf16_slice("see https://t.me/c/1/2 now", 4, 18), "https://t.me/c/1/2")

    def test_emoji_shifts_offset(self):
        # "🎬 " 在 UTF-16 里占 3 个码元（代理对 2 + 空格 1），但只有 2 个 Python 字符
        text = "🎬 https://t.me/c/1/2"
        self.assertEqual(utf16_slice(text, 3, 18), "https://t.me/c/1/2")
        # 直接用字符下标会切偏，正好证明这个函数存在的意义
        self.assertNotEqual(text[3:3 + 18], "https://t.me/c/1/2")

    def test_out_of_range(self):
        self.assertEqual(utf16_slice("abc", 99, 5), "")
        self.assertEqual(utf16_slice("abc", 0, 0), "")
        self.assertEqual(utf16_slice("", 0, 3), "")


class DescribeTests(unittest.TestCase):
    def test_describe_private(self):
        self.assertEqual(
            parse_link("https://t.me/c/123/456").describe(), "私密频道 123 消息 456"
        )

    def test_describe_range(self):
        self.assertEqual(
            parse_link("https://t.me/c/123/456-458").describe(), "私密频道 123 消息 456~458"
        )

    def test_describe_public(self):
        self.assertEqual(parse_link("https://t.me/durov/1").describe(), "@durov 消息 1")


if __name__ == "__main__":
    unittest.main()
