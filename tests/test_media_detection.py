"""has_downloadable_media 的单元测试：网页链接预览不算可下载媒体。

2026-08-15 排查"我发的是视频链接，怎么下回来一张图"时发现的陷阱：Telethon 的
``Message.photo`` 和 ``Message.document`` 在取不到真实媒体时**都会回退到
web_preview**（网页链接预览）里的图或文件。于是一条"带链接预览的普通文本消息"
会被认成有可下载媒体，守护进程下回来一张预览缩略图还报成功。

这些替身刻意复刻了 Telethon 的回退语义（见 FakeMessage 的注释），
所以测的是真实的坑，而不是一个理想化的模型。

运行：
    cd telegram-download-deamon
    python -m pytest tests/test_media_detection.py -v
"""

import ast
import os
import unittest

DAEMON_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "telegram-download-daemon.py")
)
WANTED = ("get_message_object", "has_downloadable_media")


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

    namespace = {}
    module = ast.Module(body=picked, type_ignores=[])
    exec(compile(module, DAEMON_PATH, "exec"), namespace)  # noqa: S102
    return namespace


DAEMON = _load_functions()
has_downloadable_media = DAEMON["has_downloadable_media"]


class FakeWebPage:
    """网页预览。预览本身可以带图，也可以带文件（比如直链到一个 mp4）。"""

    def __init__(self, photo=None, document=None):
        self.photo = photo
        self.document = document


class FakeMessage:
    """复刻 Telethon Message 的 photo / document 回退语义。

    Telethon 源码：两个属性的 else 分支都是 ``web = self.web_preview``，
    真实媒体取不到时就返回预览里的东西。这正是要防的坑。
    """

    def __init__(self, real_photo=None, real_document=None, web_preview=None):
        self.web_preview = web_preview
        self._real_photo = real_photo
        self._real_document = real_document

    @property
    def photo(self):
        if self._real_photo is not None:
            return self._real_photo
        return getattr(self.web_preview, 'photo', None)

    @property
    def document(self):
        if self._real_document is not None:
            return self._real_document
        return getattr(self.web_preview, 'document', None)


class FakeEvent:
    """带 original_update 的事件替身，验证 get_message_object 的解包路径。"""

    def __init__(self, message):
        self.original_update = object()
        self.message = message


class HasDownloadableMediaTest(unittest.TestCase):

    def test_real_photo_is_downloadable(self):
        self.assertTrue(has_downloadable_media(FakeMessage(real_photo=object())))

    def test_real_document_is_downloadable(self):
        self.assertTrue(has_downloadable_media(FakeMessage(real_document=object())))

    def test_plain_text_message_is_not_downloadable(self):
        self.assertFalse(has_downloadable_media(FakeMessage()))

    def test_web_preview_photo_is_not_downloadable(self):
        """回归：贴一条带预览的链接，不能把预览缩略图当成用户要的图片下下来。"""
        message = FakeMessage(web_preview=FakeWebPage(photo=object()))
        # 先确认替身真的复刻了 Telethon 的坑，否则这条测试是空跑
        self.assertIsNotNone(message.photo,
                             "替身没复刻 Telethon 的回退语义，这条测试就失去意义了")
        self.assertFalse(has_downloadable_media(message))

    def test_web_preview_document_is_not_downloadable(self):
        """预览里直链了一个文件时同样不算——document 也会回退。"""
        message = FakeMessage(web_preview=FakeWebPage(document=object()))
        self.assertIsNotNone(message.document)
        self.assertFalse(has_downloadable_media(message))

    def test_web_preview_wins_even_if_both_look_present(self):
        """web_preview 非空就一票否决，不去猜 photo/document 是真媒体还是预览。"""
        message = FakeMessage(real_photo=object(),
                              web_preview=FakeWebPage(photo=object()))
        self.assertFalse(has_downloadable_media(message))

    def test_accepts_an_event_not_just_a_message(self):
        inner = FakeMessage(real_document=object())
        self.assertTrue(has_downloadable_media(FakeEvent(inner)))
        self.assertFalse(has_downloadable_media(
            FakeEvent(FakeMessage(web_preview=FakeWebPage(photo=object())))))


if __name__ == "__main__":
    unittest.main()
