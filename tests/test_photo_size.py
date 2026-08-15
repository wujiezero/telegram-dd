"""照片体积换算的单元测试（photo_size_byte_count / get_message_media_size）。

2026-08-15 生产事故：从私密频道链接下过来的照片 100% 失败，报
``Downloaded size mismatch: got 29746 bytes, expected 9953``。原因是取"预期大小"
时只读了 ``PhotoSize.size`` 这一个字段，而 Telegram 的 5 个 PhotoSize 变体里只有
``PhotoSize`` 有这个字段——分辨率最高、Telethon 真正会下载的 ``PhotoSizeProgressive``
用的是复数 ``sizes``（列表），于是"预期大小"退化成了某个较小变体，下载完的校验必然对不上。

这里既用真实的 Telethon 类型对齐口径（防止我们和 Telethon 的算法漂移），
也直接复现生产上那两组数字。主文件不可 import，用 AST 摘函数（见 test_link_extraction.py）。
"""

import ast
import os
import unittest

from telethon.tl.types import (  # noqa: E402
    PhotoCachedSize,
    PhotoPathSize,
    PhotoSize,
    PhotoSizeEmpty,
    PhotoSizeProgressive,
    PhotoStrippedSize,
)
from telethon.utils import _photo_size_byte_count as telethon_byte_count  # noqa: E402

DAEMON_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "telegram-download-daemon.py")
)
WANTED = ("get_message_object", "photo_size_byte_count", "get_message_media_size")


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
photo_size_byte_count = DAEMON["photo_size_byte_count"]
get_message_media_size = DAEMON["get_message_media_size"]


class FakePhoto:
    def __init__(self, sizes):
        self.sizes = sizes


class FakePhotoMessage:
    """只有照片的消息替身。Telethon 的 Message.document 在这种情况下是 None。"""

    def __init__(self, sizes):
        self.photo = FakePhoto(sizes)
        self.document = None


class FakeDocument:
    def __init__(self, size):
        self.size = size


class FakeDocumentMessage:
    def __init__(self, size):
        self.photo = None
        self.document = FakeDocument(size)


class PhotoSizeByteCountTest(unittest.TestCase):
    """逐个变体核对，并和 Telethon 自己的算法对齐。"""

    def test_plain_photo_size_uses_size_field(self):
        variant = PhotoSize(type="m", w=320, h=240, size=9953)
        self.assertEqual(photo_size_byte_count(variant), 9953)
        self.assertEqual(photo_size_byte_count(variant), telethon_byte_count(variant))

    def test_progressive_takes_max_of_sizes_list(self):
        # 这就是踩坑的那个变体：字段是复数 sizes，没有 size
        variant = PhotoSizeProgressive(
            type="x", w=1280, h=960, sizes=[1234, 9953, 29746]
        )
        self.assertFalse(hasattr(variant, "size"),
                         "PhotoSizeProgressive 不应该有 size 字段，有的话这个测试的前提就变了")
        self.assertEqual(photo_size_byte_count(variant), 29746)
        self.assertEqual(photo_size_byte_count(variant), telethon_byte_count(variant))

    def test_cached_size_counts_its_bytes(self):
        variant = PhotoCachedSize(type="s", w=90, h=90, bytes=b"\x00" * 512)
        self.assertEqual(photo_size_byte_count(variant), 512)
        self.assertEqual(photo_size_byte_count(variant), telethon_byte_count(variant))

    def test_stripped_size_adds_the_jpeg_header_and_footer(self):
        # 首字节为 1 = 掐掉了标准 JPEG 头尾，下载时补回来正好 622 字节
        variant = PhotoStrippedSize(type="i", bytes=bytes([1, 2, 3]) + b"\x00" * 50)
        self.assertEqual(photo_size_byte_count(variant), 53 + 622)
        self.assertEqual(photo_size_byte_count(variant), telethon_byte_count(variant))

    def test_stripped_size_without_the_marker_byte_is_raw(self):
        variant = PhotoStrippedSize(type="i", bytes=bytes([9, 9, 9, 9]))
        self.assertEqual(photo_size_byte_count(variant), 4)
        self.assertEqual(photo_size_byte_count(variant), telethon_byte_count(variant))

    def test_empty_size_is_zero(self):
        self.assertEqual(photo_size_byte_count(PhotoSizeEmpty(type="")), 0)

    def test_path_size_is_ignored(self):
        # 动画贴纸的 SVG 轮廓，不是图片，Telethon 会把它从候选里剔除
        self.assertEqual(photo_size_byte_count(PhotoPathSize(type="j", bytes=b"\x01" * 80)), 0)

    def test_unknown_variant_is_zero(self):
        self.assertEqual(photo_size_byte_count(object()), 0)


class GetMessageMediaSizeTest(unittest.TestCase):

    def test_progressive_variant_wins_over_smaller_plain_sizes(self):
        """回归：生产事故的原始形态。

        got 29746 / expected 9953——最大的那个变体是 progressive，老代码读不到它，
        于是拿 PhotoSize 的 9953 当预期值，和实际下到的 29746 对不上。
        """
        message = FakePhotoMessage([
            PhotoStrippedSize(type="i", bytes=bytes([1]) + b"\x00" * 40),
            PhotoSize(type="m", w=320, h=240, size=9953),
            PhotoSizeProgressive(type="x", w=1280, h=960, sizes=[5000, 15000, 29746]),
        ])
        self.assertEqual(get_message_media_size(message), 29746)

    def test_second_production_case(self):
        """另一条链接：got 137741 / expected 89596。"""
        message = FakePhotoMessage([
            PhotoSize(type="x", w=800, h=600, size=89596),
            PhotoSizeProgressive(type="y", w=1600, h=1200, sizes=[40000, 89596, 137741]),
        ])
        self.assertEqual(get_message_media_size(message), 137741)

    def test_matches_the_variant_telethon_would_download(self):
        """口径一致性：我们挑的必须就是 Telethon 排序后取到的那个（字节数最大）。"""
        variants = [
            PhotoStrippedSize(type="i", bytes=bytes([1]) + b"\x00" * 30),
            PhotoSize(type="m", w=320, h=240, size=9953),
            PhotoSizeProgressive(type="x", w=1280, h=960, sizes=[5000, 29746]),
            PhotoCachedSize(type="s", w=90, h=90, bytes=b"\x00" * 700),
        ]
        expected = max(telethon_byte_count(v) for v in variants)
        self.assertEqual(get_message_media_size(FakePhotoMessage(variants)), expected)

    def test_photo_without_sizes_is_zero(self):
        self.assertEqual(get_message_media_size(FakePhotoMessage([])), 0)

    def test_document_size_is_untouched(self):
        self.assertEqual(get_message_media_size(FakeDocumentMessage(596846720)), 596846720)


if __name__ == "__main__":
    unittest.main()
