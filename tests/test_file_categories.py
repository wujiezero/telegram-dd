"""getFileTypeCategory 与分页归一化的单元测试。

运行：
    cd telegram-download-deamon
    python -m pytest tests/test_file_categories.py -v
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from tdd_utils import (  # noqa: E402
    FILE_TYPE_RULES,
    compute_total_pages,
    getFileTypeCategory,
    normalize_pagination,
)


class GetFileTypeCategoryTests(unittest.TestCase):
    def test_music(self):
        self.assertEqual(getFileTypeCategory("song.mp3"), "Music")
        self.assertEqual(getFileTypeCategory("track.FLAC"), "Music")

    def test_videos(self):
        self.assertEqual(getFileTypeCategory("movie.mp4"), "Videos")
        self.assertEqual(getFileTypeCategory("clip.MKV"), "Videos")

    def test_pictures(self):
        self.assertEqual(getFileTypeCategory("photo.jpg"), "Pictures")
        self.assertEqual(getFileTypeCategory("icon.PNG"), "Pictures")
        self.assertEqual(getFileTypeCategory("live.heic"), "Pictures")

    def test_documents(self):
        self.assertEqual(getFileTypeCategory("report.pdf"), "Documents")
        self.assertEqual(getFileTypeCategory("notes.md"), "Documents")

    def test_ignore_list(self):
        self.assertEqual(getFileTypeCategory("download.part"), "IGNORE")
        self.assertEqual(getFileTypeCategory("Thumbs.desktop"), "IGNORE")

    def test_compound_tar_gz(self):
        self.assertEqual(getFileTypeCategory("backup.tar.gz"), "Archives")
        self.assertEqual(getFileTypeCategory("dump.tar.bz2"), "Archives")
        self.assertEqual(getFileTypeCategory("data.tar.xz"), "Archives")

    def test_compound_falls_back_to_last_ext(self):
        # foo.bar.mp4 不是已知复合扩展名，应按最后一段 mp4 判定
        self.assertEqual(getFileTypeCategory("foo.bar.mp4"), "Videos")

    def test_unknown_extension_is_other(self):
        self.assertEqual(getFileTypeCategory("mystery.xyz"), "Other")

    def test_no_extension_is_other(self):
        self.assertEqual(getFileTypeCategory("README"), "Other")

    def test_empty_or_none_is_other(self):
        self.assertEqual(getFileTypeCategory(""), "Other")
        self.assertEqual(getFileTypeCategory(None), "Other")

    def test_every_rule_extension_maps_back(self):
        # 防回归：规则表里的每个扩展名都应能被正确归类
        for category, exts in FILE_TYPE_RULES.items():
            for ext in exts:
                result = getFileTypeCategory(f"file.{ext}")
                if category == "IGNORE":
                    self.assertEqual(result, "IGNORE", f"{ext} 应被忽略")
                else:
                    # 某些扩展名跨类别（如 deb/rpm/sh），只要命中一个合法类别即可
                    self.assertIn(
                        result,
                        [c for c in FILE_TYPE_RULES if c != "IGNORE"],
                        f"{ext} 未能归入任何类别，得到 {result}",
                    )


class NormalizePaginationTests(unittest.TestCase):
    def test_defaults(self):
        page, per_page, offset = normalize_pagination(1, 10)
        self.assertEqual((page, per_page, offset), (1, 10, 0))

    def test_offset_calculation(self):
        page, per_page, offset = normalize_pagination(3, 20)
        self.assertEqual((page, per_page, offset), (3, 20, 40))

    def test_zero_per_page_falls_back_to_default(self):
        # 关键：避免 ZeroDivisionError
        _, per_page, _ = normalize_pagination(1, 0, default_per_page=10)
        self.assertEqual(per_page, 10)

    def test_negative_per_page_falls_back(self):
        # 关键：避免 SQLite LIMIT -1 返回全表
        _, per_page, _ = normalize_pagination(1, -5, default_per_page=10)
        self.assertEqual(per_page, 10)

    def test_per_page_capped_at_max(self):
        _, per_page, _ = normalize_pagination(1, 100000, max_per_page=200)
        self.assertEqual(per_page, 200)

    def test_page_below_one_resets(self):
        page, _, offset = normalize_pagination(0, 10)
        self.assertEqual(page, 1)
        self.assertEqual(offset, 0)
        page, _, _ = normalize_pagination(-3, 10)
        self.assertEqual(page, 1)

    def test_non_integer_inputs(self):
        page, per_page, offset = normalize_pagination(None, "abc")
        self.assertEqual((page, per_page, offset), (1, 10, 0))

    def test_string_numbers_accepted(self):
        page, per_page, offset = normalize_pagination("2", "15")
        self.assertEqual((page, per_page, offset), (2, 15, 15))


class ComputeTotalPagesTests(unittest.TestCase):
    def test_exact_division(self):
        self.assertEqual(compute_total_pages(100, 10), 10)

    def test_rounds_up(self):
        self.assertEqual(compute_total_pages(101, 10), 11)
        self.assertEqual(compute_total_pages(1, 10), 1)

    def test_zero_total(self):
        self.assertEqual(compute_total_pages(0, 10), 0)

    def test_zero_per_page_is_safe(self):
        # 不应抛 ZeroDivisionError
        self.assertEqual(compute_total_pages(100, 0), 0)


if __name__ == "__main__":
    unittest.main()
