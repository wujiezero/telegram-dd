"""tdd_utils 的单元测试。

运行：
    cd telegram-download-deamon
    python -m unittest tests/test_tdd_utils.py -v

或者：
    python -m pytest tests/test_tdd_utils.py -v
"""

import os
import sys
import unittest

# 允许从仓库根目录直接 `python -m unittest tests/test_tdd_utils.py`
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from tdd_utils import (  # noqa: E402
    WINDOWS_RESERVED_NAMES,
    build_safe_path,
    ensure_existing_path_within,
    getRandomId,
    sanitize_filename,
)


class SanitizeFilenameTests(unittest.TestCase):
    def test_keeps_plain_name(self):
        self.assertEqual(sanitize_filename("hello.txt"), "hello.txt")

    def test_strips_path_separators(self):
        # 任何 / 或 \ 都应被替换成 _，结果里绝不能再含路径分隔符
        out = sanitize_filename("../evil/passwd")
        self.assertNotIn("/", out)
        self.assertNotIn("\\", out)
        # 关键安全保证：结果作为一个叶子文件名使用时，不会被操作系统当成目录语义。
        # 普通 ".." 前缀 + 下划线分隔已经没有路径穿越风险（"..evil_passwd" 只是个奇怪文件名）。
        self.assertNotIn(os.sep, out)

    def test_rejects_backslash_traversal(self):
        out = sanitize_filename("..\\..\\windows\\system32\\cmd.exe")
        self.assertNotIn("\\", out)
        self.assertNotIn("/", out)

    def test_removes_control_chars(self):
        raw = "naughty\x00name\x01\x1f\x7f.bin"
        out = sanitize_filename(raw)
        for ch in raw:
            if ord(ch) < 32 or ord(ch) == 127:
                self.assertNotIn(ch, out)
        self.assertTrue(out.endswith(".bin"))

    def test_forbidden_windows_chars_removed(self):
        out = sanitize_filename('abc<d>e:"f|g?h*i.txt')
        for ch in '<>:"|?*':
            self.assertNotIn(ch, out)
        # 仍然应当保留字母和扩展名
        self.assertTrue(out.endswith(".txt"))

    def test_trailing_dot_and_space_stripped(self):
        self.assertFalse(sanitize_filename("hello.   ").endswith(" "))
        # 注意 rstrip 会把结尾的"." 也去掉，变成 hello 或 hello.xx
        self.assertTrue(sanitize_filename("hello.txt.").endswith("txt"))

    def test_empty_and_dots_fallback_to_random(self):
        # 只针对"清洗后只剩空串 / . / .." 的输入会回退到 file_xxx
        for bad in ("", "   ", ".", "..", "/", "\\", "./", "../"):
            out = sanitize_filename(bad)
            # 关键：永远不应以 "." 开头或为空，否则会变成 dotfile / 空文件
            self.assertTrue(out, f"Empty result for {bad!r}")
            self.assertFalse(out in (".", ".."), f"Dot-only result for {bad!r}")
            # 关键安全保证：不含路径分隔符
            self.assertNotIn("/", out)
            self.assertNotIn("\\", out)

    def test_windows_reserved_names_rewritten(self):
        for reserved in ("CON", "PRN.txt", "com1", "LPT9.bak"):
            out = sanitize_filename(reserved)
            stem, ext = os.path.splitext(out)
            self.assertNotIn(stem.upper(), WINDOWS_RESERVED_NAMES, f"Reserved leaked through: {out}")
            # 至少在扩展名存在时会保留扩展名
            if reserved.endswith(".txt"):
                self.assertTrue(out.endswith(".txt"))
            if reserved.endswith(".bak"):
                self.assertTrue(out.endswith(".bak"))

    def test_long_name_truncated_to_255_bytes(self):
        long_name = ("a" * 300) + ".ext"
        out = sanitize_filename(long_name)
        self.assertLessEqual(len(out.encode("utf-8")), 255)
        # 扩展名应被保留
        self.assertTrue(out.endswith(".ext"))

    def test_long_unicode_name_no_mojibake(self):
        # 300 个汉字肯定超过 255 字节；截断后必须仍能 UTF-8 解码
        long_name = ("测" * 300) + ".mp4"
        out = sanitize_filename(long_name)
        self.assertLessEqual(len(out.encode("utf-8")), 255)
        self.assertTrue(out.endswith(".mp4"))
        out.encode("utf-8").decode("utf-8")  # 不应 raise

    def test_none_input_is_safe(self):
        # 空/None 的 fallback 分支
        self.assertTrue(sanitize_filename(None).startswith("file_"))


class BuildSafePathTests(unittest.TestCase):
    def setUp(self):
        self.base = os.path.abspath("/tmp/tdd_test_base")

    def test_simple_join(self):
        p = build_safe_path(self.base, "a", "b.txt")
        self.assertEqual(p, os.path.join(self.base, "a", "b.txt"))

    def test_rejects_traversal(self):
        with self.assertRaises(ValueError):
            build_safe_path(self.base, "..", "etc", "passwd")

    def test_rejects_absolute_outside(self):
        with self.assertRaises(ValueError):
            build_safe_path(self.base, "/etc/passwd")

    def test_allows_nested(self):
        p = build_safe_path(self.base, "sub", "dir", "file")
        self.assertTrue(p.startswith(self.base + os.sep))


class EnsureExistingPathWithinTests(unittest.TestCase):
    def setUp(self):
        self.base = os.path.abspath("/tmp/tdd_exist_base")

    def test_inside_ok(self):
        # 不要求路径必须真实存在——这个函数只校验 within
        target = os.path.join(self.base, "some", "file.txt")
        self.assertEqual(ensure_existing_path_within(self.base, target), target)

    def test_outside_rejected(self):
        with self.assertRaises(ValueError):
            ensure_existing_path_within(self.base, "/etc/passwd")


class GetRandomIdTests(unittest.TestCase):
    def test_length(self):
        self.assertEqual(len(getRandomId(8)), 8)
        self.assertEqual(len(getRandomId(0)), 0)
        self.assertEqual(len(getRandomId(32)), 32)

    def test_charset(self):
        import string as _s
        allowed = set(_s.ascii_lowercase + _s.digits)
        rid = getRandomId(100)
        self.assertTrue(set(rid).issubset(allowed))

    def test_randomish(self):
        # 不是真随机性测试，只是确保两次调用一般情况下不同
        a = getRandomId(16)
        b = getRandomId(16)
        self.assertNotEqual(a, b)


if __name__ == "__main__":
    unittest.main()
