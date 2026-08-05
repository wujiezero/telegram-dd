"""守护进程里超时折算与断点判定两个纯函数的单元测试。

主文件 import 即会解析命令行、连数据库、连 Telegram，所以照搬
``tests/test_link_extraction.py`` 的做法：用 AST 把函数摘出来单独执行，
测的仍是真实源码。模块级全局（download_timeout / min_speed_bps / resume_enabled）
在单独编译后退化成普通的全局查找，直接填进 namespace 即可。

运行：
    cd telegram-download-deamon
    python -m pytest tests/test_download_timeout_resume.py -v
"""

import ast
import math
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from fast_download import align_down, choose_part_size  # noqa: E402

DAEMON_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "telegram-download-daemon.py")
)
WANTED = ("compute_download_timeout", "resumable_offset")


def _load_functions(**globals_override):
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
        "math": math,
        "os": os,
        "align_down": align_down,
        "choose_part_size": choose_part_size,
        # 被测函数读取的模块级配置，给出默认值后可按用例覆盖
        "download_timeout": 3600,
        "min_speed_bps": 49152,
        "resume_enabled": True,
    }
    namespace.update(globals_override)
    module = ast.Module(body=picked, type_ignores=[])
    exec(compile(module, DAEMON_PATH, "exec"), namespace)  # noqa: S102
    return namespace


class ComputeDownloadTimeoutTests(unittest.TestCase):
    def setUp(self):
        self.ns = _load_functions()
        self.compute = self.ns["compute_download_timeout"]

    def test_small_files_keep_the_floor(self):
        # 小文件按速度折算出来远小于下限，应当拿到下限值
        self.assertEqual(self.compute(1024), 3600)
        self.assertEqual(self.compute(10 * 1024**2), 3600)

    def test_large_files_scale_with_size(self):
        """截图里那个 2.57GB 的文件：老逻辑一刀切 3600s 必然超时，现在按体积给额度。"""
        size = int(2.57 * 1024**3)
        timeout = self.compute(size)
        self.assertGreater(timeout, 3600)
        self.assertEqual(timeout, math.ceil(size / 49152))
        # 730KB/s 下这个文件需要约 3600s，新额度必须宽裕地覆盖它
        self.assertGreater(timeout, size / (730 * 1024))

    def test_timeout_is_monotonic_in_size(self):
        sizes = [10 * 1024**2, 500 * 1024**2, 2 * 1024**3, 8 * 1024**3]
        timeouts = [self.compute(s) for s in sizes]
        self.assertEqual(timeouts, sorted(timeouts))

    def test_unknown_size_falls_back_to_the_floor(self):
        for bad in (0, None, -1, "", "abc"):
            self.assertEqual(self.compute(bad), 3600, f"size={bad!r}")

    def test_min_speed_is_configurable(self):
        ns = _load_functions(min_speed_bps=1024 * 1024)  # 1MB/s 线
        self.assertEqual(ns["compute_download_timeout"](10 * 1024**3), 10 * 1024)


class ResumableOffsetTests(unittest.TestCase):
    def setUp(self):
        self.ns = _load_functions()
        self.resumable = self.ns["resumable_offset"]
        self.tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)

    def _partial(self, nbytes):
        p = os.path.join(self.tmpdir.name, "partial.tdd")
        with open(p, "wb") as fh:
            fh.write(b"\0" * nbytes)
        return p

    def test_no_file_means_no_resume_point(self):
        self.assertEqual(self.resumable(None, 1000), 0)
        self.assertEqual(
            self.resumable(os.path.join(self.tmpdir.name, "nope.tdd"), 1000), 0)

    def test_offset_is_aligned_down_to_a_part_boundary(self):
        size = 2 * 1024**3               # 大文件 → 分块 512KB
        part = choose_part_size(size)
        downloaded = part * 7 + 12345    # 尾巴不足一整块
        offset = self.resumable(self._partial(downloaded), size)
        self.assertEqual(offset, part * 7)
        self.assertEqual(offset % part, 0)
        self.assertLess(offset, downloaded)

    def test_partial_smaller_than_one_part_is_not_resumable(self):
        size = 2 * 1024**3
        part = choose_part_size(size)
        self.assertEqual(self.resumable(self._partial(part - 1), size), 0)

    def test_empty_partial_is_not_resumable(self):
        self.assertEqual(self.resumable(self._partial(0), 2 * 1024**3), 0)

    def test_partial_not_smaller_than_target_is_rejected(self):
        """临时文件 >= 目标大小说明元信息对不上，宁可重下也不要拼出坏文件。"""
        size = 4 * 1024**2
        self.assertEqual(self.resumable(self._partial(size), size), 0)
        self.assertEqual(self.resumable(self._partial(size + 10), size), 0)

    def test_unknown_target_size_is_rejected(self):
        partial = self._partial(10 * 1024**2)
        for bad in (0, None, -1, "abc"):
            self.assertEqual(self.resumable(partial, bad), 0, f"size={bad!r}")

    def test_disabled_resume_never_returns_an_offset(self):
        ns = _load_functions(resume_enabled=False)
        partial = self._partial(100 * 1024**2)
        self.assertEqual(ns["resumable_offset"](partial, 2 * 1024**3), 0)

    def test_resume_point_saves_most_of_the_work(self):
        """2.57GB 的文件下到 2GB 时失败，续传应当只剩不到 600MB 要下。"""
        size = int(2.57 * 1024**3)
        downloaded = 2 * 1024**3
        offset = self.resumable(self._partial_sparse(downloaded), size)
        self.assertGreater(offset, downloaded - choose_part_size(size))
        self.assertLess(size - offset, 600 * 1024**2)

    def _partial_sparse(self, nbytes):
        """用稀疏文件造出指定大小，避免测试真写 2GB。"""
        p = os.path.join(self.tmpdir.name, "sparse.tdd")
        with open(p, "wb") as fh:
            fh.truncate(nbytes)
        return p


if __name__ == "__main__":
    unittest.main()
