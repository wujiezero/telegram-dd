"""下载记录状态流转的 SQL 测试（含 start_time 重新打点）。

主文件不可 import，而这几条 SQL 又埋在 ``start()`` 里层层嵌套的闭包里，AST 摘函数
那套用不上。这里换个办法：把**源码里真实的 SQL 字符串**抠出来，建一张真实 schema
的 SQLite 表跑一遍。测的仍然是真正会执行的语句，改坏了这里会红。

守的核心不变量：``end_time - start_time`` 必须是一段真实连续的下载时间。老逻辑
只在 INSERT 时写一次 start_time，于是隔天点重试的记录会显示成跑了 28 小时，
实际只下了 91 分钟。

运行：
    cd telegram-download-deamon
    python -m pytest tests/test_download_record_sql.py -v
"""

import ast
import os
import re
import sqlite3
import sys
import time
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

DAEMON_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "telegram-download-daemon.py")
)


def _source():
    with open(DAEMON_PATH, "r", encoding="utf-8") as handle:
        return handle.read()


def _sql_literals():
    """源码里所有的 SQL 字符串常量。"""
    out = []
    for node in ast.walk(ast.parse(_source(), filename=DAEMON_PATH)):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            text = node.value.strip()
            if re.match(r"^(CREATE TABLE|INSERT INTO|UPDATE|ALTER TABLE)\b", text, re.I):
                out.append(text)
    return out


def find_sql(*must_contain):
    matches = [s for s in _sql_literals()
               if all(frag in s for frag in must_contain)]
    if not matches:
        raise AssertionError("源码里找不到含 %r 的 SQL" % (must_contain,))
    if len(matches) > 1:
        raise AssertionError("含 %r 的 SQL 有 %d 条，选择条件不够精确"
                             % (must_contain, len(matches)))
    return matches[0]


def build_schema(conn):
    """用源码里真实的建表语句 + ALTER 迁移搭出 downloads 表。"""
    conn.execute(find_sql("CREATE TABLE IF NOT EXISTS downloads"))
    for stmt in _sql_literals():
        if stmt.upper().startswith("ALTER TABLE DOWNLOADS ADD COLUMN"):
            try:
                conn.execute(stmt)
            except sqlite3.OperationalError:
                pass  # 建表语句里已经有这列了
    conn.commit()


# worker 把记录从 queued 翻成 downloading 时用的那条 UPDATE
UPDATE_TO_DOWNLOADING = ("UPDATE downloads", "status = 'downloading'", "temp_path = ?")
# persist_queued_download 重投时用的那条
UPDATE_TO_QUEUED = ("UPDATE downloads", "status = 'queued'", "progress = 0.0")


class SchemaTests(unittest.TestCase):
    def test_schema_builds_from_real_source(self):
        conn = sqlite3.connect(":memory:")
        build_schema(conn)
        cols = {r[1] for r in conn.execute("PRAGMA table_info(downloads)")}
        for expected in ("id", "filename", "status", "progress", "retry_count",
                         "temp_path", "start_time", "end_time",
                         "source_channel_id", "source_message_id"):
            self.assertIn(expected, cols)


class StartTimeTests(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        build_schema(self.conn)
        self.sql = find_sql(*UPDATE_TO_DOWNLOADING)

    def _seed(self, start_time="2026-08-04 11:22:10"):
        """造一条"昨天入队"的记录，模拟隔天才被重试的场景。"""
        self.conn.execute(
            "INSERT INTO downloads (filename, file_type, status, size, progress, start_time)"
            " VALUES ('movie.mp4', 'Videos', 'queued', 2762341188, 0.0, ?)",
            (start_time,))
        self.conn.commit()
        return self.conn.execute(
            "SELECT id FROM downloads ORDER BY id DESC LIMIT 1").fetchone()[0]

    def _run_update(self, row_id, progress=0.0, temp_path="/downloads/movie.mp4.tdd"):
        self.conn.execute(self.sql, (
            "movie.mp4", "Videos", 2762341188, progress,
            "/downloads/Videos/movie.mp4", -1001910864969, 45491,
            "https://t.me/c/1910864969/45491", None, temp_path, row_id,
        ))
        self.conn.commit()

    def test_start_time_is_stamped_when_the_attempt_begins(self):
        """这就是要修的那个失真：重试必须重新打点，而不是停在最初入队的时刻。"""
        row_id = self._seed("2026-08-04 11:22:10")
        self._run_update(row_id)
        start = self.conn.execute(
            "SELECT start_time FROM downloads WHERE id = ?", (row_id,)).fetchone()[0]
        self.assertNotEqual(start, "2026-08-04 11:22:10",
                            "start_time 没有重新打点，历史耗时仍然会失真")

    def test_elapsed_span_is_contiguous_not_days_long(self):
        """start_time 与 end_time 之差必须是真实的下载时长。"""
        row_id = self._seed("2026-08-04 11:22:10")
        self._run_update(row_id)
        self.conn.execute(
            "UPDATE downloads SET status='completed', end_time=CURRENT_TIMESTAMP"
            " WHERE id = ?", (row_id,))
        self.conn.commit()
        start, end = self.conn.execute(
            "SELECT start_time, end_time FROM downloads WHERE id = ?",
            (row_id,)).fetchone()
        elapsed = self.conn.execute(
            "SELECT strftime('%s', ?) - strftime('%s', ?)", (end, start)).fetchone()[0]
        self.assertGreaterEqual(elapsed, 0)
        # 老逻辑下这里会是 28 小时量级；重新打点后必须是秒级
        self.assertLess(elapsed, 60, "耗时跨度异常，start_time 疑似没有重新打点")

    def test_end_time_is_cleared_so_a_stale_one_cannot_linger(self):
        row_id = self._seed()
        self.conn.execute(
            "UPDATE downloads SET end_time = '2026-08-05 08:16:00' WHERE id = ?",
            (row_id,))
        self.conn.commit()
        self._run_update(row_id)
        end = self.conn.execute(
            "SELECT end_time FROM downloads WHERE id = ?", (row_id,)).fetchone()[0]
        self.assertIsNone(end, "重试时 end_time 必须清空，否则结束时间早于开始时间")

    def test_retry_count_and_source_survive_the_restamp(self):
        """重新打点不能顺手把重试次数冲掉——页面靠它显示"重试 N"。"""
        row_id = self._seed()
        self.conn.execute(
            "UPDATE downloads SET retry_count = 3 WHERE id = ?", (row_id,))
        self.conn.commit()
        self._run_update(row_id)
        retry, chan, msg = self.conn.execute(
            "SELECT retry_count, source_channel_id, source_message_id"
            " FROM downloads WHERE id = ?", (row_id,)).fetchone()
        self.assertEqual(retry, 3)
        self.assertEqual(chan, -1001910864969)
        self.assertEqual(msg, 45491)

    def test_resume_progress_is_not_reset_to_zero(self):
        """续传时进度从断点起算，不该在页面上倒退回 0。"""
        row_id = self._seed()
        self._run_update(row_id, progress=62.5)
        progress, temp = self.conn.execute(
            "SELECT progress, temp_path FROM downloads WHERE id = ?",
            (row_id,)).fetchone()
        self.assertAlmostEqual(progress, 62.5)
        self.assertEqual(temp, "/downloads/movie.mp4.tdd")

    def test_a_second_attempt_restamps_again(self):
        row_id = self._seed()
        self._run_update(row_id)
        first = self.conn.execute(
            "SELECT start_time FROM downloads WHERE id = ?", (row_id,)).fetchone()[0]
        time.sleep(1.1)  # CURRENT_TIMESTAMP 只到秒
        self._run_update(row_id)
        second = self.conn.execute(
            "SELECT start_time FROM downloads WHERE id = ?", (row_id,)).fetchone()[0]
        self.assertGreater(second, first, "第二次尝试应当再次重新打点")


class RequeueTests(unittest.TestCase):
    """重投（persist_queued_download）那条 UPDATE 不该动 start_time。

    打点的时机只能有一个：worker 真正开始下载的那一刻。放在入队时会把排队等待
    也算进耗时里。
    """

    def test_requeue_leaves_start_time_alone(self):
        conn = sqlite3.connect(":memory:")
        build_schema(conn)
        sql = find_sql(*UPDATE_TO_QUEUED)
        self.assertNotIn("start_time", sql,
                         "重投语句不应改动 start_time——打点归 worker 开下载那一刻管")


if __name__ == "__main__":
    unittest.main()
