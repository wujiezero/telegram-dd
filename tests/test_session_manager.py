"""sessionManager 的单元测试。

运行：
    cd telegram-download-deamon
    python -m unittest tests/test_session_manager.py -v
"""

import importlib
import os
import sys
import tempfile
import unittest
from unittest import mock

# 允许从仓库根目录直接 `python -m unittest tests/test_session_manager.py`
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


class SessionManagerLockPathTests(unittest.TestCase):
    def tearDown(self):
        for key in ("TELEGRAM_DAEMON_SESSION_PATH", "TELEGRAM_DAEMON_LOCK_FILE"):
            os.environ.pop(key, None)

    def _reload_module(self):
        if "sessionManager" in sys.modules:
            return importlib.reload(sys.modules["sessionManager"])
        import sessionManager  # noqa: F401
        return sys.modules["sessionManager"]

    def test_explicit_lock_file_takes_precedence(self):
        os.environ["TELEGRAM_DAEMON_SESSION_PATH"] = "/tmp/session-dir"
        os.environ["TELEGRAM_DAEMON_LOCK_FILE"] = "/tmp/custom/daemon.lock"

        session_manager = self._reload_module()

        self.assertEqual(session_manager.getLockPath(), "/tmp/custom/daemon.lock")

    def test_lock_defaults_to_session_directory_when_writable(self):
        with tempfile.TemporaryDirectory() as session_dir:
            os.environ["TELEGRAM_DAEMON_SESSION_PATH"] = session_dir

            session_manager = self._reload_module()

            self.assertEqual(
                session_manager.getLockPath(),
                os.path.join(session_dir, "DownloadDaemon.lock"),
            )

    def test_lock_falls_back_to_tmp_when_session_directory_is_not_writable(self):
        os.environ["TELEGRAM_DAEMON_SESSION_PATH"] = "/session"

        session_manager = self._reload_module()

        with mock.patch.object(
            session_manager,
            "_isDirectoryWritable",
            return_value=(False, PermissionError("permission denied")),
        ):
            self.assertEqual(
                session_manager.getLockPath(),
                "/tmp/DownloadDaemon.lock",
            )


if __name__ == "__main__":
    unittest.main()
