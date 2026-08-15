"""代理配置规整/构造的单元测试（页面上改代理这条链路的地基）。

页面能改代理之后，配置有三个来源（.env、数据库里保存的、页面表单），
必须收敛到同一套口径，否则"页面显示的"和"实际生效的"会对不上。
另外两条硬性要求在这里守住：

1. 半截配置（缺主机/端口非法）一律降级成直连，绝不塞给 Telethon；
2. 给页面的数据**永远不含密码明文**。

主文件不可 import，用 AST 摘函数（见 test_link_extraction.py 的说明）。
"""

import ast
import os
import unittest

import socks  # noqa: E402

DAEMON_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "telegram-download-daemon.py")
)
WANTED = (
    "normalize_proxy_config",
    "describe_proxy_config",
    "build_proxy_tuple",
    "_proxy_config_for_client",
)


class _StubLogger:
    def warning(self, *a, **k):
        pass

    def info(self, *a, **k):
        pass


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
        "socks": socks,
        "socket": __import__("socket"),
        "logger": _StubLogger(),
        "PROXY_TYPES": ("socks5", "http", "mtproxy"),
        "_PROXY_TYPE_CONSTS": {"socks5": socks.SOCKS5, "http": socks.HTTP},
        # resolve_once 的分支单独测，这里固定成恒等映射便于断言
        "resolve_proxy_host_once": lambda host, port: f"resolved({host})",
    }
    module = ast.Module(body=picked, type_ignores=[])
    exec(compile(module, DAEMON_PATH, "exec"), namespace)  # noqa: S102
    return namespace


DAEMON = _load_functions()
normalize_proxy_config = DAEMON["normalize_proxy_config"]
describe_proxy_config = DAEMON["describe_proxy_config"]
build_proxy_tuple = DAEMON["build_proxy_tuple"]
proxy_config_for_client = DAEMON["_proxy_config_for_client"]


class NormalizeTest(unittest.TestCase):

    def test_full_config_round_trips(self):
        cfg = normalize_proxy_config({
            "enabled": True, "type": "SOCKS5", "host": " 192.168.5.31 ",
            "port": "1080", "username": " user ", "password": "pw",
            "resolve_once": 1,
        })
        self.assertEqual(cfg["enabled"], True)
        self.assertEqual(cfg["type"], "socks5")      # 大小写归一
        self.assertEqual(cfg["host"], "192.168.5.31")  # 去空白
        self.assertEqual(cfg["port"], 1080)          # 字符串转 int
        self.assertEqual(cfg["username"], "user")
        self.assertEqual(cfg["resolve_once"], True)

    def test_unknown_type_falls_back_to_socks5(self):
        self.assertEqual(
            normalize_proxy_config({"host": "h", "port": 1, "type": "wireguard"})["type"],
            "socks5")

    def test_missing_host_degrades_to_direct(self):
        cfg = normalize_proxy_config({"enabled": True, "host": "", "port": 1080})
        self.assertFalse(cfg["enabled"])

    def test_invalid_port_degrades_to_direct(self):
        for bad in ("0", "70000", "-1", "abc", "", None):
            cfg = normalize_proxy_config({"enabled": True, "host": "h", "port": bad})
            self.assertFalse(cfg["enabled"], f"端口 {bad!r} 应当降级成直连")

    def test_enabled_defaults_from_host_and_port(self):
        self.assertTrue(normalize_proxy_config({"host": "h", "port": 1080})["enabled"])
        self.assertFalse(normalize_proxy_config({})["enabled"])

    def test_explicitly_disabled_stays_disabled(self):
        cfg = normalize_proxy_config({"enabled": False, "host": "h", "port": 1080})
        self.assertFalse(cfg["enabled"])


class BuildTupleTest(unittest.TestCase):

    def test_direct_connection_is_none(self):
        self.assertIsNone(build_proxy_tuple({"enabled": False}))

    def test_without_auth(self):
        self.assertEqual(
            build_proxy_tuple({"host": "1.2.3.4", "port": 1080, "type": "socks5"}),
            (socks.SOCKS5, "1.2.3.4", 1080, False))

    def test_with_auth(self):
        self.assertEqual(
            build_proxy_tuple({"host": "1.2.3.4", "port": 3128, "type": "http",
                               "username": "u", "password": "p"}),
            (socks.HTTP, "1.2.3.4", 3128, False, "u", "p"))

    def test_username_without_password_is_treated_as_no_auth(self):
        # PySocks 的 4/6 元组两种形态，半截认证信息只会让连接以奇怪的方式失败
        self.assertEqual(
            build_proxy_tuple({"host": "h", "port": 1080, "username": "u"}),
            (socks.SOCKS5, "h", 1080, False))

    def test_resolve_once_replaces_the_host(self):
        built = build_proxy_tuple({"host": "proxy.example.com", "port": 1080,
                                   "resolve_once": True})
        self.assertEqual(built[1], "resolved(proxy.example.com)")

    def test_without_resolve_once_host_is_kept(self):
        built = build_proxy_tuple({"host": "proxy.example.com", "port": 1080,
                                   "resolve_once": False})
        self.assertEqual(built[1], "proxy.example.com")


class DescribeAndClientPayloadTest(unittest.TestCase):

    def test_describe_never_leaks_the_password(self):
        text = describe_proxy_config({"enabled": True, "type": "socks5", "host": "h",
                                      "port": 1080, "username": "u",
                                      "password": "sup3rs3cret"})
        self.assertNotIn("sup3rs3cret", text)
        self.assertIn("******", text)

    def test_describe_direct(self):
        self.assertIn("直连", describe_proxy_config({"enabled": False}))

    def test_client_payload_never_contains_the_password(self):
        payload = proxy_config_for_client({
            "enabled": True, "type": "socks5", "host": "h", "port": 1080,
            "username": "u", "password": "sup3rs3cret",
        })
        self.assertNotIn("password", payload)
        self.assertTrue(payload["has_password"])
        self.assertNotIn("sup3rs3cret", repr(payload))

    def test_client_payload_reports_missing_password(self):
        payload = proxy_config_for_client({"host": "h", "port": 1080})
        self.assertFalse(payload["has_password"])


if __name__ == "__main__":
    unittest.main()
