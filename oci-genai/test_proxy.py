"""Regression tests for proxy request authorization (stdlib unittest, stub OCI client)."""
import json
import sys
import threading
import types
import unittest
import urllib.error
import urllib.request

stub = types.ModuleType("oci_client")
stub.create_oci_client = lambda *a, **k: None
sys.modules.setdefault("oci_client", stub)

import proxy  # noqa: E402


class _Resp:
    def model_dump(self):
        return {"ok": True}


class _Client:
    calls = 0

    class chat:
        class completions:
            @staticmethod
            def create(**kw):
                _Client.calls += 1
                return _Resp()


class ProxyAuthTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        proxy.OCIProxyHandler.client = _Client
        cls.srv = proxy.ThreadedHTTPServer(("127.0.0.1", 0), proxy.OCIProxyHandler)
        cls.port = cls.srv.server_address[1]
        threading.Thread(target=cls.srv.serve_forever, daemon=True).start()

    @classmethod
    def tearDownClass(cls):
        cls.srv.shutdown()

    def setUp(self):
        _Client.calls = 0
        proxy.PROXY_TOKEN = ""
        proxy.PROXY_ALLOW_ORIGIN = ""

    def post(self, headers=None):
        req = urllib.request.Request(
            f"http://127.0.0.1:{self.port}/v1/chat/completions",
            data=json.dumps({"model": "m", "messages": []}).encode(),
            headers=headers or {},
            method="POST",
        )
        try:
            return urllib.request.urlopen(req).status
        except urllib.error.HTTPError as e:
            return e.code

    def test_foreign_origin_rejected(self):
        self.assertEqual(self.post({"Origin": "https://evil.example", "Content-Type": "text/plain"}), 403)
        self.assertEqual(_Client.calls, 0)

    def test_allowed_origin_accepted(self):
        proxy.PROXY_ALLOW_ORIGIN = "http://ui.local"
        self.assertEqual(self.post({"Origin": "http://ui.local"}), 200)

    def test_no_origin_accepted_without_token(self):
        self.assertEqual(self.post(), 200)

    def test_token_required_when_set(self):
        proxy.PROXY_TOKEN = "s3cret"
        self.assertEqual(self.post(), 403)
        self.assertEqual(self.post({"Authorization": "Bearer wrong"}), 403)
        self.assertEqual(_Client.calls, 0)
        self.assertEqual(self.post({"Authorization": "Bearer s3cret"}), 200)


if __name__ == "__main__":
    unittest.main()
