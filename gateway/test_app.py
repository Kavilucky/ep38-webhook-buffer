"""Проверка шлюза без живого Redis: вместо Redis подставляется заглушка. Запуск: python test_app.py"""
import json
import sys
import threading
import time
import types
import unittest
import urllib.error
import urllib.request
from http.server import ThreadingHTTPServer


class RedisError(Exception):
    pass


class FakeRedis:
    down = False

    def __init__(self):
        self.items = []

    def lpush(self, key, value):
        if FakeRedis.down:
            raise RedisError("down")
        self.items.insert(0, (key, value))

    def ping(self):
        if FakeRedis.down:
            raise RedisError("down")
        return True


FAKE = FakeRedis()
fake_mod = types.ModuleType("redis")
fake_mod.RedisError = RedisError
fake_mod.Redis = types.SimpleNamespace(from_url=lambda *a, **k: FAKE)
sys.modules["redis"] = fake_mod

import app  # noqa: E402


class GatewayTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.srv = ThreadingHTTPServer(("127.0.0.1", 0), app.Handler)
        cls.base = f"http://127.0.0.1:{cls.srv.server_address[1]}"
        threading.Thread(target=cls.srv.serve_forever, daemon=True).start()

    @classmethod
    def tearDownClass(cls):
        cls.srv.shutdown()

    def setUp(self):
        FAKE.items.clear()
        FakeRedis.down = False
        app.TOKEN = ""

    def post(self, path, data=b"{}", headers=None):
        req = urllib.request.Request(self.base + path, data=data, method="POST", headers=headers or {})
        try:
            with urllib.request.urlopen(req, timeout=3) as r:
                return r.status, r.read()
        except urllib.error.HTTPError as e:
            return e.code, e.read()

    def test_ok_and_queued_with_headers_and_body(self):
        body = json.dumps({"event": "payment.succeeded", "object": {"id": "p1"}}).encode()
        code, out = self.post("/hook/yookassa", body, {"Content-Type": "application/json", "Stripe-Signature": "t=1,v1=x"})
        self.assertEqual((code, out), (200, b"OK"))
        self.assertEqual(len(FAKE.items), 1)
        env = json.loads(FAKE.items[0][1])
        self.assertEqual(env["provider"], "yookassa")
        self.assertEqual(env["headers"]["stripe-signature"], "t=1,v1=x")
        self.assertEqual(env["body"], body.decode())
        self.assertEqual(env["attempts"], 0)

    def test_robokassa_answer_contains_invid(self):
        code, out = self.post("/hook/robokassa", b"OutSum=4900&InvId=5&SignatureValue=abc", {"Content-Type": "application/x-www-form-urlencoded"})
        self.assertEqual((code, out), (200, b"OK5"))

    def test_redis_down_means_503_not_200(self):
        FakeRedis.down = True
        code, _ = self.post("/hook/stripe", b"{}")
        self.assertEqual(code, 503)
        self.assertEqual(FAKE.items, [])

    def test_token_required_when_set(self):
        app.TOKEN = "s3cret"
        self.assertEqual(self.post("/hook/stripe", b"{}")[0], 401)
        self.assertEqual(self.post("/hook/stripe?token=s3cret", b"{}")[0], 200)

    def test_too_large_and_unknown_path(self):
        app.MAX_BODY = 10
        try:
            self.assertEqual(self.post("/hook/stripe", b"x" * 50)[0], 413)
        finally:
            app.MAX_BODY = 1048576
        self.assertEqual(self.post("/other/x", b"{}")[0], 404)

    def test_health(self):
        with urllib.request.urlopen(self.base + "/health", timeout=3) as r:
            self.assertEqual(r.status, 200)
        FakeRedis.down = True
        try:
            urllib.request.urlopen(self.base + "/health", timeout=3)
            self.fail("ожидался 503")
        except urllib.error.HTTPError as e:
            self.assertEqual(e.code, 503)

    def test_local_latency_with_stub(self):
        """Только обработчик с заглушкой Redis, без сети и без реального Redis: к реальной задержке не относится."""
        t0 = time.perf_counter()
        for _ in range(100):
            self.post("/hook/stripe", b"{}")
        avg_ms = (time.perf_counter() - t0) / 100 * 1000
        print(f"\nсреднее на запрос (локально, заглушка Redis): {avg_ms:.2f} мс")


if __name__ == "__main__":
    unittest.main(verbosity=2)
