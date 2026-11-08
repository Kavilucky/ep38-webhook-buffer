"""Шлюз платёжных вебхуков (эпизод 38, webhook_buffer): принять, положить в очередь Redis, ответить сразу.

Принцип: подтверждение платёжному сервису (200) уходит только после успешной записи в Redis.
Если Redis недоступен, шлюз отвечает 503, и платёжный сервис повторит доставку позже (иначе платёж был бы «принят» и потерян).
Тело запроса сохраняется целиком (текст и base64 исходных байт) вместе с заголовками: подпись можно проверить позже.
Только стандартная библиотека Python и пакет redis. Запуск: python app.py (порт 8080).

Маршруты:
  POST /hook/<провайдер>   любой провайдер (yookassa, stripe, robokassa, ...); для robokassa ответ OK<InvId>
  GET  /health             200, если Redis отвечает
Переменные окружения: REDIS_URL, QUEUE_KEY, PORT, MAX_BODY_BYTES, WEBHOOK_TOKEN (если задан, нужен параметр ?token=...).
"""
import base64
import json
import os
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

import redis

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
QUEUE_KEY = os.getenv("QUEUE_KEY", "webhooks:in")
PORT = int(os.getenv("PORT", "8080"))
MAX_BODY = int(os.getenv("MAX_BODY_BYTES", "1048576"))
TOKEN = os.getenv("WEBHOOK_TOKEN", "")

R = redis.Redis.from_url(REDIS_URL, socket_timeout=1.0, socket_connect_timeout=1.0)


def build_envelope(provider, headers, body, query):
    """Конверт для очереди: всё, что нужно воркеру, чтобы разобрать и проверить вебхук позже."""
    return {
        "id": uuid.uuid4().hex,
        "provider": provider,
        "received_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "attempts": 0,
        "headers": {k.lower(): v for k, v in headers.items()},
        "query": query,
        "body": body.decode("utf-8", errors="replace"),
        "body_b64": base64.b64encode(body).decode("ascii"),
    }


def ok_body(provider, body, query):
    """Тело ответа: для Robokassa обязателен текст OK<InvId>, остальным достаточно кода 200."""
    if provider == "robokassa":
        form = parse_qs(body.decode("utf-8", errors="replace"))
        inv = (form.get("InvId") or parse_qs(query).get("InvId") or [""])[0]
        return ("OK" + inv).encode("ascii", errors="ignore")
    return b"OK"


class Handler(BaseHTTPRequestHandler):
    server_version = "webhook-buffer/1.0"

    def log_message(self, fmt, *args):  # тела и токены в логи не пишем
        pass

    def _send(self, code, body=b""):
        self.send_response(code)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _drain(self):
        """Прочитать (до 64 КБ) и отбросить тело перед ранним ответом об ошибке: иначе клиент получает сброс соединения."""
        try:
            n = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            return
        if n > 0:
            self.rfile.read(min(n, 65536))
        self.close_connection = True

    def do_GET(self):
        if urlparse(self.path).path != "/health":
            return self._send(404, b"not found")
        try:
            R.ping()
            self._send(200, b"ok")
        except redis.RedisError:
            self._send(503, b"redis unavailable")

    def do_POST(self):
        u = urlparse(self.path)
        parts = [p for p in u.path.split("/") if p]
        if len(parts) != 2 or parts[0] != "hook":
            self._drain()
            return self._send(404, b"not found")
        if TOKEN and parse_qs(u.query).get("token", [""])[0] != TOKEN:
            self._drain()
            return self._send(401, b"unauthorized")
        try:
            n = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            return self._send(400, b"bad length")
        if n > MAX_BODY:
            self._drain()
            return self._send(413, b"too large")
        body = self.rfile.read(n) if n else b""
        env = build_envelope(parts[1], dict(self.headers.items()), body, u.query)
        try:
            R.lpush(QUEUE_KEY, json.dumps(env, ensure_ascii=False))
        except redis.RedisError:
            return self._send(503, b"queue unavailable")  # не подтверждаем: провайдер повторит
        self._send(200, ok_body(parts[1], body, u.query))


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
