from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import ClassVar

import pytest

from bioetl.core.api_client import APIConfig, UnifiedAPIClient

pytestmark = pytest.mark.integration


class _StatefulHandler(BaseHTTPRequestHandler):
    """HTTP handler that serves deterministic responses for integration tests."""

    request_counts: ClassVar[dict[str, int]] = {}

    def log_message(self, _format: str, *args: object) -> None:  # pragma: no cover - quiet server
        return

    def _send_json(
        self,
        status: int,
        payload: dict[str, object],
        *,
        headers: dict[str, str] | None = None,
    ) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        for key, value in (headers or {}).items():
            self.send_header(key, value)
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:  # noqa: N802 - required by BaseHTTPRequestHandler
        count = self.request_counts.get(self.path, 0)
        self.request_counts[self.path] = count + 1

        if self.path == "/status/ok":
            self._send_json(200, {"status": "ok", "count": count + 1})
            return

        if self.path == "/status/not_modified":
            self._send_json(304, {"status": "cached", "count": count + 1})
            return

        if self.path == "/status/rate_limit":
            if count == 0:
                self._send_json(
                    429,
                    {"error": "slow down"},
                    headers={"Retry-After": "0.05"},
                )
                return

            self._send_json(200, {"status": "recovered", "count": count + 1})
            return

        if self.path == "/status/server_error":
            if count == 0:
                self._send_json(500, {"error": "flaky"})
                return

            self._send_json(200, {"status": "steady", "count": count + 1})
            return

        self._send_json(404, {"error": "missing"})


@pytest.fixture(name="live_server")
def _live_server() -> ThreadingHTTPServer:
    """Yield a live HTTP server hosting the stateful handler."""

    _StatefulHandler.request_counts = {}
    server = ThreadingHTTPServer(("127.0.0.1", 0), _StatefulHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server
    finally:
        server.shutdown()
        thread.join()


def test_unified_api_client_end_to_end(monkeypatch: pytest.MonkeyPatch, live_server: ThreadingHTTPServer) -> None:
    """UnifiedAPIClient should honour Retry-After, retry on 5xx and succeed on 2xx/3xx."""

    host, port = live_server.server_address
    base_url = f"http://{host}:{port}"

    config = APIConfig(
        name="integration",
        base_url=base_url,
        cache_enabled=False,
        rate_limit_max_calls=100,
        rate_limit_period=1.0,
        rate_limit_jitter=False,
        retry_total=3,
        retry_backoff_factor=0.0,
        timeout_connect=1.0,
        timeout_read=1.0,
        fallback_enabled=False,
    )

    client = UnifiedAPIClient(config)

    sleep_calls: list[float] = []
    monkeypatch.setattr("bioetl.core.api_client.time.sleep", lambda seconds: sleep_calls.append(seconds))

    try:
        payload_ok = client.request_json("/status/ok")
        payload_cached = client.request_json("/status/not_modified")
        payload_rate_limit = client.request_json("/status/rate_limit")
        payload_server_error = client.request_json("/status/server_error")
    finally:
        client.close()

    assert payload_ok["status"] == "ok"
    assert payload_cached == {}
    assert _StatefulHandler.request_counts["/status/not_modified"] == 1
    assert payload_rate_limit["status"] == "recovered"
    assert payload_server_error["status"] == "steady"

    assert len(sleep_calls) == 2
    assert sleep_calls == pytest.approx([0.05, 0.0], rel=0.0, abs=0.02)
