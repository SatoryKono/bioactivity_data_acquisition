from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Dict, List, Tuple

import pytest
from requests.exceptions import ConnectionError, ReadTimeout

from bioetl.core.api import ApiConnectionLimits, ApiProfile, ApiRetryPolicy, ApiTimeouts, get_api_client


@dataclass
class ResponseSpec:
    method: str | None
    status: int
    body: bytes = b""
    headers: Dict[str, str] = field(default_factory=dict)
    delay: float = 0.0


class _ThreadingHTTPServer(HTTPServer):
    allow_reuse_address = True

    def __init__(self) -> None:
        self._responses: List[ResponseSpec] = []
        self.requests: List[Tuple[str, str, Dict[str, str]]] = []
        self._lock = threading.Lock()
        super().__init__(("127.0.0.1", 0), _RequestHandler)

    @property
    def base_url(self) -> str:
        host, port = self.server_address
        return f"http://{host}:{port}"

    def queue(self, spec: ResponseSpec) -> None:
        with self._lock:
            self._responses.append(spec)

    def next_response(self, method: str) -> ResponseSpec:
        with self._lock:
            for index, spec in enumerate(self._responses):
                if spec.method is None or spec.method == method:
                    return self._responses.pop(index)
        raise AssertionError(f"No response configured for method {method}")

    def record_request(self, method: str, path: str, headers: Dict[str, str]) -> None:
        with self._lock:
            self.requests.append((method, path, headers))


class _RequestHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *_args, **_kwargs):  # pragma: no cover - silence default logging
        return

    def do_GET(self):  # noqa: N802 - defined by BaseHTTPRequestHandler
        self._handle()

    def do_POST(self):  # noqa: N802 - defined by BaseHTTPRequestHandler
        self._handle()

    def _handle(self) -> None:
        server = self.server  # type: ignore[attr-defined]
        assert isinstance(server, _ThreadingHTTPServer)
        spec = server.next_response(self.command)
        headers = {k: v for k, v in self.headers.items()}
        server.record_request(self.command, self.path, headers)
        if spec.delay:
            time.sleep(spec.delay)
        body = spec.body or b""
        self.send_response(spec.status)
        for key, value in spec.headers.items():
            self.send_header(key, value)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if self.command != "HEAD" and body:
            try:
                self.wfile.write(body)
            except BrokenPipeError:  # pragma: no cover - client closed connection
                pass


@pytest.fixture
def api_test_server() -> _ThreadingHTTPServer:
    server = _ThreadingHTTPServer()
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server
    finally:
        server.shutdown()
        thread.join()


def _profile(name: str = "test", **kwargs) -> ApiProfile:
    defaults = dict(
        name=name,
        timeouts=kwargs.get(
            "timeouts",
            ApiTimeouts(connect=0.5, read=0.5, write=0.5, pool=0.5, total=5.0),
        ),
        limits=kwargs.get(
            "limits",
            ApiConnectionLimits(max_connections=4, max_keepalive=4, pool_block=True),
        ),
        retries=kwargs.get("retries", ApiRetryPolicy()),
        headers=kwargs.get("headers", {}),
    )
    return ApiProfile(**defaults)


@pytest.mark.integration
def test_api_client_retries_on_server_errors(api_test_server: _ThreadingHTTPServer) -> None:
    api_test_server.queue(ResponseSpec(method="GET", status=500))
    api_test_server.queue(ResponseSpec(method="GET", status=502))
    api_test_server.queue(ResponseSpec(method="GET", status=200, body=b"ok"))

    profile = _profile(
        retries=ApiRetryPolicy(total=2, backoff_factor=0.1, backoff_max=0.1),
    )
    client = get_api_client(profile)
    try:
        response = client.get(f"{api_test_server.base_url}/unstable")
        assert response.status_code == 200
    finally:
        client.close()

    assert len(api_test_server.requests) == 3


@pytest.mark.integration
def test_api_client_does_not_retry_unsafe_methods(api_test_server: _ThreadingHTTPServer) -> None:
    api_test_server.queue(ResponseSpec(method="POST", status=500, body=b"error"))

    profile = _profile(
        retries=ApiRetryPolicy(total=3, methods=frozenset({"GET", "HEAD"})),
    )
    client = get_api_client(profile)
    try:
        response = client.post(f"{api_test_server.base_url}/no-retry", json={})
        assert response.status_code == 500
    finally:
        client.close()

    assert len(api_test_server.requests) == 1


@pytest.mark.integration
def test_api_client_respects_retry_after_header(api_test_server: _ThreadingHTTPServer) -> None:
    api_test_server.queue(
        ResponseSpec(method="GET", status=429, headers={"Retry-After": "1"})
    )
    api_test_server.queue(ResponseSpec(method="GET", status=200, body=b"ok"))

    profile = _profile(
        retries=ApiRetryPolicy(total=2, backoff_factor=0.0, backoff_max=2.0),
    )
    client = get_api_client(profile)
    try:
        start = time.perf_counter()
        response = client.get(f"{api_test_server.base_url}/retry-after")
        elapsed = time.perf_counter() - start
        assert response.status_code == 200
    finally:
        client.close()

    assert elapsed >= 1.0
    assert len(api_test_server.requests) >= 1


@pytest.mark.integration
def test_api_client_retries_on_read_timeout(api_test_server: _ThreadingHTTPServer) -> None:
    api_test_server.queue(ResponseSpec(method="GET", status=200, delay=0.3, body=b"slow"))
    api_test_server.queue(ResponseSpec(method="GET", status=200, delay=0.3, body=b"slow"))

    timeouts = ApiTimeouts(connect=0.1, read=0.1, write=0.1, pool=0.1, total=1.0)
    profile = _profile(
        retries=ApiRetryPolicy(total=1, retry_on_read_errors=True),
        timeouts=timeouts,
    )
    client = get_api_client(profile)
    try:
        with pytest.raises(ConnectionError):
            client.get(f"{api_test_server.base_url}/timeout")
    finally:
        client.close()

    assert len(api_test_server.requests) >= 1


@pytest.mark.integration
def test_api_client_sets_default_user_agent(api_test_server: _ThreadingHTTPServer) -> None:
    api_test_server.queue(ResponseSpec(method="GET", status=200, body=b"ok"))

    profile = _profile()
    client = get_api_client(profile)
    try:
        response = client.get(f"{api_test_server.base_url}/headers")
        assert response.status_code == 200
    finally:
        client.close()

    assert api_test_server.requests[0][2]["User-Agent"].startswith("bioetl/")
