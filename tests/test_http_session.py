import io
import time
from unittest import mock

import requests
import requests_cache

from library.clients.base import BaseHttpClient
from library.utils import logging as logging_utils
from library.utils import rate_limit


def _build_response(status: int, *, retry_after: str | None = None) -> requests.Response:
    response = requests.Response()
    response.status_code = status
    response._content = b"{}"
    response._content_consumed = True
    response.raw = io.BytesIO(b"")
    response.url = "https://example.test/resource"
    if retry_after is not None:
        response.headers["Retry-After"] = retry_after
    return response


def test_session_retries_on_server_errors():
    attempts: list[int] = []

    def fake_request(self, method, url, **kwargs):  # pragma: no cover - used in test
        attempts.append(1)
        if len(attempts) < 3:
            return _build_response(500)
        return _build_response(200)

    with mock.patch.object(requests_cache.CachedSession, "request", new=fake_request):
        session = logging_utils.create_shared_session(cache_name=".test_cache_retry")
        response = session.get("https://example.test/resource")

    assert response.status_code == 200
    assert len(attempts) == 3


def test_retry_after_header_is_respected():
    call_times: list[float] = []

    def fake_request(self, method, url, **kwargs):  # pragma: no cover - used in test
        call_times.append(time.monotonic())
        if len(call_times) == 1:
            return _build_response(429, retry_after="1")
        return _build_response(200)

    with mock.patch.object(requests_cache.CachedSession, "request", new=fake_request):
        session = logging_utils.create_shared_session(cache_name=".test_cache_retry_after")
        response = session.get("https://example.test/resource")

    assert response.status_code == 200
    assert len(call_times) == 2
    assert call_times[1] - call_times[0] >= 1.0


def test_client_uses_injected_session_and_rate_limiter():
    class DummyClient(BaseHttpClient):
        client_name = "dummy"

    session_mock = mock.Mock(spec=requests.Session)
    response = _build_response(200)
    session_mock.request.return_value = response

    limiter = mock.Mock(spec=rate_limit.RateLimiterSet)

    client = DummyClient("https://example.test", session=session_mock, rate_limiter=limiter)
    result = client.get("/resource", params={"q": 1})

    assert result is response
    limiter.acquire.assert_called_once()
    session_mock.request.assert_called_once()
    args, kwargs = session_mock.request.call_args
    assert args[0] == "GET"
    assert args[1] == "https://example.test/resource"
    assert kwargs["params"] == {"q": 1}
