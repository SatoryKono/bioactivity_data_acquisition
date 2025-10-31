"""Tests for fallback integration in :mod:`bioetl.core.api_client`."""

from __future__ import annotations

from typing import Callable

import pytest
import requests

from bioetl.core.api_client import APIConfig, UnifiedAPIClient


def _build_client(
    *,
    fallback_strategies: list[str] | None = None,
) -> UnifiedAPIClient:
    config = APIConfig(
        name="test",
        base_url="https://example.org",
        headers={},
        cache_enabled=False,
        rate_limit_max_calls=1,
        rate_limit_period=1.0,
        rate_limit_jitter=False,
        retry_total=0,
        retry_backoff_factor=1.0,
        retry_backoff_max=None,
        retry_status_codes=[],
        partial_retry_max=0,
        timeout_connect=1.0,
        timeout_read=1.0,
        cb_failure_threshold=5,
        cb_timeout=60.0,
        fallback_enabled=True,
        fallback_strategies=fallback_strategies
        if fallback_strategies is not None
        else ["CACHE", "NETWORK", "TIMEOUT", "5XX"],
    )

    client = UnifiedAPIClient(config)
    client.rate_limiter.acquire = lambda: None  # type: ignore[method-assign]
    client.retry_policy.get_wait_time = (  # type: ignore[assignment]
        lambda *_args, **_kwargs: 0.0
    )
    return client


@pytest.mark.parametrize(
    ("exception_factory", "expected_reason", "expected_error_type"),
    [
        (
            lambda: requests.exceptions.ConnectionError("offline"),
            "network",
            "ConnectionError",
        ),
        (
            lambda: requests.exceptions.Timeout("slow"),
            "timeout",
            "Timeout",
        ),
    ],
)
def test_fallback_manager_handles_network_and_timeout(
    monkeypatch: pytest.MonkeyPatch,
    exception_factory: Callable[[], requests.exceptions.RequestException],
    expected_reason: str,
    expected_error_type: str,
) -> None:
    """FallbackManager should supply deterministic payloads for network/timeout."""

    client = _build_client()

    def _raise(*_args: object, **_kwargs: object) -> requests.Response:
        raise exception_factory()

    monkeypatch.setattr(client.session, "request", _raise)

    payload = client.request_json("/resource")

    assert payload["fallback_reason"] == expected_reason
    assert payload["fallback_error_type"] == expected_error_type


def _error_response(status_code: int, body: str = "error") -> requests.Response:
    response = requests.Response()
    response.status_code = status_code
    response._content = body.encode()
    response.url = "https://example.org/resource"
    response.headers["Retry-After"] = "5"
    return response


def test_fallback_manager_handles_5xx_http_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """HTTP 5xx responses should trigger fallback payloads via the manager."""

    client = _build_client()

    response = _error_response(503)
    monkeypatch.setattr(client.session, "request", lambda *_a, **_k: response)

    payload = client.request_json("/resource")

    assert payload["fallback_reason"] == "5xx"
    assert payload["fallback_http_status"] == 503
    assert payload["fallback_retry_after_sec"] == pytest.approx(5.0)
