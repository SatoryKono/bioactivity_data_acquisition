from __future__ import annotations

from typing import Any

import pytest
import requests

from bioetl.core import api_client as api_client_module
from bioetl.core.api_client import APIConfig, PartialFailure, UnifiedAPIClient


class _DummyResponse:
    """Minimal ``requests.Response`` stand-in for tests."""

    def __init__(self, payload: dict[str, Any], status_code: int = 200) -> None:
        self._payload = payload
        self.status_code = status_code
        self.headers: dict[str, Any] = {}
        self.text = ""

    def raise_for_status(self) -> None:
        if 400 <= self.status_code:
            exc = requests.exceptions.HTTPError("error")
            exc.response = self
            raise exc

    def json(self) -> dict[str, Any]:
        return self._payload


class _ProbeCache(dict[str, Any]):
    """Cache wrapper that hides entries from ``__contains__`` checks."""

    def __contains__(self, key: object) -> bool:
        return False


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(api_client_module.time, "sleep", lambda _: None)


def _base_config(**overrides: Any) -> APIConfig:
    config = APIConfig(
        name="test",
        base_url="https://example.org",
        headers={},
        retry_total=2,
        retry_backoff_factor=0.0,
        rate_limit_max_calls=10,
        rate_limit_period=1.0,
        fallback_enabled=True,
        fallback_strategies=["cache"],
    )
    for key, value in overrides.items():
        setattr(config, key, value)
    return config


def test_request_json_uses_cache_fallback(monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture) -> None:
    config = _base_config()
    client = UnifiedAPIClient(config)

    cache_key = client._cache_key("https://example.org/resource", None)
    fallback_payload = {"from_cache": True}
    probe_cache = _ProbeCache({cache_key: fallback_payload})
    client.cache = probe_cache

    def _failing_execute(**_: Any) -> requests.Response:
        response = _DummyResponse({}, status_code=500)
        exc = requests.exceptions.HTTPError("boom", response=response)
        raise exc

    monkeypatch.setattr(client, "_execute", _failing_execute)

    caplog.set_level("DEBUG")
    payload = client.request_json("/resource")

    assert payload == fallback_payload
    assert any(
        "fallback_strategy_applied" in record.message for record in caplog.records
    )


def test_request_json_partial_retry_strategy(monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture) -> None:
    config = _base_config(
        retry_total=1,
        fallback_strategies=["partial", "cache"],
        partial_retry_max=2,
    )
    client = UnifiedAPIClient(config)

    calls = {"count": 0}

    def _partial_execute(**_: Any) -> requests.Response:
        calls["count"] += 1
        if calls["count"] <= 2:
            exc = PartialFailure()
            exc.received = 1  # type: ignore[attr-defined]
            exc.expected = 2  # type: ignore[attr-defined]
            exc.page_state = "token"  # type: ignore[attr-defined]
            raise exc
        return _DummyResponse({"ok": True})

    monkeypatch.setattr(client, "_execute", _partial_execute)

    caplog.set_level("DEBUG")
    payload = client.request_json("/resource")

    assert payload == {"ok": True}
    assert calls["count"] == 3
    assert any(
        "fallback_partial_retry_success" in record.message
        for record in caplog.records
    )
