"""Tests for UnifiedAPIClient and APIConfig."""

from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from typing import Any, Iterator

import pytest
import responses

from bioetl.core.http import (
    APIConfig,
    UnifiedAPIClient,
)


def api_config(**overrides: Any) -> APIConfig:
    """Create a default APIConfig with optional overrides."""
    cfg = APIConfig(
        base_url="http://example.com/",
        timeout_sec=0.2,
        max_retries=2,
        backoff_factor=0.1,
        max_backoff_sec=5,
        rate_limit_calls=10,
        rate_limit_period_sec=1,
        cache_enabled=True,
        cache_ttl_sec=5,
        circuit_breaker_fail_max=3,
        circuit_breaker_reset_sec=0.2,
        default_headers={},
        user_agent="pytest",
    )
    for key, value in overrides.items():
        setattr(cfg, key, value)
    return cfg


def test_unified_client_respects_retry_after(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    Test that the client sleeps for the duration specified in
    Retry-After header.
    """
    sleep_calls: list[float] = []
    monkeypatch.setattr(time, "sleep", sleep_calls.append)

    client = UnifiedAPIClient.from_config(api_config())
    retry_at = (
        datetime.now(timezone.utc) + timedelta(seconds=2)
    ).strftime("%a, %d %b %Y %H:%M:%S GMT")

    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.GET,
            "http://example.com/resource",
            status=429,
            headers={"Retry-After": retry_at},
            json={"message": "slow down"},
        )
        rsps.add(
            responses.GET,
            "http://example.com/resource",
            status=200,
            json={"ok": True},
        )

        payload = client.get_json("/resource")

    assert payload == {"ok": True}
    # допускаем небольшой дрейф parse_retry_after для коротких интервалов
    assert sleep_calls and sleep_calls[0] >= 0.1


def test_unified_client_cache_and_pagination() -> None:
    """Test pagination iterator and caching behavior."""
    client = UnifiedAPIClient.from_config(api_config())
    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.GET,
            "http://example.com/items",
            json={"items": [{"id": 1}, {"id": 2}]},
        )
        rsps.add(
            responses.GET,
            "http://example.com/items?page=2",
            json={"items": []},
        )

        pages: Iterator[dict[str, Any]] = client.paginate_json("/items")
        first = next(pages)
        second = next(pages)
        with pytest.raises(StopIteration):
            next(pages)

        # cache hit: повторный вызов не дергает сеть
        payload_cached = client.get_json("/items", params={"page": 1})

    assert first == {"id": 1}
    assert second == {"id": 2}
    assert payload_cached == {"items": [{"id": 1}, {"id": 2}]}
