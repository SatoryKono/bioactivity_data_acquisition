"""Contract and thread-safety tests for :mod:`bioetl.core.api_client`."""

from __future__ import annotations

from typing import Any
from unittest.mock import Mock

import pytest
import requests
from requests.structures import CaseInsensitiveDict

from bioetl.core import api_client
from bioetl.core.api_client import APIConfig, UnifiedAPIClient


def test_fallback_manager_strategies() -> None:
    """Ensure fallback strategies stay in sync with documentation."""

    expected = {"cache", "partial_retry", "network", "timeout", "5xx"}

    config = api_client.APIConfig(name="chembl", base_url="https://chembl/api")

    assert {strategy.lower() for strategy in config.fallback_strategies} == expected

    assert api_client.FALLBACK_MANAGER_SUPPORTED_STRATEGIES == {
        "network",
        "timeout",
        "5xx",
    }

    manager = api_client.FallbackManager(tuple(sorted(expected)))

    assert set(manager.strategies) == expected


class _SpyLock:
    """Lightweight lock stub that records usage counts."""

    def __init__(self) -> None:
        self.enter_count = 0

    def __enter__(self) -> _SpyLock:
        self.enter_count += 1
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: Any,
    ) -> bool:
        return False


def test_cache_thread_safety_guard(monkeypatch: pytest.MonkeyPatch) -> None:
    """UnifiedAPIClient must guard TTLCache operations with a lock."""

    config = APIConfig(
        name="test",
        base_url="https://api.example.com",
        cache_enabled=True,
        cache_ttl=10,
    )
    client = UnifiedAPIClient(config)

    spy_lock = _SpyLock()
    # Replace the default lock with a spy to detect usage.
    client._cache_lock = spy_lock  # type: ignore[assignment]

    response = Mock(spec=requests.Response)
    response.raise_for_status = Mock()
    response.json = Mock(return_value={"value": 1})
    response.headers = CaseInsensitiveDict()

    execute_calls: list[dict[str, Any]] = []

    def fake_execute(**kwargs: Any) -> requests.Response:
        execute_calls.append(kwargs)
        return response

    monkeypatch.setattr(client, "_execute", fake_execute)

    first = client.request_json("endpoint", params={"q": "1"})
    second = client.request_json("endpoint", params={"q": "1"})

    assert first == {"value": 1}
    assert second == {"value": 1}
    # Only the first call should reach the network layer.
    assert len(execute_calls) == 1
    # Lock must wrap cache read (miss), write, and subsequent read (hit).
    assert spy_lock.enter_count >= 3
