"""Unit tests for :mod:`bioetl.core.fallback_manager`."""

from __future__ import annotations

import pytest
import requests

from bioetl.core.fallback_manager import FallbackManager


def _http_error(status_code: int) -> requests.exceptions.HTTPError:
    response = requests.Response()
    response.status_code = status_code
    error = requests.exceptions.HTTPError(response=response)
    return error


def test_execute_with_fallback_success() -> None:
    manager = FallbackManager(strategies=("network", "timeout", "5xx"))

    def _operation() -> str:
        return "ok"

    assert manager.execute_with_fallback(_operation) == "ok"


@pytest.mark.parametrize(
    ("exception", "strategies", "expected"),
    [
        (requests.exceptions.ConnectionError("boom"), ("network",), True),
        (requests.exceptions.Timeout("slow"), ("timeout",), True),
        (_http_error(503), ("5xx",), True),
    ],
)
def test_should_fallback_on_supported_errors(
    exception: Exception, strategies: tuple[str, ...], expected: bool
) -> None:
    manager = FallbackManager(strategies=strategies)

    assert manager.should_fallback(exception) is expected


def test_execute_with_fallback_returns_fallback_payload() -> None:
    manager = FallbackManager(strategies=("network",))
    manager.fallback_data = {"status": "fallback"}

    def _operation() -> str:
        raise requests.exceptions.ConnectionError("offline")

    payload = manager.execute_with_fallback(_operation)
    assert payload == {"status": "fallback"}


def test_execute_with_custom_fallback_provider() -> None:
    manager = FallbackManager(strategies=("timeout",))

    def _operation() -> str:
        raise requests.exceptions.Timeout("slow")

    def _fallback() -> dict[str, str]:
        return {"status": "custom"}

    payload = manager.execute_with_fallback(_operation, fallback_data=_fallback)
    assert payload == {"status": "custom"}


def test_execute_with_fallback_raises_when_strategy_disabled() -> None:
    manager = FallbackManager(strategies=("network",))

    def _operation() -> str:
        raise _http_error(503)

    with pytest.raises(requests.exceptions.HTTPError):
        manager.execute_with_fallback(_operation)
