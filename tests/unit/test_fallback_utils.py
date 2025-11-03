"""Unit tests for :mod:`bioetl.utils.fallback`."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from email.utils import format_datetime

import pytest

from bioetl.core import api_client
from bioetl.utils import fallback


class _DummyResponse:
    def __init__(self, headers: dict[str, str | int | float]) -> None:
        self.headers = headers


class _DummyHTTPError:
    def __init__(self, *, retry_after: object | None = None, headers: dict[str, str] | None = None) -> None:
        self.retry_after = retry_after
        self.response = _DummyResponse(headers or {})


def test_extract_retry_after_numeric_attribute() -> None:
    """Numeric ``retry_after`` attribute is parsed into float seconds."""

    error = _DummyHTTPError(retry_after=42)

    assert fallback._extract_retry_after(error) == pytest.approx(42.0)


def test_extract_retry_after_http_date_header(monkeypatch: pytest.MonkeyPatch) -> None:
    """HTTP-date ``Retry-After`` header values are converted into seconds."""

    base_time = datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc)
    future_time = base_time + timedelta(seconds=120)
    monkeypatch.setattr(api_client, "_current_utc_time", lambda: base_time)

    error = _DummyHTTPError(headers={"Retry-After": format_datetime(future_time)})

    assert fallback._extract_retry_after(error) == pytest.approx(120.0, rel=0.0, abs=1.0)
