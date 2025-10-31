"""Property-based tests for API client parsing helpers."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from email.utils import format_datetime, parsedate_to_datetime
from unittest.mock import patch

import pytest
from hypothesis import given, strategies as st

from bioetl.core import api_client


@given(st.floats(allow_nan=False, allow_infinity=False, width=32, min_value=-1e6, max_value=1e6))
def test_parse_retry_after_numeric_float_returns_non_negative(seconds: float) -> None:
    """Numeric Retry-After values should be coerced to non-negative seconds."""

    result = api_client.parse_retry_after(seconds)
    assert result == pytest.approx(max(float(seconds), 0.0))


@given(st.integers(min_value=-1_000_000, max_value=1_000_000))
def test_parse_retry_after_numeric_string_matches_float(value: int) -> None:
    """String payloads representing numbers should map to their float value."""

    payload = f"  {value}  "
    result = api_client.parse_retry_after(payload)
    assert result == pytest.approx(max(float(value), 0.0))


@given(st.timedeltas(min_value=timedelta(days=-1), max_value=timedelta(days=365)))
def test_parse_retry_after_http_date_uses_future_delta(delta: timedelta) -> None:
    """HTTP-date Retry-After headers should reflect the delta from the current time."""

    now = datetime(2024, 1, 1, tzinfo=timezone.utc)
    target = now + delta
    header_value = format_datetime(target)

    with patch("bioetl.core.api_client._current_utc_time", return_value=now):
        result = api_client.parse_retry_after(header_value)

    parsed = parsedate_to_datetime(header_value)
    assert parsed is not None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    expected = max((parsed - now).total_seconds(), 0.0)

    assert result == pytest.approx(expected, abs=1e-6, rel=1e-6)
