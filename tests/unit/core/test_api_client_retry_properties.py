"""Hypothesis property tests for retry helpers in the unified API client."""

from __future__ import annotations

import math
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime

import pytest
from hypothesis import given
from hypothesis import strategies as st

from bioetl.core.api_client import RetryPolicy, parse_retry_after


@st.composite
def _finite_non_nan_floats(draw: st.DrawFn, *, min_value: float, max_value: float) -> float:
    """Return a finite float within the provided inclusive bounds."""

    value = draw(
        st.floats(
            allow_nan=False,
            allow_infinity=False,
            min_value=min_value,
            max_value=max_value,
        )
    )
    return float(value)


@given(_finite_non_nan_floats(min_value=-1e6, max_value=1e6))
def test_parse_retry_after_numeric_inputs_are_clamped(value: float) -> None:
    """Numeric retry-after payloads should round-trip to non-negative seconds."""

    parsed = parse_retry_after(value)
    assert parsed is not None
    assert parsed >= 0
    if value >= 0:
        assert math.isclose(parsed, float(value))
    else:
        assert parsed == pytest.approx(0.0)


@given(_finite_non_nan_floats(min_value=-1e6, max_value=1e6))
def test_parse_retry_after_numeric_strings_follow_same_rules(value: float) -> None:
    """String encoded numeric values should be treated identically to numbers."""

    encoded = ("%f" % value).rstrip("0").rstrip(".")
    parsed = parse_retry_after(encoded)
    assert parsed is not None
    assert parsed >= 0
    if value >= 0:
        assert math.isclose(parsed, float(encoded))
    else:
        assert parsed == pytest.approx(0.0)


@given(st.integers(min_value=1, max_value=12 * 60 * 60))
def test_parse_retry_after_http_date_strings_produce_future_offsets(seconds: int) -> None:
    """RFC 2822 date headers should translate into positive backoff windows."""

    target = datetime.now(timezone.utc) + timedelta(seconds=seconds)
    header_value = format_datetime(target)

    parsed = parse_retry_after(header_value)
    assert parsed is not None
    assert parsed >= 0
    assert parsed == pytest.approx(seconds, abs=1.0)


@given(
    st.integers(min_value=0, max_value=8),
    _finite_non_nan_floats(min_value=1.1, max_value=5.0),
    st.one_of(st.none(), _finite_non_nan_floats(min_value=1.0, max_value=1e4)),
)
def test_retry_policy_backoff_respects_maximum(
    attempt: int, backoff_factor: float, backoff_max: float | None
) -> None:
    """Exponential wait calculations should be capped by the configured maximum."""

    policy = RetryPolicy(backoff_factor=backoff_factor, backoff_max=backoff_max)

    wait = policy.get_wait_time(attempt)
    expected = float(backoff_factor) ** float(max(attempt, 0))
    if backoff_max is not None:
        expected = min(expected, backoff_max)

    assert wait == pytest.approx(expected)


@given(_finite_non_nan_floats(min_value=0.0, max_value=7200.0))
def test_retry_policy_honours_retry_after_override(override: float) -> None:
    """Retry-After override should take precedence over exponential backoff."""

    policy = RetryPolicy(backoff_factor=3.5, backoff_max=100.0)

    wait = policy.get_wait_time(5, retry_after=override)
    assert wait == pytest.approx(override)
