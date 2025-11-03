"""Property-based tests for :class:`bioetl.normalizers.date.DateNormalizer`."""

from __future__ import annotations

from datetime import UTC, date, datetime

import pandas as pd
import pytest

hypothesis = pytest.importorskip("hypothesis")
strategies = pytest.importorskip("hypothesis.strategies")
from hypothesis import given

from bioetl.normalizers.date import DateNormalizer
from bioetl.normalizers.registry import registry

_DATE_STRATEGY = strategies.dates(min_value=date(1900, 1, 1), max_value=date(2100, 12, 31))


@given(_DATE_STRATEGY)
def test_normalize_date_instances_returns_iso(candidate: date) -> None:
    normalizer = DateNormalizer()
    assert normalizer.normalize(candidate) == candidate.isoformat()


@given(_DATE_STRATEGY)
def test_normalize_iso_strings_is_idempotent(candidate: date) -> None:
    normalizer = DateNormalizer()
    iso = candidate.isoformat()
    assert normalizer.normalize(iso) == iso


@given(_DATE_STRATEGY, strategies.times())
def test_normalize_datetime_preserves_date_part(d: date, t: datetime.time) -> None:
    normalizer = DateNormalizer()
    dt = datetime.combine(d, t)
    assert normalizer.normalize(dt) == d.isoformat()


@given(_DATE_STRATEGY)
def test_registry_exposes_date_normalizer(candidate: date) -> None:
    iso = candidate.isoformat()
    assert registry.normalize("date", iso) == iso


def test_normalize_timestamp_with_timezone() -> None:
    normalizer = DateNormalizer()
    timestamp = pd.Timestamp(datetime(2024, 5, 1, 12, 0, tzinfo=UTC))
    assert normalizer.normalize(timestamp) == "2024-05-01"


@pytest.mark.parametrize("value", ["", "invalid", 123])
def test_normalize_invalid_inputs_returns_none(value: object) -> None:
    normalizer = DateNormalizer()
    assert normalizer.normalize(value) is None
