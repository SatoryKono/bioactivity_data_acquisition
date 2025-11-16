"""Tests for JSON-series validators."""

from __future__ import annotations

import math

import pandas as pd

from bioetl.schemas._validators import validate_json_series


def test_validate_json_series_rejects_invalid_payload() -> None:
    series = pd.Series(['{"valid": true}', "not-json"])
    assert validate_json_series(series) is False


def test_validate_json_series_optional_accepts_missing_values() -> None:
    series = pd.Series([None, pd.NA, float("nan"), "{\"key\": 1}"])
    assert validate_json_series(series, optional=True) is True


def test_validate_json_series_optional_rejects_invalid_values() -> None:
    series = pd.Series([None, "not-json"])
    assert validate_json_series(series, optional=True) is False


def test_validate_json_series_optional_false_requires_values() -> None:
    series = pd.Series([None, "{\"key\": 1}"])
    assert validate_json_series(series, optional=False) is False


def test_validate_json_series_optional_all_missing_is_true() -> None:
    series = pd.Series([math.nan, pd.NA, None])
    assert validate_json_series(series, optional=True) is True


def test_validate_json_series_accepts_valid_payloads() -> None:
    series = pd.Series(['{"valid": true}', '{"items": [1, 2]}'])
    assert validate_json_series(series) is True
