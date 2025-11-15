"""Tests for shared schema validators."""

from __future__ import annotations

import pandas as pd

from bioetl.schemas._validators import (
    RELATIONS,
    validate_json_series,
    validate_membership_series,
    validate_optional_json_series,
    validate_relation_series,
)


def test_validate_relation_series_accepts_allowed_values() -> None:
    series = pd.Series(["=", "<", ">"])
    assert validate_relation_series(series)


def test_validate_relation_series_rejects_unknown_value() -> None:
    series = pd.Series(["=", "??"])
    assert not validate_relation_series(series, allowed=RELATIONS)


def test_validate_membership_series_strips_values() -> None:
    series = pd.Series([" A ", "B "])
    assert validate_membership_series(series, allowed=("A", "B"))


def test_validate_json_series_rejects_invalid_payload() -> None:
    series = pd.Series(['{"valid": true}', "not-json"])
    assert validate_json_series(series) is False


def test_validate_optional_json_series_ignores_missing_values() -> None:
    series = pd.Series([None, pd.NA, float("nan"), "{\"key\": 1}"])
    assert validate_optional_json_series(series) is True

    invalid_series = pd.Series([None, "not-json"])
    assert validate_optional_json_series(invalid_series) is False

