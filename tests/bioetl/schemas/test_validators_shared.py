"""Tests for shared schema validators."""

from __future__ import annotations

import pandas as pd

from bioetl.schemas._validators import (
    RELATIONS,
    validate_membership_series,
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
