"""Unit tests for :mod:`bioetl.utils.dtypes`."""

from __future__ import annotations

import pandas as pd
import pytest

from bioetl.utils.dtypes import coerce_nullable_int, coerce_retry_after


def test_coerce_nullable_int_masks_fractional_and_below_minimum() -> None:
    df = pd.DataFrame({"value": ["7", "not-a-number", -3, "4.5", None]})

    coerce_nullable_int(df, ["value"], min_values={"value": 0})

    series = df["value"]
    assert series.dtype == "Int64"
    assert series.tolist() == [7, pd.NA, pd.NA, pd.NA, pd.NA]


def test_coerce_nullable_int_supports_default_minimum() -> None:
    df = pd.DataFrame(
        {
            "a": ["5", "-1", None],
            "b": ["2", "0", "5"],
        }
    )

    coerce_nullable_int(df, ["a", "b"], min_values={"__default__": 0, "b": 1})

    assert df["a"].tolist() == [5, pd.NA, pd.NA]
    assert df["b"].tolist() == [2, pd.NA, 5]


def test_coerce_retry_after_normalises_numeric_strings() -> None:
    df = pd.DataFrame({"fallback_retry_after_sec": [pd.NA, "3.2", "invalid"]})

    coerce_retry_after(df)

    series = df["fallback_retry_after_sec"]
    assert series.dtype == "float64"
    assert pytest.approx(series.iloc[1]) == 3.2
    assert pd.isna(series.iloc[0])
    assert pd.isna(series.iloc[2])
