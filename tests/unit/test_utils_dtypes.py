"""Unit tests for :mod:`bioetl.utils.dtypes`."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pandas as pd
import pytest
from pandas.api.types import is_integer_dtype

MODULE_PATH = Path(__file__).resolve().parents[2] / "src" / "bioetl" / "utils" / "dtypes.py"
SPEC = importlib.util.spec_from_file_location("bioetl.utils.dtypes", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
_module = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(_module)

coerce_nullable_int = getattr(_module, "coerce_nullable_int")
coerce_retry_after = getattr(_module, "coerce_retry_after")


def test_coerce_nullable_int_replaces_fractional_values_with_na() -> None:
    df = pd.DataFrame({
        "col_a": ["1", "2", "3.5", None],
        "col_b": ["7.0", "8.2", pd.NA, "9"],
    })

    coerce_nullable_int(df, ["col_a", "col_b"])

    assert is_integer_dtype(df["col_a"])
    assert list(df["col_a"].astype("object")) == [1, 2, pd.NA, pd.NA]
    assert is_integer_dtype(df["col_b"])
    assert list(df["col_b"].astype("object")) == [7, pd.NA, pd.NA, 9]


def test_coerce_nullable_int_respects_minimum_constraints() -> None:
    df = pd.DataFrame({"value": [-1, 0, 1, 2, 3]})

    coerce_nullable_int(df, ["value"], min_values=({"value": 1}, 0))

    assert df["value"].dtype == "Int64"
    assert list(df["value"].astype("object")) == [pd.NA, pd.NA, 1, 2, 3]


def test_coerce_retry_after_normalises_object_series() -> None:
    df = pd.DataFrame({"fallback_retry_after_sec": [pd.NA, "1.5", None]})

    coerce_retry_after(df)

    assert str(df["fallback_retry_after_sec"].dtype) == "float64"
    assert pd.isna(df.at[0, "fallback_retry_after_sec"])
    assert df.at[1, "fallback_retry_after_sec"] == pytest.approx(1.5)
