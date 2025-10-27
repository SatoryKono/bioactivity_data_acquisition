"""Utility helpers for deterministic DataFrame joins."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from typing import Literal

import pandas as pd


def safe_left_join(
    left: pd.DataFrame,
    right: pd.DataFrame,
    *,
    on: Sequence[str] | str,
    suffixes: tuple[str, str] = ("_left", "_right"),
    validate: Literal["one_to_one", "1:1", "one_to_many", "1:m", "many_to_one", "m:1", "many_to_many", "m:m"] = "one_to_many",
) -> pd.DataFrame:
    """Perform a left join with validation and deterministic column ordering."""

    merged = left.merge(right, how="left", on=on, suffixes=suffixes, validate=validate)
    ordered_columns: list[str] = list(dict.fromkeys(list(left.columns) + list(right.columns)))
    return merged.loc[:, [column for column in ordered_columns if column in merged.columns]]


def ensure_unique(left: pd.DataFrame, subset: Iterable[str]) -> pd.DataFrame:
    """Return a DataFrame without duplicate rows on the provided subset."""

    deduplicated = left.drop_duplicates(subset=list(subset))
    deduplicated.reset_index(drop=True, inplace=True)
    return deduplicated


__all__ = ["safe_left_join", "ensure_unique"]
