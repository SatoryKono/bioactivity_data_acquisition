"""Utility helpers for deterministic DataFrame joins."""

from __future__ import annotations

from typing import Iterable, Sequence

import pandas as pd


def safe_left_join(
    left: pd.DataFrame,
    right: pd.DataFrame,
    *,
    on: Sequence[str] | str,
    suffixes: tuple[str, str] = ("_left", "_right"),
    validate: str = "one_to_many",
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
