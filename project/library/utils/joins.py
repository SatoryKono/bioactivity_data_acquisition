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

    def append_column(name: str, *, primary_suffix: str, secondary_suffix: str) -> None:
        for candidate in (name, f"{name}{primary_suffix}", f"{name}{secondary_suffix}"):
            if candidate in merged.columns and candidate not in seen:
                ordered_columns.append(candidate)
                seen.add(candidate)
                break

    ordered_columns: list[str] = []
    seen: set[str] = set()

    for column in left.columns:
        append_column(column, primary_suffix=suffixes[0], secondary_suffix=suffixes[1])

    for column in right.columns:
        append_column(column, primary_suffix=suffixes[1], secondary_suffix=suffixes[0])

    ordered_columns.extend([column for column in merged.columns if column not in seen])

    return merged.loc[:, ordered_columns]


def ensure_unique(left: pd.DataFrame, subset: Iterable[str]) -> pd.DataFrame:
    """Return a DataFrame without duplicate rows on the provided subset."""

    deduplicated = left.drop_duplicates(subset=list(subset))
    deduplicated.reset_index(drop=True, inplace=True)
    return deduplicated
