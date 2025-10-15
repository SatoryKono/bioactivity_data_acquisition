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

    merged_columns = list(merged.columns)
    ordered_columns: list[str] = []
    seen: set[str] = set()

    def extend_with(columns: Sequence[str], suffix: str) -> None:
        for column in columns:
            candidates = [column]
            if suffix:
                candidates.append(f"{column}{suffix}")
            for candidate in candidates:
                if candidate in merged_columns and candidate not in seen:
                    ordered_columns.append(candidate)
                    seen.add(candidate)
                    break

    extend_with(list(left.columns), suffixes[0])
    extend_with(list(right.columns), suffixes[1])

    ordered_columns.extend([column for column in merged_columns if column not in seen])
    return merged.loc[:, ordered_columns]


def ensure_unique(left: pd.DataFrame, subset: Iterable[str]) -> pd.DataFrame:
    """Return a DataFrame without duplicate rows on the provided subset."""

    deduplicated = left.drop_duplicates(subset=list(subset))
    deduplicated.reset_index(drop=True, inplace=True)
    return deduplicated
