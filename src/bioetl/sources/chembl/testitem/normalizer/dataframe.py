"""Pandas helpers for the TestItem pipeline."""

from __future__ import annotations

from collections.abc import Sequence

import pandas as pd

from bioetl.normalizers import registry
from bioetl.utils.dtypes import coerce_nullable_int, coerce_optional_bool

__all__ = [
    "normalize_smiles_columns",
    "coerce_boolean_and_integer_columns",
]


def normalize_smiles_columns(
    df: pd.DataFrame,
    *,
    canonical_column: str = "canonical_smiles",
    standardized_column: str = "standardized_smiles",
) -> pd.DataFrame:
    """Normalize SMILES columns and drop the canonical placeholder."""

    if canonical_column not in df.columns:
        return df

    canonical_series = df[canonical_column]
    normalized_canonical = canonical_series.apply(
        lambda value: registry.normalize("chemistry", value) if pd.notna(value) else None
    )

    if standardized_column in df.columns:
        missing_mask = df[standardized_column].isna()
        if missing_mask.any():
            df.loc[missing_mask, standardized_column] = normalized_canonical[missing_mask]
    else:
        df[standardized_column] = normalized_canonical

    df = df.drop(columns=[canonical_column], errors="ignore")
    return df


def coerce_boolean_and_integer_columns(
    df: pd.DataFrame,
    *,
    boolean_columns: Sequence[str],
    nullable_int_columns: Sequence[str],
    int_minimums: dict[str, int],
) -> pd.DataFrame:
    """Apply boolean/int coercions consistent with TestItem schema."""

    if boolean_columns:
        coerce_optional_bool(df, columns=list(boolean_columns), nullable=False)

    if nullable_int_columns:
        defaults = dict.fromkeys(nullable_int_columns, 0)
        defaults.update(int_minimums)
        coerce_nullable_int(df, list(nullable_int_columns), min_values=defaults)

    return df
