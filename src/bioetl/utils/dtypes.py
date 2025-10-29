"""Utilities for coercing dataframe columns to schema-aligned dtypes."""

from __future__ import annotations

from collections.abc import Iterable, Mapping

import numpy as np
import pandas as pd

NullableIntMinSpec = (
    Mapping[str, int]
    | tuple[Mapping[str, int] | None, int | None]
    | int
    | None
)

__all__ = ["coerce_nullable_int", "coerce_retry_after"]


def _resolve_minimum(column: str, spec: NullableIntMinSpec) -> int | None:
    """Resolve minimum allowed value for ``column`` from ``spec``."""

    if spec is None:
        return None

    if isinstance(spec, tuple) and len(spec) == 2:
        column_map, default_min = spec
        if column_map and column in column_map:
            return column_map[column]
        return default_min

    if isinstance(spec, Mapping):
        if column in spec:
            return spec[column]
        return spec.get("*")

    try:
        return int(spec)
    except (TypeError, ValueError):  # pragma: no cover - defensive
        return None


def coerce_nullable_int(
    df: pd.DataFrame,
    columns: Iterable[str],
    *,
    min_values: NullableIntMinSpec = None,
) -> None:
    """Normalise integer-like columns to Pandas' nullable ``Int64`` dtype."""

    target_dtype = pd.Int64Dtype()

    for column in columns:
        if column not in df.columns:
            continue

        series = pd.to_numeric(df[column], errors="coerce")

        minimum = _resolve_minimum(column, min_values)
        if minimum is not None:
            below_min_mask = series < minimum
            if below_min_mask.any():
                series.loc[below_min_mask] = pd.NA

        if series.empty:
            df[column] = pd.Series(pd.array(series, dtype=target_dtype), index=df.index)
            continue

        non_null = series.notna()
        fractional_mask = pd.Series(False, index=series.index)
        if non_null.any():
            remainders = (series[non_null] % 1).abs()
            fractional_mask.loc[non_null] = ~np.isclose(remainders, 0.0)

        if fractional_mask.any():
            series.loc[fractional_mask] = pd.NA

        coerced = pd.Series(pd.array(series, dtype=target_dtype), index=df.index)
        df[column] = coerced


def coerce_retry_after(
    df: pd.DataFrame,
    column: str = "fallback_retry_after_sec",
) -> None:
    """Coerce retry-after metadata column to concrete ``float64`` dtype."""

    if column not in df.columns:
        return

    series = df[column]
    if not isinstance(series, pd.Series):  # pragma: no cover - defensive guard
        return

    numeric = pd.to_numeric(series, errors="coerce")
    df[column] = numeric.astype("float64", copy=False)
