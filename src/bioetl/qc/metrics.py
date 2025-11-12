"""Low-level QC metric computations."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable, Sequence
from decimal import Decimal, ROUND_HALF_UP
from typing import TypedDict

import pandas as pd


def _string_dtype_repr(self: pd.StringDtype) -> str:
    """Patch pandas ``StringDtype`` to expose storage in its textual form."""

    storage = getattr(self, "storage", None)
    normalized = storage or getattr(self, "_storage", None)
    if normalized:
        return f"string[{normalized}]"
    return "string"


# Ensure deterministic ``string[python]`` representation even on older pandas.
pd.StringDtype.__repr__ = _string_dtype_repr  # type: ignore[assignment]
pd.StringDtype.__str__ = _string_dtype_repr  # type: ignore[assignment]

__all__ = [
    "compute_duplicate_stats",
    "compute_missingness",
    "compute_correlation_matrix",
    "compute_categorical_distributions",
]


class DuplicateStats(TypedDict):
    """Structured duplicate statistics returned by :func:`compute_duplicate_stats`."""

    row_count: int
    duplicate_count: int
    duplicate_ratio: float
    deduplicated_count: int


def compute_duplicate_stats(
    df: pd.DataFrame,
    *,
    business_key_fields: Sequence[str] | None = None,
) -> DuplicateStats:
    """Return deterministic duplicate statistics for ``df``.

    ``business_key_fields`` allows the caller to provide a logical business key
    distinct from the full row identity. When omitted the check uses the
    complete row to determine duplicates.
    """

    total_rows = int(len(df))
    if total_rows == 0:
        return DuplicateStats(
            row_count=0,
            duplicate_count=0,
            duplicate_ratio=0.0,
            deduplicated_count=0,
        )

    subset = list(business_key_fields) if business_key_fields else None
    if subset:
        deduplicated = df.drop_duplicates(subset=subset, keep="first")
    else:
        deduplicated = df.drop_duplicates(keep="first")

    deduplicated_count = int(len(deduplicated))
    duplicate_count = total_rows - deduplicated_count
    duplicate_ratio = float(duplicate_count / total_rows) if total_rows else 0.0

    return DuplicateStats(
        row_count=total_rows,
        duplicate_count=duplicate_count,
        duplicate_ratio=duplicate_ratio,
        deduplicated_count=deduplicated_count,
    )


class CategoricalDistributionEntry(TypedDict):
    """Single bucket statistics for categorical distributions."""

    count: int
    ratio: float


CategoricalDistribution = dict[str, dict[str, CategoricalDistributionEntry]]


def _default_value_normalizer(value: object) -> str:
    """Normalize raw categorical values to canonical textual keys."""

    if pd.isna(value):
        return "null"

    if isinstance(value, str):
        normalized = value.strip()
        return normalized if normalized else "__empty__"

    normalized = str(value)
    return normalized.strip() if normalized.strip() else "__empty__"


def _quantize_ratio(value: Decimal, *, precision: int) -> Decimal:
    """Quantize ratios using half-up rounding with deterministic precision."""

    quantizer = Decimal((0, (1,), -precision))
    return value.quantize(quantizer, rounding=ROUND_HALF_UP)


def _ensure_ratio_sum(
    ordered_items: list[tuple[str, int, Decimal]],
    *,
    precision: int,
) -> list[tuple[str, int, float]]:
    """Convert Decimal ratios to floats ensuring their rounded sum equals 1."""

    quantized: list[Decimal] = []
    total = Decimal("0")
    one = Decimal("1")
    for index, (_, _, ratio) in enumerate(ordered_items):
        if index == len(ordered_items) - 1:
            remaining = one - total
            if remaining < Decimal("0"):
                remaining = Decimal("0")
            quantized_ratio = _quantize_ratio(remaining, precision=precision)
        else:
            quantized_ratio = _quantize_ratio(ratio, precision=precision)
            total += quantized_ratio
            if total > one:
                total = one
        quantized.append(quantized_ratio)

    adjusted: list[tuple[str, int, float]] = []
    for (value, count, _), ratio_decimal in zip(ordered_items, quantized, strict=True):
        adjusted.append((value, count, float(ratio_decimal)))
    return adjusted


def compute_categorical_distributions(
    df: pd.DataFrame,
    *,
    column_suffixes: Sequence[str],
    value_normalizer: Callable[[object], str] | None = None,
    top_n: int = 20,
    ratio_precision: int = 6,
    other_bucket_label: str = "__other__",
) -> CategoricalDistribution:
    """Return deterministic categorical distributions for columns matching suffixes.

    Args:
        df: Исходный DataFrame.
        column_suffixes: Список суффиксов, по которым выбираются целевые колонки.
        value_normalizer: Функция нормализации категорий; по умолчанию убирает
            пробелы, нормализует пустые значения и NaN.
        top_n: Максимальное число категорий на колонку; избыточные категории
            агрегируются в ``other_bucket_label``.
        ratio_precision: Количество знаков после запятой для метрик долей.
        other_bucket_label: Имя агрегированного bucket для редких категорий.

    Returns:
        Словарь: ``{column: {category: {count, ratio}}}``, где ключи отсортированы
        по имени колонки и по убыванию счётчиков с детерминированной альф. стабилизацией.
    """

    if top_n <= 0:
        msg = "top_n must be positive to maintain deterministic ordering"
        raise ValueError(msg)
    if ratio_precision < 0:
        msg = "ratio_precision must be non-negative"
        raise ValueError(msg)

    normalizer = value_normalizer or _default_value_normalizer

    matched_columns = [
        column
        for column in df.columns
        if any(column.endswith(suffix) for suffix in column_suffixes)
    ]

    distributions: CategoricalDistribution = {}
    for column in sorted(matched_columns):
        series = df[column]
        non_null_count = int(series.notna().sum())
        if non_null_count == 0:
            continue

        raw_counts = series.astype("object").value_counts(dropna=False, sort=False)
        aggregated: dict[str, int] = defaultdict(int)
        for raw_value, raw_count in raw_counts.items():
            normalized_value = normalizer(raw_value)
            aggregated[normalized_value] += int(raw_count)

        # Determine priority order for top-N selection.
        prioritized = sorted(
            aggregated.items(),
            key=lambda item: (-item[1], item[0]),
        )

        if len(prioritized) > top_n:
            retained = prioritized[:top_n]
            remainder_count = sum(count for _, count in prioritized[top_n:])
            merged: dict[str, int] = {key: count for key, count in retained}
            if remainder_count > 0:
                merged[other_bucket_label] = merged.get(other_bucket_label, 0) + remainder_count
            final_items = merged.items()
        else:
            final_items = aggregated.items()

        ordered = sorted(final_items, key=lambda item: item[0])
        total_count = sum(count for _, count in ordered)
        if total_count == 0:
            continue

        ratio_entries: list[tuple[str, int, Decimal]] = [
            (value, count, Decimal(count) / Decimal(total_count))
            for value, count in ordered
        ]
        quantized = _ensure_ratio_sum(ratio_entries, precision=ratio_precision)

        if pd.isna(column):
            msg = f"Column key must not be NaN: {column!r}"
            raise ValueError(msg)

        inner_keys = [value for value, *_ in quantized]
        if inner_keys != sorted(inner_keys):
            msg = f"Distribution keys must be sorted deterministically for {column}"
            raise ValueError(msg)

        for value in inner_keys:
            if pd.isna(value):
                msg = f"Category key must not be NaN in column {column}"
                raise ValueError(msg)

        inner = {value: {"count": count, "ratio": ratio} for value, count, ratio in quantized}
        distributions[column] = inner

    return distributions
def compute_missingness(df: pd.DataFrame) -> pd.DataFrame:
    """Return per-column missing value statistics with stable ordering."""

    if df.empty:
        result = pd.DataFrame(
            {
                "column": pd.Series(dtype="string[python]"),
                "missing_count": pd.Series(dtype="int64"),
                "missing_ratio": pd.Series(dtype="float64"),
            }
        )
        return result.astype(
            {
                "column": "string[python]",
                "missing_count": "int64",
                "missing_ratio": "float64",
            }
        )

    column_dtype = pd.StringDtype(storage="python")

    missing_count = df.isna().sum()
    total_rows = int(len(df))
    missing_count_int = missing_count.astype("int64")
    missing_ratio = (missing_count / total_rows).astype("float64")

    column_labels: list[str] = [str(label) for label in missing_count.index]
    column_array = pd.array(column_labels, dtype=column_dtype)

    result = pd.DataFrame(
        {
            "column": column_array,
            "missing_count": pd.Series(missing_count_int.values, dtype="int64"),
            "missing_ratio": pd.Series(missing_ratio.values, dtype="float64"),
        }
    )
    result = result.astype(
        {
            "column": column_dtype,
            "missing_count": "int64",
            "missing_ratio": "float64",
        }
    )
    result = result.sort_values(
        by=["missing_count", "column"],
        ascending=[False, True],
        kind="mergesort",
    ).reset_index(drop=True)
    return result


def compute_correlation_matrix(df: pd.DataFrame) -> pd.DataFrame | None:
    """Return the numeric correlation matrix or ``None`` when not applicable.

    Columns are sorted alphabetically to guarantee deterministic output.
    """

    numeric_df = df.select_dtypes(include=["number", "bool"])
    if numeric_df.shape[1] < 2:
        return None
    ordered_columns = sorted(numeric_df.columns)
    numeric_df = numeric_df.loc[:, ordered_columns]
    correlation = numeric_df.corr(numeric_only=True)
    if correlation.empty:
        return None
    return correlation.reindex(index=ordered_columns, columns=ordered_columns)
