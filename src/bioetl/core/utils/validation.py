"""Utilities for processing Pandera ``SchemaErrors`` instances."""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping
from typing import Any, cast

import pandas as pd
from pandera.errors import SchemaErrors

__all__ = ["summarize_schema_errors", "format_failure_cases"]


def summarize_schema_errors(exc: SchemaErrors) -> dict[str, Any]:
    """Return high-level summary information extracted from ``SchemaErrors``."""

    summary: dict[str, Any] = {
        "affected_rows": 0,
        "affected_columns": [],
        "error_types": {},
        "message": str(exc),
    }

    failure_cases = getattr(exc, "failure_cases", None)
    if isinstance(failure_cases, pd.DataFrame) and not failure_cases.empty:
        if "index" in failure_cases.columns:
            summary["affected_rows"] = int(failure_cases["index"].nunique())
        else:
            summary["affected_rows"] = int(len(failure_cases))

        if "schema_context" in failure_cases.columns:
            context_counter: Counter[str] = Counter(
                str(value) for value in failure_cases["schema_context"].dropna()
            )
            summary["error_types"] = dict(context_counter)

        if "column" in failure_cases.columns:
            summary["affected_columns"] = (
                failure_cases["column"].dropna().astype(str).unique().tolist()
            )

    error_counts_attr = getattr(exc, "error_counts", None)
    if isinstance(error_counts_attr, Mapping):
        error_counts_mapping = cast(Mapping[str, Any], error_counts_attr)
        error_counts: dict[str, int] = {}
        for key_obj, value_obj in error_counts_mapping.items():
            key_str = str(key_obj)
            if isinstance(value_obj, (int, float)):
                error_counts[key_str] = int(value_obj)
            else:
                continue
        summary["error_counts"] = error_counts

    return summary


def format_failure_cases(
    failure_cases: pd.DataFrame,
    *,
    sample_size: int = 5,
) -> dict[str, Any]:
    """Format the Pandera ``failure_cases`` frame for structured logging."""

    if failure_cases.empty:
        return {}

    total_failures = int(len(failure_cases))
    formatted: dict[str, Any] = {"total_failures": total_failures}

    if "index" in failure_cases.columns:
        formatted["unique_rows"] = int(failure_cases["index"].nunique())
    else:
        formatted["unique_rows"] = total_failures

    if "schema_context" in failure_cases.columns:
        context_counter: Counter[str] = Counter(
            str(value) for value in failure_cases["schema_context"].dropna()
        )
        formatted["error_types"] = {
            key: int(count) for key, count in context_counter.most_common(10)
        }

    if "column" in failure_cases.columns:
        column_counter: Counter[str] = Counter(
            str(value) for value in failure_cases["column"].dropna()
        )
        formatted["column_errors"] = {
            key: int(count) for key, count in column_counter.most_common(10)
        }

    if sample_size > 0:
        sample = failure_cases.head(sample_size)
        sample_records: list[dict[str, Any]] = []
        for _, row in sample.iterrows():
            record: dict[str, Any] = {str(column): row[column] for column in sample.columns}
            sample_records.append(record)
        formatted["sample"] = sample_records

    return formatted


