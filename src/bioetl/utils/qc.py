"""Utility helpers for building QC summaries and artifacts."""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping, Sequence
from typing import Any

import numpy as np
import pandas as pd

from bioetl.utils.dtypes import coerce_retry_after

__all__ = [
    "compute_field_coverage",
    "duplicate_summary",
    "prepare_enrichment_metrics",
    "prepare_missing_mappings",
    "register_fallback_statistics",
    "update_summary_metrics",
    "update_summary_section",
    "update_validation_issue_summary",
]


def _to_builtin(value: Any) -> Any:
    """Best-effort conversion of pandas/numpy scalars to Python primitives."""

    if value is None:
        return None

    if value is pd.NA:  # pragma: no cover - direct identity check
        return None

    if isinstance(value, (str, bool, int, float)):
        return value

    if isinstance(value, np.generic):
        return value.item()

    if isinstance(value, (pd.Timestamp, pd.Timedelta)):
        return value.isoformat()

    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:  # pragma: no cover - defensive guard
            return value

    return value


def _convert_nested(value: Any) -> Any:
    """Recursively coerce nested structures to JSON-serialisable primitives."""

    if isinstance(value, Mapping):
        return {str(key): _convert_nested(item) for key, item in value.items()}

    if isinstance(value, (list, tuple, set)):
        return [_convert_nested(item) for item in value]

    return _to_builtin(value)


def update_summary_section(
    summary: dict[str, Any],
    section: str,
    values: Mapping[str, Any] | Any,
    *,
    merge: bool = True,
) -> Any:
    """Update a QC summary section, normalising values for JSON serialisation."""

    converted = _convert_nested(values)

    if merge and isinstance(converted, Mapping):
        target = summary.setdefault(section, {})
        target.update(converted)
        return target

    summary[section] = converted
    return converted


def update_summary_metrics(summary: dict[str, Any], metrics: Mapping[str, Any]) -> None:
    """Merge QC metrics into the summary payload."""

    if not metrics:
        return

    section = summary.setdefault("metrics", {})
    section.update(_convert_nested(metrics))


def update_validation_issue_summary(
    summary: dict[str, Any], issues: Sequence[Mapping[str, Any]]
) -> None:
    """Populate validation issue counters in the QC summary."""

    if not issues:
        summary["validation_issue_counts"] = {}
        summary["validation_issue_total"] = 0
        return

    counts = Counter(str(issue.get("severity", "info")).lower() for issue in issues)
    summary["validation_issue_counts"] = dict(counts)
    summary["validation_issue_total"] = int(sum(counts.values()))


def duplicate_summary(
    total_rows: int,
    duplicate_count: int,
    *,
    field: str | None = None,
    threshold: float | int | None = None,
) -> dict[str, Any]:
    """Return a structured duplicate summary for QC reporting."""

    ratio = float(duplicate_count / total_rows) if total_rows else 0.0
    payload: dict[str, Any] = {
        "count": int(duplicate_count),
        "ratio": ratio,
        "total": int(total_rows),
    }

    if field is not None:
        payload["field"] = field

    if threshold is not None:
        payload["threshold"] = _to_builtin(threshold)

    return payload


def compute_field_coverage(
    df: pd.DataFrame, columns: Sequence[str], *, normalise: bool = True
) -> dict[str, float]:
    """Calculate coverage ratios for the requested columns."""

    total = int(len(df))
    coverage: dict[str, float] = {}

    if total == 0:
        return dict.fromkeys(columns, 0.0)

    for column in columns:
        if column not in df.columns:
            coverage[column] = 0.0
            continue

        present = df[column].notna().sum()
        coverage[column] = float(present / total) if normalise else float(present)

    return coverage


def register_fallback_statistics(
    df: pd.DataFrame,
    *,
    summary: dict[str, Any],
    source_column: str = "source_system",
    fallback_marker: str = "CHEMBL_FALLBACK",
    id_column: str | None = None,
    fallback_columns: Sequence[str] | None = None,
    reason_column: str = "fallback_reason",
) -> pd.DataFrame:
    """Compute fallback statistics, updating the summary and returning records."""

    total_rows = int(len(df))
    update_summary_section(summary, "row_counts", {"total": total_rows})

    if source_column not in df.columns:
        summary.pop("fallbacks", None)
        return pd.DataFrame(columns=list(fallback_columns or []))

    source_series = df[source_column].astype("string")
    marker = str(fallback_marker).upper()
    fallback_mask = source_series.str.upper() == marker

    fallback_count = int(fallback_mask.sum())
    success_count = int(total_rows - fallback_count)
    fallback_rate = float(fallback_count / total_rows) if total_rows else 0.0

    fallback_summary: dict[str, Any] = {
        "total_rows": total_rows,
        "success_count": success_count,
        "fallback_count": fallback_count,
        "fallback_rate": fallback_rate,
    }

    available_columns: list[str] = []
    if fallback_columns:
        available_columns = [column for column in fallback_columns if column in df.columns]

    fallback_records = pd.DataFrame(columns=available_columns)
    if fallback_count and (available_columns or not fallback_columns):
        record_columns = available_columns if available_columns else list(df.columns)
        fallback_records = (
            df.loc[fallback_mask, record_columns]
            .copy()
            .reset_index(drop=True)
            .convert_dtypes()
        )

        coerce_retry_after(fallback_records)

        if reason_column in fallback_records.columns:
            counts = (
                fallback_records[reason_column]
                .fillna("<missing>")
                .astype(str)
                .value_counts(dropna=False)
                .to_dict()
            )
            fallback_summary["reason_counts"] = {
                str(reason): int(count) for reason, count in counts.items()
            }

        if id_column and id_column in fallback_records.columns:
            id_series = pd.to_numeric(fallback_records[id_column], errors="coerce")
            ids = {int(value) for value in id_series.dropna().astype(int).tolist()}
            fallback_summary["ids"] = sorted(ids)

    update_summary_section(
        summary,
        "row_counts",
        {"total": total_rows, "success": success_count, "fallback": fallback_count},
    )
    summary["fallbacks"] = _convert_nested(fallback_summary)

    return fallback_records


def prepare_missing_mappings(
    records: Sequence[Mapping[str, Any]],
    *,
    sort_by: Sequence[str] = ("stage", "target_chembl_id", "input_accession"),
) -> pd.DataFrame:
    """Materialise QC missing mapping records into a deterministic dataframe."""

    if not records:
        return pd.DataFrame()

    frame = pd.DataFrame(records).convert_dtypes()
    ordering = [column for column in sort_by if column in frame.columns]
    if ordering:
        frame = frame.sort_values(ordering, kind="stable").reset_index(drop=True)

    return frame


def prepare_enrichment_metrics(
    records: Sequence[Mapping[str, Any]],
    *,
    sort_by: Sequence[str] = ("metric",),
) -> pd.DataFrame:
    """Convert enrichment metric payloads into a dataframe for export."""

    if not records:
        return pd.DataFrame()

    frame = pd.DataFrame(records).convert_dtypes()
    ordering = [column for column in sort_by if column in frame.columns]
    if ordering:
        frame = frame.sort_values(ordering, kind="stable").reset_index(drop=True)

    return frame
