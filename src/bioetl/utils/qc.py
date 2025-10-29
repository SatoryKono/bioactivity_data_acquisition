"""Utility helpers for building QC summaries and artifacts."""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Iterable

import numpy as np
import pandas as pd

from bioetl.utils.dtypes import coerce_retry_after

__all__ = [
    "QCMetric",
    "QCMetricsRegistry",
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


@dataclass(slots=True)
class QCMetric:
    """Canonical representation of a QC metric evaluation."""

    name: str
    value: Any
    passed: bool
    severity: str
    threshold: Any | None = None
    threshold_min: float | int | None = None
    threshold_max: float | int | None = None
    count: int | None = None
    details: Mapping[str, Any] | None = None

    def to_payload(self) -> dict[str, Any]:
        """Return a JSON-serialisable payload for summaries and reports."""

        payload: dict[str, Any] = {
            "value": _convert_nested(self.value),
            "passed": bool(self.passed),
            "severity": str(self.severity),
        }

        if self.threshold is not None:
            payload["threshold"] = _convert_nested(self.threshold)

        if self.threshold_min is not None:
            payload["threshold_min"] = _to_builtin(self.threshold_min)

        if self.threshold_max is not None:
            payload["threshold_max"] = _to_builtin(self.threshold_max)

        if self.count is not None:
            payload["count"] = _to_builtin(self.count)

        if self.details:
            payload["details"] = _convert_nested(self.details)

        return payload

    def to_issue_payload(self, *, prefix: str = "qc") -> dict[str, Any]:
        """Convert the metric into the payload used by validation issues."""

        metric_name = f"{prefix}.{self.name}" if prefix else self.name
        issue: dict[str, Any] = {
            "metric": metric_name,
            "issue_type": "qc_metric",
            "severity": str(self.severity),
            "value": _convert_nested(self.value),
            "passed": bool(self.passed),
        }

        if self.threshold is not None:
            issue["threshold"] = _convert_nested(self.threshold)

        if self.threshold_min is not None:
            issue["threshold_min"] = _to_builtin(self.threshold_min)

        if self.threshold_max is not None:
            issue["threshold_max"] = _to_builtin(self.threshold_max)

        if self.count is not None:
            issue["count"] = _to_builtin(self.count)

        if self.details:
            issue["details"] = _convert_nested(self.details)

        return issue


class QCMetricsRegistry:
    """Utility for normalising QC metrics against configuration thresholds."""

    def __init__(self, qc_config: Any | None = None):
        thresholds: Mapping[str, Any] | None = None
        if qc_config is None:
            thresholds = None
        elif isinstance(qc_config, Mapping):
            thresholds = qc_config
        else:
            thresholds = getattr(qc_config, "thresholds", None)

        self._thresholds: dict[str, Any] = dict(thresholds or {})
        self._metrics: dict[str, QCMetric] = {}

    @staticmethod
    def _coerce_number(value: Any) -> float | None:
        """Safely convert a threshold-like value to float."""

        if value is None:
            return None

        if isinstance(value, (int, float)):
            return float(value)

        try:
            return float(value)  # type: ignore[arg-type]
        except (TypeError, ValueError):
            return None

    def register(
        self,
        name: str,
        value: Any,
        *,
        comparison: str = "max",
        threshold_key: str | None = None,
        default_threshold: float | int | None = None,
        default_min: float | int | None = None,
        default_max: float | int | None = None,
        pass_severity: str | None = None,
        fail_severity: str | None = None,
        count: int | None = None,
        details: Mapping[str, Any] | None = None,
        passed: bool | None = None,
    ) -> QCMetric:
        """Register a QC metric applying configuration thresholds when available."""

        config_key = threshold_key or name
        config_entry = self._thresholds.get(config_key)

        fail_level = str(fail_severity or "error")
        threshold: Any | None = None
        threshold_min: float | int | None = None
        threshold_max: float | int | None = None

        if isinstance(config_entry, Mapping):
            entry_min = self._coerce_number(config_entry.get("min"))
            entry_max = self._coerce_number(config_entry.get("max"))
            if entry_min is not None:
                threshold_min = entry_min
            if entry_max is not None:
                threshold_max = entry_max
            if "value" in config_entry:
                threshold = config_entry["value"]
            if config_entry.get("severity") is not None:
                fail_level = str(config_entry["severity"])
        elif config_entry is not None:
            numeric = self._coerce_number(config_entry)
            if numeric is not None:
                if comparison == "min":
                    threshold_min = numeric
                elif comparison == "range":
                    threshold_max = numeric if threshold_max is None else threshold_max
                    threshold_min = threshold_min
                else:
                    threshold_max = numeric
                threshold = numeric

        if comparison == "min" and threshold_min is None and default_threshold is not None:
            threshold_min = self._coerce_number(default_threshold)
            threshold = threshold_min
        elif comparison == "max" and threshold_max is None and default_threshold is not None:
            threshold_max = self._coerce_number(default_threshold)
            threshold = threshold_max
        elif comparison == "range":
            if threshold_min is None and default_min is not None:
                threshold_min = self._coerce_number(default_min)
            if threshold_max is None and default_max is not None:
                threshold_max = self._coerce_number(default_max)

        pass_level = str(pass_severity or "info")

        metric_passed = passed
        if metric_passed is None:
            if comparison == "min":
                metric_passed = True
                if threshold_min is not None and value is not None:
                    metric_passed = bool(value >= threshold_min)
            elif comparison == "range":
                metric_passed = True
                if threshold_min is not None and value is not None:
                    metric_passed = metric_passed and bool(value >= threshold_min)
                if threshold_max is not None and value is not None:
                    metric_passed = metric_passed and bool(value <= threshold_max)
            elif comparison == "none":
                metric_passed = bool(value)
            else:  # default to max comparison
                metric_passed = True
                if threshold_max is not None and value is not None:
                    metric_passed = bool(value <= threshold_max)

        severity = pass_level if metric_passed else fail_level

        metric = QCMetric(
            name=name,
            value=value,
            passed=metric_passed,
            severity=severity,
            threshold=threshold,
            threshold_min=threshold_min,
            threshold_max=threshold_max,
            count=count,
            details=dict(details) if details else None,
        )

        self._metrics[name] = metric
        return metric

    def as_dict(self) -> dict[str, dict[str, Any]]:
        """Return metrics as JSON-friendly dictionaries."""

        return {name: metric.to_payload() for name, metric in self._metrics.items()}

    def items(self) -> Iterable[tuple[str, QCMetric]]:
        """Iterate over registered metrics."""

        return self._metrics.items()

    def values(self) -> Iterable[QCMetric]:
        """Iterate over registered metric objects."""

        return self._metrics.values()

    def __contains__(self, item: str) -> bool:  # pragma: no cover - trivial
        return item in self._metrics

    def __len__(self) -> int:  # pragma: no cover - trivial
        return len(self._metrics)

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
        return {column: 0.0 for column in columns}

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
