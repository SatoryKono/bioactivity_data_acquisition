"""Shared QC assertions reused across pytest modules."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import pandas as pd

from bioetl.core.io.output import WriteResult

QUALITY_COLUMNS: tuple[str, ...] = (
    "section",
    "metric",
    "column",
    "value",
    "count",
    "ratio",
    "lower_bound",
    "upper_bound",
)

QC_PAYLOAD_REQUIRED_KEYS: tuple[str, ...] = (
    "row_count",
    "deduplicated_count",
    "duplicate_count",
    "duplicate_ratio",
    "total_missing_values",
    "columns_with_missing",
)


def assert_quality_report_structure(
    report: pd.DataFrame,
    *,
    required_sections: Sequence[str] | None = None,
) -> None:
    """Ensure the quality report dataframe exposes the canonical structure."""

    assert isinstance(report, pd.DataFrame), "quality report must be a DataFrame"
    assert not report.empty, "quality report must not be empty"

    missing_columns = [column for column in QUALITY_COLUMNS if column not in report.columns]
    assert not missing_columns, f"quality report missing columns: {missing_columns}"

    if required_sections:
        actual_sections = set(report["section"].unique())
        missing = sorted(set(required_sections) - actual_sections)
        assert not missing, f"quality report missing sections: {missing}"


def assert_qc_metrics_payload_structure(
    payload: Mapping[str, Any],
    *,
    required_keys: Iterable[str] | None = None,
) -> None:
    """Validate the QC metrics payload includes the canonical keys."""

    assert isinstance(payload, Mapping), "QC metrics payload must be a mapping"
    keys = set(payload.keys())
    base_required = set(QC_PAYLOAD_REQUIRED_KEYS)
    if required_keys is not None:
        base_required.update(required_keys)
    missing = sorted(base_required - keys)
    assert not missing, f"QC metrics payload missing keys: {missing}"


def assert_qc_artifact_set(
    write_result: WriteResult,
    *,
    expect_correlation: bool = False,
    expect_metrics: bool = False,
) -> None:
    """Ensure the expected QC artefact files exist on disk."""

    _assert_path_exists(write_result.dataset, "dataset")
    if write_result.metadata is not None:
        _assert_path_exists(write_result.metadata, "meta.yaml")
    if write_result.quality_report is not None:
        _assert_path_exists(write_result.quality_report, "quality_report")
    if expect_correlation:
        assert write_result.correlation_report is not None, "correlation report path missing"
        _assert_path_exists(write_result.correlation_report, "correlation_report")
    if expect_metrics:
        assert write_result.qc_metrics is not None, "qc_metrics path missing"
        _assert_path_exists(write_result.qc_metrics, "qc_metrics")


def _assert_path_exists(path: Path, label: str) -> None:
    assert path.exists(), f"{label} path does not exist: {path}"

