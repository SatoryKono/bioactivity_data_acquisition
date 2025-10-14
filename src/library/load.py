"""Load stage utilities."""
from __future__ import annotations

import pandas as pd

from .config import OutputConfig


def write_outputs(frame: pd.DataFrame, output: OutputConfig) -> None:
    """Persist the transformed frame and QC artefacts."""
    if frame.empty:
        output.output_path.parent.mkdir(parents=True, exist_ok=True)
        frame.to_csv(output.output_path, index=False)
        return

    sorted_frame = frame.sort_values(
        by=["molecule_chembl_id", "assay_id"],
        kind="mergesort",
    )
    output.output_path.parent.mkdir(parents=True, exist_ok=True)
    sorted_frame.to_csv(output.output_path, index=False, encoding="utf-8")

    qc = _build_qc_report(sorted_frame)
    qc.to_csv(output.qc_report_path, index=False, encoding="utf-8")

    correlation = sorted_frame.select_dtypes(include="number").corr(numeric_only=True)
    correlation.to_csv(output.correlation_path, encoding="utf-8")


def _build_qc_report(frame: pd.DataFrame) -> pd.DataFrame:
    """Generate a QC report for ``frame``."""
    total_rows = len(frame)
    missing_counts = frame.isna().sum()
    duplicate_rows = int(frame.duplicated().sum())

    report = pd.DataFrame(
        {
            "metric": ["rows", "duplicates", "missing_values"],
            "value": [total_rows, duplicate_rows, int(missing_counts.sum())],
        }
    )
    return report
