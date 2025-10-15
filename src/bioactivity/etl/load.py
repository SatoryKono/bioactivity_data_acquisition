"""Loading stage utilities for the ETL pipeline."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
from structlog.stdlib import BoundLogger

from bioactivity.etl.qc import build_correlation_matrix, build_qc_report


def write_deterministic_csv(
    df: pd.DataFrame, destination: Path, logger: BoundLogger | None = None
) -> Path:
    """Persist data to CSV in a deterministic order."""

    destination.parent.mkdir(parents=True, exist_ok=True)
    if not df.empty:
        df_to_write = df.sort_values(df.columns.tolist()).reset_index(drop=True)
    else:
        df_to_write = df
    df_to_write.to_csv(destination, index=False, encoding="utf-8")
    if logger is not None:
        logger.info("load_complete", path=str(destination), rows=len(df_to_write))
    return destination


def write_qc_artifacts(df: pd.DataFrame, qc_path: Path, corr_path: Path) -> None:
    """Write QC and correlation reports to disk."""

    qc_report = build_qc_report(df)
    qc_path.parent.mkdir(parents=True, exist_ok=True)
    qc_report.to_csv(qc_path, index=False, encoding="utf-8")

    correlation = build_correlation_matrix(df)
    corr_path.parent.mkdir(parents=True, exist_ok=True)
    correlation.to_csv(corr_path, encoding="utf-8")


__all__ = ["write_deterministic_csv", "write_qc_artifacts"]
