"""Loading stage utilities for the ETL pipeline."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd
from structlog.stdlib import BoundLogger

from bioactivity.etl.qc import build_correlation_matrix, build_qc_report

if TYPE_CHECKING:  # pragma: no cover - type checking helpers
    from bioactivity.config import (
        CsvFormatSettings,
        DeterminismSettings,
        OutputSettings,
        ParquetFormatSettings,
        PostprocessSettings,
        QCValidationSettings,
    )


def _deterministic_order(
    df: pd.DataFrame,
    determinism: DeterminismSettings,
) -> pd.DataFrame:
    desired_order = [col for col in determinism.column_order if col in df.columns]
    remaining = [col for col in df.columns if col not in desired_order]
    ordered = df[desired_order + remaining]
    sort_by = determinism.sort.by or ordered.columns.tolist()
    from bioactivity.etl.transform import _resolve_ascending

    ascending = _resolve_ascending(sort_by, determinism.sort.ascending)
    return ordered.sort_values(
        sort_by,
        ascending=ascending,
        na_position=determinism.sort.na_position,
    ).reset_index(drop=True)


def _csv_options(settings: CsvFormatSettings) -> dict[str, object]:
    options: dict[str, object] = {
        "index": False,
        "encoding": settings.encoding,
    }
    if settings.float_format is not None:
        options["float_format"] = settings.float_format
    if settings.date_format is not None:
        options["date_format"] = settings.date_format
    if settings.na_rep is not None:
        options["na_rep"] = settings.na_rep
    if settings.line_terminator is not None:
        options["line_terminator"] = settings.line_terminator
    return options


def _apply_qc_thresholds(
    qc_report: pd.DataFrame,
    df: pd.DataFrame,
    validation: QCValidationSettings,
) -> pd.DataFrame:
    row_count = len(df)
    thresholds: list[float | None] = []
    ratios: list[float | None] = []
    statuses: list[str] = []

    for metric, value in zip(qc_report["metric"], qc_report["value"]):
        threshold: float | None = None
        ratio: float | None = None
        status = "pass"
        numeric_value = float(value)

        if metric in {"missing_compound_id", "missing_target"}:
            threshold = validation.max_missing_fraction
            ratio = numeric_value / row_count if row_count else 0.0
            if ratio > threshold:
                status = "fail"
        elif metric == "duplicates":
            threshold = validation.max_duplicate_fraction
            ratio = numeric_value / row_count if row_count else 0.0
            if ratio > threshold:
                status = "fail"

        thresholds.append(threshold)
        ratios.append(ratio)
        statuses.append(status)

    qc_with_thresholds = qc_report.assign(
        threshold=thresholds,
        ratio=ratios,
        status=statuses,
    )

    summary_status = (
        "fail"
        if any(
            status == "fail" and threshold is not None
            for status, threshold in zip(statuses, thresholds)
        )
        else "pass"
    )
    summary = pd.DataFrame(
        [
            {
                "metric": "qc_passed",
                "value": summary_status == "pass",
                "threshold": None,
                "ratio": None,
                "status": summary_status,
            }
        ]
    )
    return pd.concat([qc_with_thresholds, summary], ignore_index=True)


def write_deterministic_csv(
    df: pd.DataFrame,
    destination: Path,
    logger: BoundLogger | None = None,
    *,
    determinism: DeterminismSettings | None = None,
    output: OutputSettings | None = None,
) -> Path:
    """Persist data to disk in a deterministic order using configuration."""

    from bioactivity.config import (
        CsvFormatSettings as _CsvFormatSettings,
        DeterminismSettings as _DeterminismSettings,
        ParquetFormatSettings as _ParquetFormatSettings,
    )

    determinism = determinism or _DeterminismSettings()
    csv_settings: CsvFormatSettings
    parquet_settings: ParquetFormatSettings
    file_format = "csv"

    if output is not None:
        csv_settings = output.csv
        parquet_settings = output.parquet
        file_format = output.format
    else:  # pragma: no cover - fallback for direct usage
        csv_settings = _CsvFormatSettings()
        parquet_settings = _ParquetFormatSettings()

    destination.parent.mkdir(parents=True, exist_ok=True)

    if df.empty:
        df_to_write = df.copy()
    else:
        df_to_write = _deterministic_order(df, determinism)

    if file_format == "parquet":
        df_to_write.to_parquet(destination, index=False, compression=parquet_settings.compression)
    else:
        options = _csv_options(csv_settings)
        df_to_write.to_csv(destination, **options)

    if logger is not None:
        logger.info("load_complete", path=str(destination), rows=len(df_to_write))
    return destination


def write_qc_artifacts(
    df: pd.DataFrame,
    qc_path: Path,
    corr_path: Path,
    *,
    output: OutputSettings | None = None,
    validation: QCValidationSettings | None = None,
    postprocess: PostprocessSettings | None = None,
) -> None:
    """Write QC and correlation reports according to configuration."""

    from bioactivity.config import (
        CsvFormatSettings as _CsvFormatSettings,
        ParquetFormatSettings as _ParquetFormatSettings,
        PostprocessSettings as _PostprocessSettings,
        QCValidationSettings as _QCValidationSettings,
    )

    csv_settings: CsvFormatSettings
    parquet_settings: ParquetFormatSettings
    file_format = "csv"

    if output is not None:
        csv_settings = output.csv
        parquet_settings = output.parquet
        file_format = output.format
    else:  # pragma: no cover - fallback for direct usage
        csv_settings = _CsvFormatSettings()
        parquet_settings = _ParquetFormatSettings()

    validation = validation or _QCValidationSettings()
    postprocess = postprocess or _PostprocessSettings()

    if not postprocess.qc.enabled:
        return

    qc_report = build_qc_report(df)
    qc_report = _apply_qc_thresholds(qc_report, df, validation)
    qc_path.parent.mkdir(parents=True, exist_ok=True)

    if file_format == "parquet":
        qc_report.to_parquet(qc_path, index=False, compression=parquet_settings.compression)
    else:
        qc_report.to_csv(qc_path, **_csv_options(csv_settings))

    if not postprocess.correlation.enabled:
        return

    correlation = build_correlation_matrix(df)
    corr_path.parent.mkdir(parents=True, exist_ok=True)
    if file_format == "parquet":
        correlation.to_parquet(corr_path, index=False, compression=parquet_settings.compression)
    else:
        correlation.to_csv(corr_path, **_csv_options(csv_settings))


__all__ = ["write_deterministic_csv", "write_qc_artifacts"]
