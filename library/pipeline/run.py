"""End-to-end orchestration of the ETL pipeline."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
from structlog.stdlib import BoundLogger

from ..config import Config
from .extract import fetch_bioactivity_data
from .load import write_deterministic_csv, write_qc_artifacts
from .transform import create_empty_normalized_frame, normalize_bioactivity_data


def _empty_normalized_frame() -> pd.DataFrame:
    return create_empty_normalized_frame()


def run_pipeline(config: Config, logger: BoundLogger) -> Path:
    """Execute the ETL pipeline using the provided configuration."""

    frames: list[pd.DataFrame] = []
    for client in config.clients:
        stage_logger = logger.bind(source=client.name)
        raw_frame = fetch_bioactivity_data(client, config.retries, logger=stage_logger)
        normalized = normalize_bioactivity_data(raw_frame, logger=stage_logger)
        frames.append(normalized)

    if frames:
        combined = pd.concat(frames, ignore_index=True)
    else:
        combined = _empty_normalized_frame()

    output_path = config.output.data_path
    qc_path = config.output.qc_report_path
    corr_path = config.output.correlation_path

    write_deterministic_csv(combined, output_path, logger=logger)
    write_qc_artifacts(combined, qc_path, corr_path)

    return output_path


__all__ = ["run_pipeline"]
