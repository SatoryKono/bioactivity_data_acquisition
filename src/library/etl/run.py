"""End-to-end orchestration of the ETL pipeline."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
from structlog.stdlib import BoundLogger

from library.config import Config
from library.etl.extract import fetch_bioactivity_data
from library.etl.load import write_deterministic_csv, write_qc_artifacts
from library.etl.transform import normalize_bioactivity_data
from library.schemas import NormalizedBioactivitySchema


def _empty_normalized_frame() -> pd.DataFrame:
    schema = NormalizedBioactivitySchema.to_schema()
    empty = schema.empty_dataframe()
    return schema.validate(empty, lazy=True)  # type: ignore


def run_pipeline(config: Config, logger: BoundLogger) -> Path:
    """Execute the ETL pipeline using the provided configuration."""

    frames: list[pd.DataFrame] = []
    for client in config.clients:
        stage_logger = logger.bind(source=client.name)
        raw_frame = fetch_bioactivity_data(client, logger=stage_logger)
        normalized = normalize_bioactivity_data(
            raw_frame,
            transforms=config.transforms,
            determinism=config.determinism,
            logger=stage_logger,
        )
        frames.append(normalized)

    if frames:
        combined = pd.concat(frames, ignore_index=True)
    else:
        combined = _empty_normalized_frame()

    output_settings = config.io.output
    output_path = output_settings.data_path
    qc_path = output_settings.qc_report_path
    corr_path = output_settings.correlation_path

    write_deterministic_csv(
        combined,
        output_path,
        logger=logger,
        determinism=config.determinism,
        output=output_settings,
    )
    write_qc_artifacts(
        combined,
        qc_path,
        corr_path,
        output=output_settings,
        validation=config.validation.qc,
        postprocess=config.postprocess,
    )

    return Path(output_path)


__all__ = ["run_pipeline"]
