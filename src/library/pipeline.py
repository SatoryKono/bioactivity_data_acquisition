"""Orchestration of the ETL pipeline."""
from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from .config import PipelineConfig
from .extract import extract_all
from .load import write_outputs
from .logging import configure_logging
from .transform import normalise_units


def run_pipeline(config_path: Path, *, env_file: Path | None = None) -> pd.DataFrame:
    """Run the ETL pipeline using the configuration at ``config_path``."""
    config = PipelineConfig.from_file(config_path, env_file=env_file)
    configure_logging(getattr(logging, config.log_level.upper(), logging.INFO))

    extracted = extract_all(config.sources, strict=config.strict_validation)
    if extracted.empty:
        write_outputs(extracted, config.output)
        return extracted

    transformed = normalise_units(extracted, strict=config.strict_validation)
    write_outputs(transformed, config.output)
    return transformed
