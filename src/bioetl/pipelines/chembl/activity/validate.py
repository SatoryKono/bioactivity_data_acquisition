"""Validate stage wrapper for the ChEMBL activity pipeline."""

from __future__ import annotations

import pandas as pd

from bioetl.config.models.models import PipelineConfig

from .run import ChemblActivityPipeline

__all__ = ["validate"]


def validate(
    config: PipelineConfig,
    run_id: str,
    df: pd.DataFrame,
) -> pd.DataFrame:
    """Execute schema validation and QC hooks."""

    pipeline = ChemblActivityPipeline(config=config, run_id=run_id)
    return pipeline.validate(df)

