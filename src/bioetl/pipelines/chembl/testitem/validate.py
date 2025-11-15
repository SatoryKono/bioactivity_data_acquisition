"""Validate stage wrapper for the ChEMBL test item pipeline."""

from __future__ import annotations

import pandas as pd

from bioetl.config.models.models import PipelineConfig

from .run import TestItemChemblPipeline

__all__ = ["validate"]


def validate(
    config: PipelineConfig,
    run_id: str,
    df: pd.DataFrame,
) -> pd.DataFrame:
    pipeline = TestItemChemblPipeline(config=config, run_id=run_id)
    return pipeline.validate(df)

