"""Transform stage wrapper for the ChEMBL activity pipeline."""

from __future__ import annotations

import pandas as pd

from bioetl.config.models.models import PipelineConfig

from .run import ChemblActivityPipeline

__all__ = ["transform"]


def transform(
    config: PipelineConfig,
    run_id: str,
    df: pd.DataFrame,
) -> pd.DataFrame:
    """Apply the activity transform stage."""

    pipeline = ChemblActivityPipeline(config=config, run_id=run_id)
    return pipeline.transform(df)

