"""Transform stage wrapper for the ChEMBL document pipeline."""

from __future__ import annotations

import pandas as pd

from bioetl.config.models.models import PipelineConfig

from .run import ChemblDocumentPipeline

__all__ = ["transform"]


def transform(
    config: PipelineConfig,
    run_id: str,
    df: pd.DataFrame,
) -> pd.DataFrame:
    """Execute document-specific transformation logic."""

    pipeline = ChemblDocumentPipeline(config=config, run_id=run_id)
    return pipeline.transform(df)

