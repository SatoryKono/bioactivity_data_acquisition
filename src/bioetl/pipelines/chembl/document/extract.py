"""Extract stage wrappers for the ChEMBL document pipeline."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import pandas as pd

from bioetl.config.models.models import PipelineConfig

from .run import ChemblDocumentPipeline

__all__ = ["extract", "extract_all", "extract_by_ids"]


def _build_pipeline(config: PipelineConfig, run_id: str) -> ChemblDocumentPipeline:
    return ChemblDocumentPipeline(config=config, run_id=run_id)


def extract(
    config: PipelineConfig,
    run_id: str,
    *args: object,
    **kwargs: object,
) -> pd.DataFrame:
    """Execute extract stage with CLI overrides."""

    pipeline = _build_pipeline(config, run_id)
    return pipeline.extract(*args, **kwargs)


def extract_all(config: PipelineConfig, run_id: str) -> pd.DataFrame:
    """Extract the full document dataset."""

    pipeline = _build_pipeline(config, run_id)
    return pipeline.extract_all()


def extract_by_ids(
    config: PipelineConfig,
    run_id: str,
    ids: Sequence[str],
) -> pd.DataFrame:
    """Extract documents by ChEMBL identifiers."""

    pipeline = _build_pipeline(config, run_id)
    return pipeline.extract_by_ids(ids)

