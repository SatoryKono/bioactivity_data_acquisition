"""Extract stage wrappers for the ChEMBL target pipeline."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import pandas as pd

from bioetl.config.models.models import PipelineConfig

from .run import ChemblTargetPipeline

__all__ = ["extract", "extract_all", "extract_by_ids"]


def _build_pipeline(config: PipelineConfig, run_id: str) -> ChemblTargetPipeline:
    return ChemblTargetPipeline(config=config, run_id=run_id)


def extract(
    config: PipelineConfig,
    run_id: str,
    *args: object,
    **kwargs: object,
) -> pd.DataFrame:
    pipeline = _build_pipeline(config, run_id)
    return pipeline.extract(*args, **kwargs)


def extract_all(config: PipelineConfig, run_id: str) -> pd.DataFrame:
    pipeline = _build_pipeline(config, run_id)
    return pipeline.extract_all()


def extract_by_ids(
    config: PipelineConfig,
    run_id: str,
    ids: Sequence[str],
) -> pd.DataFrame:
    pipeline = _build_pipeline(config, run_id)
    return pipeline.extract_by_ids(ids)

