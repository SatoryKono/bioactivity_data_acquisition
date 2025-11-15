"""Extract stage wrappers for the ChEMBL activity pipeline."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import pandas as pd

from bioetl.config.models.models import PipelineConfig

from .run import ChemblActivityPipeline

__all__ = ["extract", "extract_all", "extract_by_ids"]


def _build_pipeline(config: PipelineConfig, run_id: str) -> ChemblActivityPipeline:
    """Instantiate the domain pipeline for the current stage."""

    return ChemblActivityPipeline(config=config, run_id=run_id)


def extract(
    config: PipelineConfig,
    run_id: str,
    *args: object,
    **kwargs: object,
) -> pd.DataFrame:
    """Run the configured extract stage (auto-routing between IDs/all)."""

    pipeline = _build_pipeline(config, run_id)
    return pipeline.extract(*args, **kwargs)


def extract_all(config: PipelineConfig, run_id: str) -> pd.DataFrame:
    """Extract the full activity dataset using pagination."""

    pipeline = _build_pipeline(config, run_id)
    return pipeline.extract_all()


def extract_by_ids(
    config: PipelineConfig,
    run_id: str,
    ids: Sequence[str],
) -> pd.DataFrame:
    """Extract a targeted slice of activities by identifiers."""

    pipeline = _build_pipeline(config, run_id)
    return pipeline.extract_by_ids(ids)

