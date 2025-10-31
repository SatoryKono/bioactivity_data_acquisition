"""Factories for ChEMBL pipeline output writers."""

from __future__ import annotations

from bioetl.config import PipelineConfig
from bioetl.config.models import DeterminismConfig
from bioetl.core.output_writer import UnifiedOutputWriter

__all__ = ["create_pipeline_output_writer"]


def create_pipeline_output_writer(
    run_id: str,
    *,
    determinism: DeterminismConfig,
    pipeline_config: PipelineConfig,
) -> UnifiedOutputWriter:
    """Instantiate a :class:`UnifiedOutputWriter` for ChEMBL pipelines."""

    return UnifiedOutputWriter(
        run_id,
        determinism=determinism,
        pipeline_config=pipeline_config,
    )
