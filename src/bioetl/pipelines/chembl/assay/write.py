"""Write stage wrapper for the ChEMBL assay pipeline."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from bioetl.config.models.models import PipelineConfig
from bioetl.core.pipeline import RunResult

from .run import ChemblAssayPipeline

__all__ = ["write"]


def write(
    config: PipelineConfig,
    run_id: str,
    df: pd.DataFrame,
    output_path: Path,
    **kwargs: Any,
) -> RunResult:
    """Materialize deterministic assay artifacts."""

    pipeline = ChemblAssayPipeline(config=config, run_id=run_id)
    return pipeline.write(df, output_path, **kwargs)

