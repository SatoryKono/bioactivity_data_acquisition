"""Write stage wrapper for the ChEMBL activity pipeline."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from bioetl.config.models.models import PipelineConfig
from bioetl.core.pipeline import RunResult

from .run import ChemblActivityPipeline

__all__ = ["write"]


def write(
    config: PipelineConfig,
    run_id: str,
    df: pd.DataFrame,
    output_path: Path,
    **kwargs: Any,
) -> RunResult:
    """Materialize deterministic outputs for the activity pipeline."""

    pipeline = ChemblActivityPipeline(config=config, run_id=run_id)
    return pipeline.write(df, output_path, **kwargs)

