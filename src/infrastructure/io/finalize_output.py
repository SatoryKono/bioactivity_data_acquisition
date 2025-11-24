"""Application-level orchestration tying determinism and writer layers."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any, Tuple

import pandas as pd

from infrastructure.config.models.models import PipelineConfig

from .artifacts import WriteResult
from .determinism import DeterministicWriteArtifacts, build_write_artifacts
from .writer import write_dataset_atomic, write_yaml_atomic

__all__ = ["finalize_output"]


def finalize_output(
    df: pd.DataFrame,
    *,
    config: PipelineConfig,
    run_id: str,
    pipeline_code: str,
    dataset_path: Path,
    metadata_path: Path | None = None,
    stage_durations_ms: Mapping[str, float] | None = None,
) -> Tuple[DeterministicWriteArtifacts, WriteResult]:
    """Prepare deterministic artifacts and persist them to disk."""

    durations = stage_durations_ms or {}
    prepared = build_write_artifacts(
        df,
        config=config,
        run_id=run_id,
        pipeline_code=pipeline_code,
        dataset_path=dataset_path,
        stage_durations_ms=durations,
    )

    write_dataset_atomic(prepared.dataframe, dataset_path, config=config)

    metadata_written: Path | None = None
    if metadata_path:
        write_yaml_atomic(dict(prepared.metadata), metadata_path)
        metadata_written = metadata_path

    write_result = WriteResult(dataset=dataset_path, metadata=metadata_written)
    return prepared, write_result
