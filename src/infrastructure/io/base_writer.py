"""Thin wrapper around deterministic dataset writes for pipelines."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any

import pandas as pd

from infrastructure.config.models.models import PipelineConfig

from .artifacts import RunArtifacts, WriteArtifacts, WriteResult
from .determinism import prepare_dataframe
from .writer import write_dataset_atomic

__all__ = ["BaseDatasetWriter"]


class BaseDatasetWriter:
    """Helper that encapsulates deterministic dataset emission."""

    def __init__(
        self,
        config: PipelineConfig,
        *,
        dataset_extension: str = "csv",
        encoding: str = "utf-8",
    ) -> None:
        self.config = config
        self.dataset_extension = dataset_extension
        self.encoding = encoding

    def write(
        self,
        df: pd.DataFrame,
        artifacts: RunArtifacts | WriteArtifacts | Path,
        *,
        mode: str | None = None,
        metadata: Mapping[str, Any] | None = None,
    ) -> WriteResult:
        """Materialise ``df`` into the dataset path described by ``artifacts``."""

        del mode, metadata  # Reserved for subclasses / future extensions
        dataset_path = self._resolve_dataset_path(artifacts)
        prepared = prepare_dataframe(df, config=self.config)
        write_dataset_atomic(prepared, dataset_path, config=self.config)
        return WriteResult(dataset=dataset_path)

    @staticmethod
    def _resolve_dataset_path(
        artifacts: RunArtifacts | WriteArtifacts | Path,
    ) -> Path:
        if isinstance(artifacts, RunArtifacts):
            return artifacts.write.dataset
        if isinstance(artifacts, WriteArtifacts):
            return artifacts.dataset
        if isinstance(artifacts, Path):
            return artifacts
        msg = f"Unsupported artifacts payload: {type(artifacts)!r}"
        raise TypeError(msg)
