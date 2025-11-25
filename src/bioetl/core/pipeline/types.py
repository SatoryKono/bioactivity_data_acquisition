"""Typed contracts for pipeline orchestration primitives."""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Protocol, runtime_checkable

import pandas as pd

from bioetl.core.logging import UnifiedLogger


class PipelineExtractionMode(str, Enum):
    """Enumeration describing the strategy for fetching data."""

    FULL = "full"
    SAMPLE = "sample"
    INCREMENTAL = "incremental"


@dataclass(slots=True)
class StageExecutionOptions:
    """Runtime options that are shared across pipeline stages."""

    run_tag: str | None
    mode: str | None
    extended: bool = False
    dry_run: bool = False
    sample: int | None = None
    limit: int | None = None
    include_qc_metrics: bool = False
    fail_on_schema_drift: bool = True
    extraction_mode: PipelineExtractionMode = PipelineExtractionMode.FULL


@dataclass(slots=True)
class WriteArtifacts:
    """Paths produced by the ``save_results`` stage."""

    data_path: Path | None = None
    manifest_path: Path | None = None
    extra: dict[str, Path] = field(default_factory=dict)


@dataclass(slots=True)
class WriteResult:
    """Result of persisting transformed data."""

    rows: int
    artifacts: WriteArtifacts


@dataclass(slots=True)
class RunArtifacts:
    """Top-level artifacts describing the executed run."""

    output_dir: Path
    logs_directory: Path
    write_artifacts: WriteArtifacts | None = None
    qc_metrics_path: Path | None = None


@dataclass(slots=True)
class RunResult:
    """Summary of a pipeline execution."""

    success: bool
    rows: int
    artifacts: RunArtifacts
    duration_ms: dict[str, int]
    error: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class PipelineStageCommand:
    """Lightweight callable used to execute a pipeline stage."""

    name: str
    handler: Callable[["StageContext", StageExecutionOptions], Any]
    description: str | None = None


@dataclass(slots=True)
class StageContext:
    """Shared context passed to :class:`PipelineStageCommand` handlers."""

    pipeline: "PipelineStagesProtocol"
    output_dir: Path
    logger: UnifiedLogger
    run_id: str
    run_tag: str | None
    mode: str | None
    descriptor: Any | None = None
    artifacts: WriteArtifacts | None = None
    current_df: pd.DataFrame | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class PipelineInfo:
    """Minimal metadata required for deterministic output planning."""

    name: str


@dataclass(slots=True)
class MaterializationConfig:
    """Configuration describing where pipeline outputs are stored."""

    root: Path


@dataclass(slots=True)
class PipelineConfig:
    """Pipeline configuration passed to orchestration primitives."""

    pipeline: PipelineInfo
    materialization: MaterializationConfig


@runtime_checkable
class PipelineStagesProtocol(Protocol):
    """Protocol describing the default ETL stages."""

    def prepare_run(self, options: StageExecutionOptions) -> None:
        ...

    def extract(self, descriptor: Any, options: StageExecutionOptions) -> pd.DataFrame:
        ...

    def transform(self, df: pd.DataFrame, options: StageExecutionOptions) -> pd.DataFrame:
        ...

    def validate(self, df: pd.DataFrame, options: StageExecutionOptions) -> pd.DataFrame:
        ...

    def save_results(
        self, df: pd.DataFrame, artifacts: WriteArtifacts, options: StageExecutionOptions
    ) -> WriteResult:
        ...

    def finalize_run(self, run_result: RunResult) -> None:
        ...


__all__ = [
    "MaterializationConfig",
    "PipelineConfig",
    "PipelineExtractionMode",
    "PipelineInfo",
    "PipelineStageCommand",
    "PipelineStagesProtocol",
    "RunArtifacts",
    "RunResult",
    "StageContext",
    "StageExecutionOptions",
    "WriteArtifacts",
    "WriteResult",
]
