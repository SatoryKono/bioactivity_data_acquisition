"""Typed contracts for pipeline orchestration primitives."""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Protocol, runtime_checkable

import pandas as pd

from bioetl.core.logging import UnifiedLogger
from bioetl.core.io.artifacts import RunArtifacts, WriteArtifacts


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
class WriteResult:
    """Result of persisting transformed data."""

    rows: int
    artifacts: WriteArtifacts


@dataclass(slots=True)
class RunResult:
    """Summary of a pipeline execution."""

    success: bool
    rows: int
    artifacts: RunArtifacts
    duration_ms: dict[str, int]
    error: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def metrics(self) -> dict[str, Any]:
        """Backward-compatible alias for metadata payload."""

        return self.metadata


@dataclass(slots=True)
class PipelineStageCommand:
    """Lightweight callable used to execute a pipeline stage."""

    name: str
    handler: Callable[["StageContextProtocol", "StageRuntimeContext"], Any]
    description: str | None = None


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


@runtime_checkable
class PipelineBaseProtocol(PipelineStagesProtocol, Protocol):
    """Базовый контракт для конвейеров с полным жизненным циклом."""

    pipeline_code: str
    validator: Any | None

    def run(
        self,
        output_dir: Path,
        *,
        run_tag: str | None = None,
        mode: str | None = None,
        extended: bool = False,
        dry_run: bool | None = None,
        sample: int | None = None,
        limit: int | None = None,
        include_qc_metrics: bool = False,
        fail_on_schema_drift: bool = True,
    ) -> RunResult:
        ...

    def build_stage_plan(
        self, context: "StageContextProtocol", runtime: "StageRuntimeContext"
    ) -> tuple[PipelineStageCommand, ...]:
        ...

    def plan_run_artifacts(
        self, output_dir: Path, run_tag: str | None, mode: str | None
    ) -> tuple[Path, WriteArtifacts]:
        ...

    def build_run_metadata(
        self,
        context: "StageContextProtocol",
        runtime: "StageRuntimeContext",
        stage_plan: Iterable[PipelineStageCommand],
        durations: Mapping[str, int],
        run_tag: str | None,
        mode: str | None,
    ) -> dict[str, Any]:
        ...

    def resolve_logs_directory(self, output_dir: Path) -> Path:
        ...


@runtime_checkable
class StageContextProtocol(Protocol):
    """Stable contract for pipeline stage dependencies."""

    logger: UnifiedLogger
    request_id: str | None
    trace_id: str | None

    def get_client(self, name: str) -> Any:
        ...

    def get_config(self, key: str) -> Any:
        ...

    def emit_metric(self, name: str, value: Any, tags: Mapping[str, str] | None = None) -> None:
        ...


@dataclass(slots=True)
class StageContext(StageContextProtocol):
    """Default implementation of :class:`StageContextProtocol`."""

    logger: UnifiedLogger
    request_id: str | None
    trace_id: str | None = None
    clients: Mapping[str, Any] = field(default_factory=dict)
    config: Mapping[str, Any] = field(default_factory=dict)
    metric_emitter: Callable[[str, Any, Mapping[str, str] | None], None] | None = None

    def __post_init__(self) -> None:  # pragma: no cover - trivial
        if self.trace_id is None:
            self.trace_id = self.request_id

    def get_client(self, name: str) -> Any:
        return self.clients[name]

    def get_config(self, key: str) -> Any:
        return self.config[key]

    def emit_metric(self, name: str, value: Any, tags: Mapping[str, str] | None = None) -> None:
        if self.metric_emitter:
            self.metric_emitter(name, value, tags)


@dataclass(slots=True)
class StageRuntimeContext:
    """Mutable runtime context shared between stage handlers."""

    options: StageExecutionOptions
    input_data: Any | None = None
    attributes: dict[str, Any] = field(default_factory=dict)
    cancellation_token: Callable[[], bool] | None = None
    timeout: float | None = None


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


__all__ = [
    "MaterializationConfig",
    "PipelineConfig",
    "PipelineExtractionMode",
    "PipelineInfo",
    "PipelineBaseProtocol",
    "PipelineStageCommand",
    "PipelineStagesProtocol",
    "RunArtifacts",
    "RunResult",
    "StageContext",
    "StageContextProtocol",
    "StageExecutionOptions",
    "StageRuntimeContext",
    "WriteArtifacts",
    "WriteResult",
]
