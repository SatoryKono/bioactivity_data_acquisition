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
class RunState:
    """Хранилище промежуточного состояния запуска пайплайна."""

    durations: dict[str, int] = field(default_factory=dict)
    error: str | None = None
    artifacts: WriteArtifacts | None = None


@dataclass(frozen=True, slots=True)
class StageDescriptor:
    """Pure description of a pipeline stage independent of runtime deps."""

    id: str
    kind: str
    params: dict[str, Any]
    next: list[str]


@dataclass(slots=True)
class StageResult:
    """Result of executing a stage."""

    name: str
    output: Any = None


@dataclass(slots=True)
class StageRuntimeContext:
    """Runtime payload passed into instantiated stages."""

    context: "StageContext | None" = None
    options: StageExecutionOptions | None = None
    descriptor: StageDescriptor | None = None
    input_data: Any | None = None
    attributes: dict[str, Any] = field(default_factory=dict)


class DataBucket:
    """Mutable storage for the current in-flight dataframe."""

    def __init__(self) -> None:
        self._frame: pd.DataFrame | None = None

    def get(self) -> pd.DataFrame | None:
        return self._frame

    def set(self, frame: pd.DataFrame) -> None:
        self._frame = frame

    def require(self, *, stage: str | None = None) -> pd.DataFrame:
        if self._frame is None:
            msg = "Stage requires a DataFrame from a previous step"
            if stage:
                msg = f"Stage '{stage}' requires a DataFrame from a previous step"
            raise ValueError(msg)
        return self._frame

    def clear(self) -> None:
        self._frame = None


class ArtifactStore:
    """Container for WriteArtifacts shared across stages."""

    def __init__(self, artifacts: WriteArtifacts | None = None) -> None:
        self._artifacts = artifacts or WriteArtifacts()

    def get(self) -> WriteArtifacts:
        return self._artifacts

    def set(self, artifacts: WriteArtifacts) -> None:
        self._artifacts = artifacts


@runtime_checkable
class StageProtocol(Protocol):
    """Executable stage with deterministic contract."""

    name: str

    def execute(self, runtime_context: StageRuntimeContext) -> StageResult:
        ...


@dataclass(slots=True, frozen=True)
class StageCommand(StageProtocol):
    """Lightweight callable used to execute a pipeline stage."""

    name: str
    handler: Callable[["StageContextProtocol", StageRuntimeContext], Any]
    description: str | None = None

    def execute(self, runtime_context: StageRuntimeContext) -> StageResult:
        output = self.handler(runtime_context.context, runtime_context)
        if isinstance(output, StageResult):
            return output
        return StageResult(name=self.name, output=output)

    def validate(self) -> None:
        if not self.name:
            raise ValueError("Stage name must be provided")


# Backwards-compatible alias
PipelineStageCommand = StageCommand


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
        self, context: "StageContext", options: StageExecutionOptions
    ) -> tuple[StageDescriptor, ...]:
        ...

    def plan_run_artifacts(
        self, output_dir: Path, run_tag: str | None, mode: str | None
    ) -> tuple[Path, WriteArtifacts]:
        ...

    def build_run_metadata(
        self,
        context: "StageContext",
        stage_plan: Iterable[StageProtocol],
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
    pipeline: "PipelineBaseProtocol" | None
    data_bucket: DataBucket
    artifact_store: ArtifactStore

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
    pipeline: "PipelineBaseProtocol" | None = None
    clients: Mapping[str, Any] = field(default_factory=dict)
    config_provider: Callable[[str], Any] | None = None
    metric_emitter: Callable[[str, Any, Mapping[str, str] | None], None] | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    descriptor: Any | None = None
    output_dir: Path = field(default_factory=lambda: Path.cwd())
    data_bucket: DataBucket = field(default_factory=DataBucket)
    artifact_store: ArtifactStore = field(default_factory=ArtifactStore)

    def __post_init__(self) -> None:  # pragma: no cover - trivial
        if self.trace_id is None:
            self.trace_id = self.request_id

    def get_client(self, name: str) -> Any:
        return self.clients[name]

    def get_config(self, key: str) -> Any:
        if self.config_provider is None:
            msg = "Config provider is not configured"
            raise KeyError(msg)
        return self.config_provider(key)

    def emit_metric(self, name: str, value: Any, tags: Mapping[str, str] | None = None) -> None:
        if self.metric_emitter:
            self.metric_emitter(name, value, tags)


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
    "ArtifactStore",
    "DataBucket",
    "MaterializationConfig",
    "PipelineConfig",
    "PipelineExtractionMode",
    "PipelineInfo",
    "PipelineBaseProtocol",
    "PipelineStageCommand",
    "PipelineStagesProtocol",
    "StageCommand",
    "StageDescriptor",
    "StageProtocol",
    "StageResult",
    "StageRuntimeContext",
    "RunArtifacts",
    "RunResult",
    "RunState",
    "StageContext",
    "StageContextProtocol",
    "StageExecutionOptions",
    "WriteArtifacts",
    "WriteResult",
]
