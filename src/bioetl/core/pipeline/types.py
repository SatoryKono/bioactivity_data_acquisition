"""Typed contracts for pipeline orchestration primitives."""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import (
    Any,
    Callable,
    Iterable,
    Mapping,
    Protocol,
    TYPE_CHECKING,
    runtime_checkable,
)

import pandas as pd

from bioetl.core.logging import UnifiedLogger
from bioetl.core.io.artifacts import RunArtifacts, WriteArtifacts

if TYPE_CHECKING:  # pragma: no cover
    from bioetl.core.pipeline.unified import ChemblExtractionDescriptor


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

    context: "StageContextProtocol | None" = None
    options: StageExecutionOptions | None = None
    descriptor: StageDescriptor | None = None
    input_data: Any | None = None
    attributes: dict[str, Any] = field(default_factory=dict)


class DataBucket:
    """Mutable storage for the current in-flight dataframe."""

    def __init__(self) -> None:
        self._frame: pd.DataFrame | None = None

    def get(self) -> pd.DataFrame | None:
        """Retrieve the current dataframe."""
        return self._frame

    def set(self, frame: pd.DataFrame) -> None:
        """Update the current dataframe."""
        self._frame = frame

    def require(self, *, stage: str | None = None) -> pd.DataFrame:
        """Return the current dataframe or raise if empty."""
        if self._frame is None:
            msg = "Stage requires a DataFrame from a previous step"
            if stage:
                msg = (
                    f"Stage '{stage}' requires a DataFrame "
                    "from a previous step"
                )
            raise ValueError(msg)
        return self._frame

    def clear(self) -> None:
        """Clear the current dataframe."""
        self._frame = None


class ArtifactStore:
    """Container for WriteArtifacts shared across stages."""

    def __init__(self, artifacts: WriteArtifacts | None = None) -> None:
        self._artifacts = artifacts or WriteArtifacts()

    def get(self) -> WriteArtifacts:
        """Retrieve the current artifacts."""
        return self._artifacts

    def set(self, artifacts: WriteArtifacts) -> None:
        """Update the current artifacts."""
        self._artifacts = artifacts


@runtime_checkable
class StageProtocol(Protocol):
    """Executable stage with deterministic contract."""

    name: str

    def execute(self, runtime_context: StageRuntimeContext) -> StageResult:
        """Execute the stage logic."""
        ...


@dataclass(slots=True, frozen=True)
class StageCommand(StageProtocol):
    """Lightweight callable used to execute a pipeline stage."""

    name: str
    handler: Callable[["StageContextProtocol", StageRuntimeContext], Any]
    description: str | None = None

    def execute(self, runtime_context: StageRuntimeContext) -> StageResult:
        """Execute the wrapped handler."""
        output = self.handler(runtime_context.context, runtime_context)
        if isinstance(output, StageResult):
            return output
        return StageResult(name=self.name, output=output)

    def validate(self) -> None:
        """Validate the command configuration."""
        if not self.name:
            raise ValueError("Stage name must be provided")


# Backwards-compatible alias
PipelineStageCommand = StageCommand


@runtime_checkable
class PipelineStagesProtocol(Protocol):
    """Protocol describing the default ETL stages."""

    def prepare_run(self, options: StageExecutionOptions) -> None:
        """Prepare the run environment."""
        ...

    def extract(
        self, descriptor: "ChemblExtractionDescriptor | None", options: StageExecutionOptions
    ) -> pd.DataFrame:
        """Extract data from source."""
        ...

    def transform(
        self, df: pd.DataFrame, options: StageExecutionOptions
    ) -> pd.DataFrame:
        """Transform the extracted data."""
        ...

    def validate(
        self, df: pd.DataFrame, options: StageExecutionOptions
    ) -> pd.DataFrame:
        """Validate the transformed data."""
        ...

    def save_results(
        self,
        df: pd.DataFrame,
        artifacts: WriteArtifacts,
        options: StageExecutionOptions,
    ) -> WriteResult:
        """Persist the results."""
        ...

    def finalize_run(self, run_result: RunResult) -> None:
        """Cleanup and finalize the run."""
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
        """Execute the full pipeline lifecycle."""
        ...

    def build_stage_plan(
        self, context: "StageContext", options: StageExecutionOptions
    ) -> tuple[StageDescriptor, ...]:
        """Create the plan of stages to execute."""
        ...

    def plan_run_artifacts(
        self, output_dir: Path, run_tag: str | None, mode: str | None
    ) -> tuple[Path, WriteArtifacts]:
        """Determine output paths and artifact locations."""
        ...

    def build_run_metadata(
        self,
        context: "StageContext",
        stage_plan: Iterable[StageProtocol],
        durations: Mapping[str, int],
        run_tag: str | None,
        mode: str | None,
    ) -> dict[str, Any]:
        """Collect metadata about the completed run."""
        ...

    def resolve_logs_directory(self, output_dir: Path) -> Path:
        """Determine the directory for log files."""
        ...


@runtime_checkable
class ExecutionContext(Protocol):
    """Execution metadata such as logging and trace identifiers."""

    logger: UnifiedLogger
    request_id: str | None
    trace_id: str | None


@runtime_checkable
class ConfigContext(Protocol):
    """Configuration access contract."""

    def get_config(self, key: str) -> Any:
        ...


@runtime_checkable
class ClientContext(Protocol):
    """Lookup contract for external clients."""

    def get_client(self, name: str) -> Any:
        ...


@runtime_checkable
class DataContext(Protocol):
    """Access to data and artifact stores shared between stages."""

    data_bucket: DataBucket
    artifact_store: ArtifactStore


@runtime_checkable
class MetricsContext(Protocol):
    """Metrics emission contract."""

    def emit_metric(
        self, name: str, value: Any, tags: Mapping[str, str] | None = None
    ) -> None:
        ...


@runtime_checkable
class StageFactoryContext(DataContext, Protocol):
    """Minimal contract required to execute built stage descriptors."""

    descriptor: Any | None


@runtime_checkable
class StageContextProtocol(
    ExecutionContext,
    ConfigContext,
    ClientContext,
    MetricsContext,
    StageFactoryContext,
    Protocol,
):
    """Stable contract for pipeline stage dependencies."""

    pipeline: "PipelineBaseProtocol" | None
    metadata: dict[str, Any]
    output_dir: Path
    metadata_service: Any | None
    qc_orchestrator: Any | None

    @property
    def current_df(self) -> pd.DataFrame | None:
        ...

    @current_df.setter
    def current_df(self, value: pd.DataFrame) -> None:
        ...


@dataclass(slots=True)
class DefaultExecutionContext(ExecutionContext):
    """Default implementation for execution context."""

    logger: UnifiedLogger
    request_id: str | None
    trace_id: str | None = None

    def __post_init__(self) -> None:  # pragma: no cover - trivial
        if self.trace_id is None:
            self.trace_id = self.request_id


@dataclass(slots=True)
class DefaultConfigContext(ConfigContext):
    """Config provider-based context."""

    config_provider: Callable[[str], Any] | None = None

    def get_config(self, key: str) -> Any:
        if self.config_provider is None:
            msg = "Config provider is not configured"
            raise KeyError(msg)
        return self.config_provider(key)


@dataclass(slots=True)
class DefaultClientContext(ClientContext):
    """Simple registry-based client context."""

    clients: Mapping[str, Any] = field(default_factory=dict)

    def get_client(self, name: str) -> Any:
        return self.clients[name]


@dataclass(slots=True)
class DefaultDataContext(DataContext):
    """In-memory data and artifact context."""

    data_bucket: DataBucket = field(default_factory=DataBucket)
    artifact_store: ArtifactStore = field(default_factory=ArtifactStore)


@dataclass(slots=True)
class DefaultMetricsContext(MetricsContext):
    """Callable-based metrics context."""

    metric_emitter: (
        Callable[[str, Any, Mapping[str, str] | None], None] | None
    ) = None

    def emit_metric(
        self, name: str, value: Any, tags: Mapping[str, str] | None = None
    ) -> None:
        """Emit a metric using the configured emitter."""
        if self.metric_emitter:
            self.metric_emitter(name, value, tags)


@dataclass(slots=True)
class StageContextAdapter:
    """Composable adapter that delegates to specialized contexts."""

    execution: ExecutionContext
    data: DataContext
    config: ConfigContext | None = None
    clients: ClientContext | None = None
    metrics: MetricsContext | None = None
    pipeline: "PipelineBaseProtocol" | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    descriptor: Any | None = None
    output_dir: Path = field(default_factory=Path.cwd)
    metadata_service: Any | None = None
    qc_orchestrator: Any | None = None

    @property
    def logger(self) -> UnifiedLogger:
        return self.execution.logger

    @property
    def request_id(self) -> str | None:
        return self.execution.request_id

    @property
    def trace_id(self) -> str | None:
        return self.execution.trace_id

    @property
    def data_bucket(self) -> DataBucket:
        return self.data.data_bucket

    @property
    def artifact_store(self) -> ArtifactStore:
        return self.data.artifact_store

    @property
    def current_df(self) -> pd.DataFrame | None:
        return self.data_bucket.get()

    @current_df.setter
    def current_df(self, value: pd.DataFrame) -> None:
        self.data_bucket.set(value)

    def get_config(self, key: str) -> Any:
        if self.config is None:
            msg = "Config provider is not configured"
            raise KeyError(msg)
        return self.config.get_config(key)

    def get_client(self, name: str) -> Any:
        if self.clients is None:
            msg = "Client registry is not configured"
            raise KeyError(msg)
        return self.clients.get_client(name)

    def emit_metric(
        self, name: str, value: Any, tags: Mapping[str, str] | None = None
    ) -> None:
        if self.metrics:
            self.metrics.emit_metric(name, value, tags)


@dataclass(slots=True)
class StageContext(StageContextAdapter):
    """Default implementation of :class:`StageContextProtocol`."""

    def __init__(
        self,
        *,
        logger: UnifiedLogger,
        request_id: str | None,
        trace_id: str | None = None,
        pipeline: "PipelineBaseProtocol" | None = None,
        clients: Mapping[str, Any] | None = None,
        config_provider: Callable[[str], Any] | None = None,
        metric_emitter: (
            Callable[[str, Any, Mapping[str, str] | None], None] | None
        ) = None,
        metadata: dict[str, Any] | None = None,
        descriptor: Any | None = None,
        output_dir: Path | None = None,
        data_bucket: DataBucket | None = None,
        artifact_store: ArtifactStore | None = None,
        metadata_service: Any | None = None,
        qc_orchestrator: Any | None = None,
    ) -> None:
        execution = DefaultExecutionContext(
            logger=logger, request_id=request_id, trace_id=trace_id
        )
        data_context = DefaultDataContext(
            data_bucket=data_bucket or DataBucket(),
            artifact_store=artifact_store or ArtifactStore(),
        )
        config_context = DefaultConfigContext(config_provider)
        client_context = DefaultClientContext(clients or {})
        metrics_context = DefaultMetricsContext(metric_emitter)

        StageContextAdapter.__init__(
            self,
            execution=execution,
            data=data_context,
            config=config_context,
            clients=client_context,
            metrics=metrics_context,
            pipeline=pipeline,
            metadata=metadata or {},
            descriptor=descriptor,
            output_dir=output_dir or Path.cwd(),
            metadata_service=metadata_service,
            qc_orchestrator=qc_orchestrator,
        )


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
    "ExecutionContext",
    "ConfigContext",
    "ClientContext",
    "DataContext",
    "MetricsContext",
    "StageFactoryContext",
    "DefaultExecutionContext",
    "DefaultConfigContext",
    "DefaultClientContext",
    "DefaultDataContext",
    "DefaultMetricsContext",
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
    "StageContextAdapter",
    "StageContext",
    "StageContextProtocol",
    "StageExecutionOptions",
    "WriteArtifacts",
    "WriteResult",
]
