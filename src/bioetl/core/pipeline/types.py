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
    TypedDict,
    Type,
    runtime_checkable,
)

import pandas as pd

from bioetl.clients.chembl.entities import (
    ChemblEntity,
    ChemblEntityClientFactory,
)
from bioetl.clients.enrichers.factory import (
    EnricherClientFactory,
    EnricherEntity,
)
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
    enable_validation: bool = True
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


@dataclass(slots=True, frozen=True)
class StageCommand(StageProtocol):
    """Lightweight callable used to execute a pipeline stage."""

    name: str
    handler: Callable[
        ["StageContextProtocol | None", StageRuntimeContext],
        Any,
    ]
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

    def extract(
        self,
        descriptor: Any,
        options: StageExecutionOptions,
    ) -> pd.DataFrame:
        """Extract data from source."""

    def transform(
        self, df: pd.DataFrame, options: StageExecutionOptions
    ) -> pd.DataFrame:
        """Transform the extracted data."""

    def validate(
        self, df: pd.DataFrame, options: StageExecutionOptions
    ) -> pd.DataFrame:
        """Validate the transformed data."""

    def save_results(
        self,
        df: pd.DataFrame,
        artifacts: WriteArtifacts,
        options: StageExecutionOptions,
    ) -> WriteResult:
        """Persist the results."""

    def finalize_run(self, run_result: RunResult) -> None:
        """Cleanup and finalize the run."""


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

    def build_stage_plan(
        self, context: "StageContext", options: StageExecutionOptions
    ) -> tuple[StageDescriptor, ...]:
        """Create the plan of stages to execute."""

    def plan_run_artifacts(
        self, output_dir: Path, run_tag: str | None, mode: str | None
    ) -> tuple[Path, WriteArtifacts]:
        """Determine output paths and artifact locations."""

    def build_run_metadata(
        self,
        context: "StageContext",
        stage_plan: Iterable[StageProtocol],
        durations: Mapping[str, int],
        run_tag: str | None,
        mode: str | None,
    ) -> dict[str, Any]:
        """Collect metadata about the completed run."""

    def resolve_logs_directory(self, output_dir: Path) -> Path:
        """Determine the directory for log files."""


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
        """Retrieve a configuration value by key."""


@runtime_checkable
class ClientContext(Protocol):
    """Lookup contract for external clients grouped by namespace."""

    def get_client(
        self,
        namespace: ClientNamespace | str,
        entity: Any | None = None,
    ) -> Any:
        """Retrieve a client instance by namespace and entity."""


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
        """Emit a metric point."""


@runtime_checkable
class StageFactoryContext(DataContext, Protocol):
    """Minimal contract required to execute built stage descriptors."""

    descriptor: Any | None


@runtime_checkable
class DomainContext(Protocol):
    """Domain-level metadata and pipeline context."""

    pipeline: "PipelineBaseProtocol" | None
    metadata: dict[str, Any]
    descriptor: Any | None


@runtime_checkable
class InfrastructureContext(Protocol):
    """Infrastructure dependencies shared between stages."""

    output_dir: Path
    metadata_service: Any | None
    qc_orchestrator: Any | None


@runtime_checkable
class ArtifactContext(DataContext, Protocol):
    """Access to artifacts and intermediate data."""

    artifact_store: ArtifactStore


@runtime_checkable
class StageContextProtocol(
    ExecutionContext,
    ConfigContext,
    ClientContext,
    MetricsContext,
    StageFactoryContext,
    DomainContext,
    InfrastructureContext,
    ArtifactContext,
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
        """Get the current dataframe from the bucket."""

    @current_df.setter
    def current_df(self, value: pd.DataFrame) -> None:
        """Set the current dataframe in the bucket."""


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


class ClientNamespace(str, Enum):
    """Supported client namespaces."""

    CHEMBL = "chembl"
    ENRICHER = "enricher"


class ClientFactoryRegistry(TypedDict, total=False):
    """Typed mapping between namespace and a factory implementation."""

    chembl: ChemblEntityClientFactory
    enricher: EnricherClientFactory


class LegacyClientLookupAdapter:
    """Adapter converting legacy names like ``chembl:activity`` to pairs."""

    def __call__(self, name: str) -> tuple[ClientNamespace, str]:
        for separator in (":", "."):
            if separator in name:
                namespace, entity = name.split(separator, 1)
                try:
                    return ClientNamespace(namespace), entity
                except ValueError as err:
                    msg = f"Namespace '{namespace}' is not supported"
                    raise KeyError(msg) from err
        msg = f"Client '{name}' is not registered"
        raise KeyError(msg)

    def resolve(
        self,
        namespace: ClientNamespace | str,
        entity: Enum | str | None,
    ) -> tuple[ClientNamespace, Enum | str]:
        """Resolve the namespace and entity from legacy or new arguments."""
        if entity is None:
            if not isinstance(namespace, str):
                msg = (
                    "Entity must be provided when namespace is not a string"
                )
                raise KeyError(msg)
            return self(namespace)
        else:
            resolved_namespace = (
                namespace
                if isinstance(namespace, ClientNamespace)
                else ClientNamespace(namespace)
            )
            entity_value = entity
        return resolved_namespace, entity_value


@dataclass(slots=True)
class ClientRegistryContext(ClientContext):
    """Client context backed by :class:`ClientRegistry`."""

    registry: ClientRegistry = field(
        default_factory=lambda: ClientRegistry({})
    )
    adapter: LegacyClientLookupAdapter = field(
        default_factory=LegacyClientLookupAdapter
    )

    def get_client(
        self,
        namespace: str,
        entity: Any | None = None,
    ) -> Any:
        """Retrieve a client instance by namespace and entity."""
        resolved_namespace, resolved_entity = self.adapter.resolve(
            namespace,
            entity,
        )
        return self.registry.get(resolved_namespace, resolved_entity)


@dataclass(slots=True)
class ClientRegistry(ClientContext):
    """Registry resolving clients by namespace and entity."""

    factories: Mapping[str, Any] = field(default_factory=dict)

    _entity_validators: Mapping[ClientNamespace, Type[Enum]] = field(
        default_factory=lambda: {
            ClientNamespace.CHEMBL: ChemblEntity,
            ClientNamespace.ENRICHER: EnricherEntity,
        }
    )

    def _normalize_namespace(
        self,
        namespace: ClientNamespace | str,
    ) -> ClientNamespace:
        if isinstance(namespace, ClientNamespace):
            return namespace
        return ClientNamespace(namespace)

    def _normalize_entity(
        self,
        namespace: ClientNamespace,
        entity: Enum | str,
    ) -> tuple[str, Enum]:
        validator = self._entity_validators.get(namespace)
        if validator is None:
            msg = f"Namespace '{namespace.value}' is not registered"
            raise KeyError(msg)
        try:
            value = validator(entity)
        except ValueError as err:
            msg = (
                f"Entity '{entity}' is not valid "
                f"for namespace '{namespace.value}'"
            )
            raise KeyError(msg) from err
        return value.value, value

    def get(
        self,
        namespace: ClientNamespace | str,
        entity: Any | None = None,
    ) -> Any:
        """Retrieve a client instance by namespace and entity."""
        normalized_namespace = self._normalize_namespace(namespace)
        if entity is None:
            msg = "Client entity must be provided"
            raise KeyError(msg)

        factory = self.factories.get(normalized_namespace.value)
        if factory is None:
            msg = (
                "Client namespace "
                f"'{normalized_namespace.value}' is not registered"
            )
            raise KeyError(msg)

        _, normalized_entity = self._normalize_entity(
            normalized_namespace,
            entity,
        )

        return factory.create(normalized_entity)

    def get_client(
        self,
        namespace: ClientNamespace | str,
        entity: Any | None = None,
    ) -> Any:
        """Retrieve a client instance by namespace and entity."""
        return self.get(namespace, entity)


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
class DefaultDomainContext(DomainContext):
    """Default in-memory domain context."""

    pipeline: "PipelineBaseProtocol" | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    descriptor: Any | None = None


@dataclass(slots=True)
class DefaultInfrastructureContext(InfrastructureContext):
    """Default infrastructure context for stage execution."""

    output_dir: Path = field(default_factory=Path.cwd)
    metadata_service: Any | None = None
    qc_orchestrator: Any | None = None


@dataclass(slots=True)
class DefaultArtifactContext(DefaultDataContext, ArtifactContext):
    """Default artifact context with data and artifact storage."""

    artifact_store: ArtifactStore = field(default_factory=ArtifactStore)


@dataclass(slots=True)
class StageContextAdapter:
    """Composable adapter that delegates to specialized contexts."""

    execution: ExecutionContext
    domain: DomainContext
    infrastructure: InfrastructureContext
    artifacts: ArtifactContext
    config: ConfigContext | None = None
    clients: ClientContext | None = None
    metrics: MetricsContext | None = None
    legacy_client_adapter: LegacyClientLookupAdapter | None = None

    @property
    def logger(self) -> UnifiedLogger:
        """Return the unified logger."""
        return self.execution.logger

    @property
    def request_id(self) -> str | None:
        """Return the request ID."""
        return self.execution.request_id

    @property
    def trace_id(self) -> str | None:
        """Return the trace ID."""
        return self.execution.trace_id

    @property
    def pipeline(self) -> "PipelineBaseProtocol | None":
        """Return the pipeline instance."""
        return self.domain.pipeline

    @property
    def metadata(self) -> dict[str, Any]:
        """Return domain metadata."""
        return self.domain.metadata

    @metadata.setter
    def metadata(self, value: dict[str, Any]) -> None:
        self.domain.metadata = value

    @property
    def descriptor(self) -> Any | None:
        """Return the stage descriptor."""
        return self.domain.descriptor

    @descriptor.setter
    def descriptor(self, value: Any | None) -> None:
        self.domain.descriptor = value

    @property
    def output_dir(self) -> Path:
        """Return the output directory."""
        return self.infrastructure.output_dir

    @property
    def metadata_service(self) -> Any | None:
        """Return the metadata service."""
        return self.infrastructure.metadata_service

    @property
    def qc_orchestrator(self) -> Any | None:
        """Return the QC orchestrator."""
        return self.infrastructure.qc_orchestrator

    @property
    def data_bucket(self) -> DataBucket:
        """Return the data bucket."""
        return self.artifacts.data_bucket

    @data_bucket.setter
    def data_bucket(self, value: DataBucket) -> None:
        self.artifacts.data_bucket = value

    @property
    def artifact_store(self) -> ArtifactStore:
        """Return the artifact store."""
        return self.artifacts.artifact_store

    @artifact_store.setter
    def artifact_store(self, value: ArtifactStore) -> None:
        self.artifacts.artifact_store = value

    @property
    def current_df(self) -> pd.DataFrame | None:
        """Get the current dataframe."""
        return self.data_bucket.get()

    @current_df.setter
    def current_df(self, value: pd.DataFrame) -> None:
        self.data_bucket.set(value)

    def get_config(self, key: str) -> Any:
        """Retrieve configuration from the configured provider."""
        if self.config is None:
            msg = "Config provider is not configured"
            raise KeyError(msg)
        return self.config.get_config(key)

    def get_client(
        self,
        namespace: ClientNamespace | str,
        entity: Any | None = None,
    ) -> Any:
        """Retrieve a client from the registry."""
        if self.clients is None:
            msg = "Client registry is not configured"
            raise KeyError(msg)
        resolved_namespace = namespace
        resolved_entity = entity

        if entity is None:
            if self.legacy_client_adapter is None:
                msg = "Entity must be provided when legacy adapter is missing"
                raise KeyError(msg)
            resolved_namespace, resolved_entity = self.legacy_client_adapter(
                str(namespace)
            )

        if resolved_entity is None:
            msg = "Client entity must be provided"
            raise KeyError(msg)

        return self.clients.get_client(
            resolved_namespace,
            resolved_entity,
        )

    def emit_metric(
        self, name: str, value: Any, tags: Mapping[str, str] | None = None
    ) -> None:
        """Emit a metric using the configured provider."""
        if self.metrics:
            self.metrics.emit_metric(name, value, tags)


@dataclass(slots=True)
class StageContext(StageContextAdapter):
    """Default implementation of :class:`StageContextProtocol`."""

    def __init__(
        self,
        *,
        execution: ExecutionContext,
        domain: DomainContext,
        infrastructure: InfrastructureContext,
        artifacts: ArtifactContext,
        config: ConfigContext | None = None,
        clients: ClientContext | None = None,
        metrics: MetricsContext | None = None,
        config_provider: Callable[[str], Any] | None = None,
        client_factories: Mapping[str | ClientNamespace, Any] | None = None,
        legacy_client_adapter: LegacyClientLookupAdapter | None = None,
        metric_emitter: (
            Callable[[str, Any, Mapping[str, str] | None], None] | None
        ) = None,
    ) -> None:
        config_context = config or DefaultConfigContext(config_provider)
        factories = {  # normalize enum keys to raw values
            (key.value if isinstance(key, ClientNamespace) else key): value
            for key, value in (client_factories or {}).items()
        }
        client_context = clients or ClientRegistry(factories)
        metrics_context = metrics or DefaultMetricsContext(metric_emitter)

        StageContextAdapter.__init__(
            self,
            execution=execution,
            domain=domain,
            infrastructure=infrastructure,
            artifacts=artifacts,
            config=config_context,
            clients=client_context,
            metrics=metrics_context,
            legacy_client_adapter=(
                legacy_client_adapter or LegacyClientLookupAdapter()
            ),
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
    "ClientNamespace",
    "ClientRegistry",
    "ClientRegistryContext",
    "DataContext",
    "MetricsContext",
    "StageFactoryContext",
    "DomainContext",
    "InfrastructureContext",
    "ArtifactContext",
    "DefaultExecutionContext",
    "DefaultConfigContext",
    "ClientNamespace",
    "ClientFactoryRegistry",
    "LegacyClientLookupAdapter",
    "ClientRegistry",
    "DefaultDataContext",
    "DefaultMetricsContext",
    "DefaultDomainContext",
    "DefaultInfrastructureContext",
    "DefaultArtifactContext",
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
