"""Runtime services used by pipeline stage plans."""

from __future__ import annotations

import hashlib
import json
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Iterable,
    Mapping,
    Protocol,
    cast,
)

import pandas as pd
import pandera as pa

from bioetl.core.io.writer import ArtifactWriter  # type: ignore[attr-defined]
from bioetl.core.pipeline.types import (
    ArtifactContext,
    DomainContext,
    ExecutionContext,
    InfrastructureContext,
    PipelineBaseProtocol,
    RunArtifacts,
    RunResult,
    RunState,
    StageCommand,
    StageContext,
    StageContextProtocol,
    StageExecutionOptions,
    StageRuntimeContext,
    StageProtocol,
    WriteArtifacts,
    WriteResult,
)
from bioetl.qc.executor import QCMetricsExecutor
from bioetl.qc.plan import QCPlan
from bioetl.core.runtime.qc import (
    default_qc_runtime_service_factory,
    default_qc_service_factory,
)

if TYPE_CHECKING:  # pragma: no cover - typing only
    from bioetl.core.runtime.lifecycle import OrchestrationCoordinatorProtocol
    from bioetl.core.runtime.metadata import MetadataCoordinator
    from bioetl.core.runtime.qc import QCOrchestratorProtocol


class ValidationService(Protocol):
    """Protocol for validating DataFrame results."""

    def empty_frame(self) -> pd.DataFrame:
        """Return an empty DataFrame with the correct schema."""

    def validate(
        self,
        df: pd.DataFrame,
        *,
        pipeline: PipelineBaseProtocol,
        options: StageExecutionOptions,
    ) -> pd.DataFrame:
        """
        Validate the DataFrame against the schema.

        Args:
            df: The DataFrame to validate.
            pipeline: The pipeline instance (for context).
            options: Execution options.

        Returns:
            The validated (and potentially sorted/coerced) DataFrame.
        """
        ...


class WriteService(Protocol):
    """Protocol for persisting transformed data."""

    def save(
        self,
        df: pd.DataFrame,
        artifacts: WriteArtifacts,
        options: StageExecutionOptions,
        *,
        context: StageContextProtocol,
        runtime: StageRuntimeContext,
    ) -> WriteResult:
        """
        Save the DataFrame to artifacts.

        Args:
            df: The DataFrame to save.
            artifacts: The write artifacts definition.
            options: Execution options.
            context: Stage context.
            runtime: Runtime context.

        Returns:
            The result of the write operation.
        """

    def write_metadata(
        self,
        output_dir: Path,
        artifacts: WriteArtifacts,
        df: pd.DataFrame | None,
        *,
        dry_run: bool,
    ) -> None:
        """
        Write metadata sidecars (meta.yaml).

        Args:
            output_dir: Directory to write to.
            artifacts: Artifacts definition.
            df: The DataFrame associated with the artifacts (optional).
            dry_run: Whether to perform a dry run.
        """


class StagePlanExecutor:
    """Responsible for executing the stage plan and tracking durations."""

    def __init__(
        self, qc_orchestrator: QCOrchestratorProtocol | None = None
    ) -> None:
        self.qc_orchestrator = qc_orchestrator

    def execute(
        self,
        stages: Iterable[StageCommand],
        context: StageContextProtocol,
        options: StageExecutionOptions,
        runtime_context: StageRuntimeContext | None = None,
    ) -> tuple[dict[str, int], str | None]:
        """
        Execute a sequence of stages.

        Args:
            stages: The list of stage commands to execute.
            context: The shared stage context.
            options: Execution options.
            runtime_context: Optional pre-configured runtime context.

        Returns:
            A tuple containing a dictionary of durations (stage_name -> ms)
            and an error message string if failure occurred (otherwise None).
        """
        logger = context.logger
        any_logger = cast(Any, logger)
        durations: dict[str, int] = {}
        error: str | None = None
        runtime_context = runtime_context or StageRuntimeContext(
            context=context,
            options=options,
        )
        runtime_context.context = context
        runtime_context.options = options

        for stage in stages:
            started = time.perf_counter()
            if logger:
                any_logger.info("STAGE_RUN_START", stage=stage.name)
            try:
                result = stage.execute(runtime_context)
                if isinstance(result.output, pd.DataFrame):
                    context.data_bucket.set(result.output)
                if (
                    stage.name == "extract"
                    and isinstance(result.output, pd.DataFrame)
                ):
                    context.metadata["extract_rows"] = int(
                        result.output.shape[0]
                    )
                if (
                    not options.dry_run
                    and options.sample is not None
                    and options.sample > 0
                    and isinstance(context.current_df, pd.DataFrame)
                    and stage.name in ("extract", "transform", "validate")
                ):
                    context.current_df = context.current_df.head(
                        options.sample
                    )
                if (
                    stage.name == "save_results"
                    and hasattr(result.output, "artifacts")
                ):
                    artifacts = (
                        result.output.artifacts  # type: ignore[attr-defined]
                    )
                    if isinstance(artifacts, WriteArtifacts):
                        context.artifact_store.set(artifacts)
            except Exception as exc:  # noqa: BLE001  # pylint: disable=broad-exception-caught
                error = str(exc)
                if logger:
                    any_logger.error(
                        "STAGE_RUN_ERROR",
                        stage=stage.name,
                        error=error,
                    )
                break
            finally:
                duration_ms = int((time.perf_counter() - started) * 1000)
                durations[stage.name] = duration_ms
                if logger:
                    any_logger.info(
                        "STAGE_RUN_END",
                        stage=stage.name,
                        duration_ms=duration_ms,
                    )

        return durations, error


@dataclass(slots=True)
class ArtifactService:
    """Service responsible for deterministic artifact planning."""

    artifact_planner: ArtifactPlanner

    def plan_run_artifacts(
        self,
        output_dir: Path,
        pipeline_code: str,
        run_tag: str | None,
        mode: str | None,
    ) -> tuple[Path, WriteArtifacts]:
        """
        Plan output paths and artifacts for a pipeline run.

        Args:
            output_dir: Base output directory.
            pipeline_code: Code of the pipeline.
            run_tag: Optional run tag.
            mode: Execution mode.

        Returns:
            Tuple of (resolved_output_path, WriteArtifacts).
        """
        return self.artifact_planner.plan(
            output_dir, pipeline_code, run_tag, mode
        )


@dataclass(slots=True)
class OrchestrationService:
    """Orchestration of stages and artifact planning."""

    stage_plan_executor: StagePlanExecutor
    artifact_service: ArtifactService

    def execute(
        self,
        stages: Iterable[StageCommand],
        context: StageContextProtocol,
        options: StageExecutionOptions,
        runtime_context: StageRuntimeContext | None = None,
    ) -> tuple[dict[str, int], str | None]:
        """
        Execute the pipeline stages.

        Delegates to StagePlanExecutor.
        """
        return self.stage_plan_executor.execute(
            stages,
            context,
            options,
            runtime_context,
        )

    def plan_run_artifacts(
        self,
        output_dir: Path,
        pipeline_code: str,
        run_tag: str | None,
        mode: str | None,
    ) -> tuple[Path, WriteArtifacts]:
        """
        Plan artifacts for the run.

        Delegates to ArtifactService.
        """
        return self.artifact_service.plan_run_artifacts(
            output_dir,
            pipeline_code,
            run_tag,
            mode,
        )


def _sort_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    columns = sorted(df.columns)
    if not columns:
        return df.reset_index(drop=True)
    return df.loc[:, columns].sort_values(by=columns).reset_index(drop=True)


@dataclass(slots=True)
class DefaultValidationService:
    """Validates dataframes using Pandera schemas and pipeline hooks."""

    validator: pa.DataFrameSchema | None = None

    def empty_frame(self) -> pd.DataFrame:
        """Create an empty DataFrame based on the schema."""
        if self.validator is None:
            return pd.DataFrame()
        columns = {
            name: pd.Series(dtype=str(schema.dtype))
            for name, schema in self.validator.columns.items()
        }
        return pd.DataFrame(columns)

    def validate(
        self,
        df: pd.DataFrame,
        *,
        pipeline: PipelineBaseProtocol,
        options: StageExecutionOptions,
    ) -> pd.DataFrame:
        """
        Validate DataFrame against the internal Pandera validator.

        Sorts columns and rows for determinism after validation.
        """
        _ = (pipeline, options)
        frame = df if self.validator is None else self.validator.validate(df)
        return _sort_dataframe(frame)


@dataclass(slots=True)
class DefaultWriteService:
    """Deterministic writer backed by :class:`ArtifactWriter`."""

    artifact_writer: ArtifactWriter

    def save(
        self,
        df: pd.DataFrame,
        artifacts: WriteArtifacts,
        options: StageExecutionOptions,
        *,
        context: StageContextProtocol,
        runtime: StageRuntimeContext,
    ) -> WriteResult:
        """Save DataFrame using ArtifactWriter."""
        _ = context
        any_artifacts = cast(Any, artifacts)
        runtime_context = cast(Any, runtime)
        output_dir = (
            any_artifacts.data_path.parent
            if any_artifacts.data_path
            else runtime_context.context.output_dir
        )
        return cast(
            WriteResult,
            self.artifact_writer.write(
                df,
                artifacts,
                output_dir=output_dir,
                dry_run=options.dry_run,
                extended=options.extended,
            ),
        )

    def write_metadata(
        self,
        output_dir: Path,
        artifacts: WriteArtifacts,
        df: pd.DataFrame | None,
        *,
        dry_run: bool,
    ) -> None:
        """Write metadata sidecars using ArtifactWriter."""
        self.artifact_writer.write_metadata(
            output_dir,
            artifacts,
            df,
            dry_run=dry_run,
        )


def default_validation_service_factory(
    pipeline: PipelineBaseProtocol,
) -> ValidationService:
    """Create a default validation service for the pipeline."""
    return DefaultValidationService(getattr(pipeline, "validator", None))


def default_write_service_factory(
    pipeline: PipelineBaseProtocol,
) -> WriteService:
    """Create a default write service using ArtifactWriter."""
    artifact_writer = ArtifactWriter(
        pipeline_code=pipeline.pipeline_code,
        run_id=getattr(pipeline, "run_id", ""),
        git_commit=getattr(pipeline, "_git_commit", None),
        config_hash=getattr(pipeline, "_config_hash", None),
    )
    return DefaultWriteService(artifact_writer)


class ArtifactPlanner:
    """Base class responsible for deterministic artifact planning."""

    def plan(
        self,
        output_dir: Path,
        pipeline_code: str,
        run_tag: str | None,
        mode: str | None,
    ) -> tuple[Path, WriteArtifacts]:
        """
        Abstract method to plan artifacts.

        Must be implemented by subclasses.
        """
        raise NotImplementedError


class DefaultArtifactPlanner(ArtifactPlanner):
    """Simple planner that writes directly into ``output_dir``."""

    def plan(
        self,
        output_dir: Path,
        pipeline_code: str,
        run_tag: str | None,
        mode: str | None,
    ) -> tuple[Path, WriteArtifacts]:
        """Plan artifacts by appending pipeline code to output directory."""
        output_dir.mkdir(parents=True, exist_ok=True)
        write_artifacts_cls = cast(Any, WriteArtifacts)
        artifacts = cast(WriteArtifacts, write_artifacts_cls())
        any_artifacts = cast(Any, artifacts)
        any_artifacts.data_path = output_dir / f"{pipeline_code}.csv"
        return output_dir, artifacts


class QCExecutorAdapter:
    """Thin wrapper over :class:`QCMetricsExecutor` with artifact wiring."""

    def __init__(
        self,
        *,
        executor_factory: Callable[[], QCMetricsExecutor] | None = None,
    ) -> None:
        self.executor_factory = executor_factory

    def execute(
        self,
        context: StageContextProtocol,
        plan: QCPlan,
        artifacts: WriteArtifacts | None = None,
    ) -> Path | None:
        """
        Execute QC metrics calculation and save reports.

        Args:
            context: Stage context containing the dataframe.
            plan: QC execution plan.
            artifacts: Optional output artifacts to update.

        Returns:
            Path to the QC metrics JSON file, or None if no metrics produced.
        """
        current_df = context.data_bucket.get()
        if current_df is None:
            return None

        dataset_artifacts = artifacts or context.artifact_store.get()
        any_artifacts = cast(Any, dataset_artifacts)
        dataset_name = (
            any_artifacts.data_path.stem
            if any_artifacts and any_artifacts.data_path
            else "dataset"
        )
        executor_factory = self.executor_factory or QCMetricsExecutor
        executor = executor_factory()
        quality_report, metrics_payload = executor.execute(
            current_df,
            plan,
            dataset_name=dataset_name,
        )
        if quality_report.empty and not metrics_payload:
            return None

        qc_dir = context.output_dir / "qc"
        qc_dir.mkdir(parents=True, exist_ok=True)
        quality_path = qc_dir / f"{dataset_name}_quality_report.csv"
        metrics_path = qc_dir / f"{dataset_name}_qc_metrics.json"
        quality_report.to_csv(quality_path, index=False)
        metrics_path.write_text(json.dumps(metrics_payload, indent=2))
        any_artifacts.quality_report_path = quality_path
        any_artifacts.qc_summary_path = metrics_path
        context.artifact_store.set(any_artifacts)
        return metrics_path


class QCService:
    """Service wrapper around QC execution pipeline."""

    def __init__(
        self,
        adapter: QCExecutorAdapter | None = None,
        *,
        enabled: bool | None = None,
        plan: QCPlan | None = None,
        dry_run: bool | None = None,
        thresholds: Mapping[str, float] | None = None,
    ) -> None:
        self.adapter = adapter or QCExecutorAdapter()
        self.enabled = enabled
        self.plan = plan
        self.dry_run = dry_run
        self.thresholds = thresholds or {}

    def execute(
        self,
        context: StageContextProtocol,
        options: StageExecutionOptions,
    ) -> Path | None:
        """
        Execute QC workflow if enabled.

        Resolves the effective plan and delegates to adapter.
        """
        if self.enabled is False or not options.include_qc_metrics:
            return None
        resolved_plan = self._resolve_plan(context, options)
        if not resolved_plan.enabled:
            return None
        artifacts = context.artifact_store.get()
        return self.adapter.execute(context, resolved_plan, artifacts)

    def _resolve_plan(
        self,
        context: StageContextProtocol,
        options: StageExecutionOptions,
    ) -> QCPlan:
        base_plan = (
            self.plan
            or getattr(context.pipeline, "qc_plan", None)
            or QCPlan.with_default_metrics()
        )
        thresholds = {**base_plan.thresholds, **self.thresholds}
        resolved_dry_run = (
            self.dry_run if self.dry_run is not None else options.dry_run
        )
        plan_updates: dict[str, Mapping[str, float] | bool] = {
            "dry_run": resolved_dry_run,
            "thresholds": thresholds,
        }
        return base_plan.model_copy(update=plan_updates)


@dataclass(slots=True)
class QCOrchestrator:
    """Orchestrates QC execution and error handling."""

    qc_service: QCService

    def run(
        self,
        context: StageContextProtocol,
        options: StageExecutionOptions,
    ) -> tuple[Path | None, str | None]:
        """
        Run the QC process safely.

        Catches any exceptions and returns them as error string.
        """
        try:
            return self.qc_service.execute(context, options), None
        except Exception as exc:  # noqa: BLE001  # pylint: disable=broad-exception-caught
            return None, str(exc)


@dataclass(slots=True)
class QCRuntimeService:
    """Runtime coordinator for QC execution."""

    qc_service: QCService | None
    qc_orchestrator: QCOrchestrator | None

    def run(
        self, context: StageContextProtocol, options: StageExecutionOptions
    ) -> tuple[Path | None, str | None]:
        """
        Execute QC if orchestrator is available.

        Delegates to QCOrchestrator.
        """
        if self.qc_orchestrator is None:
            return None, None
        return self.qc_orchestrator.run(context, options)


@dataclass(slots=True)
class MetadataService:
    """Service delegating metadata building to injected builder."""

    builder: Any
    _git_commit: str | None = None
    _config_hash: str | None = None

    def __post_init__(self) -> None:  # pragma: no cover - trivial
        self._git_commit = getattr(self.builder, "git_commit", None)
        self._config_hash = getattr(self.builder, "config_hash", None)

    def build(
        self,
        context: StageContextProtocol,
        stage_plan: Iterable[StageProtocol],
        durations: Mapping[str, int],
        run_tag: str | None,
        mode: str | None,
    ) -> dict[str, Any]:
        """Build metadata dictionary using the internal builder."""
        return cast(
            dict[str, Any],
            self.builder.build(
                context, stage_plan, durations, run_tag, mode
            ),
        )

    def build_for_run(
        self,
        context: StageContextProtocol,
        stage_plan: Iterable[StageProtocol],
        durations: Mapping[str, int],
        run_tag: str | None,
        mode: str | None,
        *,
        rows: int,
        qc_metrics_path: Path | None,
    ) -> dict[str, Any]:
        """Build metadata enriched with run stats (rows, QC path)."""
        metadata = self.build(context, stage_plan, durations, run_tag, mode)
        metadata["rows"] = rows
        if qc_metrics_path is not None:
            metadata["qc_metrics_path"] = str(qc_metrics_path)
        return metadata

    @property
    def git_commit(self) -> str | None:
        return self._git_commit

    @property
    def config_hash(self) -> str | None:
        return self._config_hash


@dataclass(slots=True)
class MetadataRuntimeService:
    """Runtime coordinator for building run metadata and results."""

    metadata_service: MetadataService
    logs_directory_resolver: Callable[[Path], Path]
    builder: Any | None = None
    git_commit: str | None = None
    config_hash: str | None = None

    def __post_init__(self) -> None:  # pragma: no cover - trivial
        self.builder = getattr(self.metadata_service, "builder", None)
        self.git_commit = getattr(self.metadata_service, "git_commit", None)
        self.config_hash = getattr(self.metadata_service, "config_hash", None)

    def build_run_metadata(
        self,
        context: StageContextProtocol,
        stage_plan: Iterable[StageProtocol],
        durations: Mapping[str, int],
        run_tag: str | None,
        mode: str | None,
        *,
        rows: int,
        qc_metrics_path: Path | None,
    ) -> dict[str, Any]:
        """
        Build comprehensive run metadata.

        Collects info from metadata service and adds runtime stats.
        """
        if hasattr(self.metadata_service, "build_for_run"):
            return cast(
                dict[str, Any],
                self.metadata_service.build_for_run(
                    context,
                    stage_plan,
                    durations,
                    run_tag,
                    mode,
                    rows=rows,
                    qc_metrics_path=qc_metrics_path,
                ),
            )
        builder = getattr(self.metadata_service, "builder", None)
        if builder is not None and callable(builder):
            return cast(
                dict[str, Any],
                builder(
                    context,
                    stage_plan,
                    durations,
                    run_tag,
                    mode,
                    rows=rows,
                    qc_metrics_path=qc_metrics_path,
                ),
            )
        if hasattr(self.metadata_service, "build"):
            return cast(
                dict[str, Any],
                self.metadata_service.build(
                    context,
                    stage_plan,
                    durations,
                    run_tag,
                    mode,
                ),
            )
        return {}

    def build_run_result(
        self,
        *,
        context: StageContextProtocol,
        stage_plan: Iterable[StageProtocol],
        run_state: RunState,
        run_tag: str | None,
        mode: str | None,
        rows: int,
        qc_metrics_path: Path | None,
        success: bool,
        output_dir: Path,
        logs_directory: Path,
    ) -> RunResult:
        """
        Construct the final RunResult object.

        Aggregates metadata, artifacts, stats, and error info.
        """
        resolved_logs_directory = (
            logs_directory
            or self.logs_directory_resolver(output_dir)
        )
        metadata = self.build_run_metadata(
            context,
            stage_plan,
            run_state.durations,
            run_tag,
            mode,
            rows=rows,
            qc_metrics_path=qc_metrics_path,
        )
        if context.artifact_store:
            artifacts = context.artifact_store.get()
        elif run_state.artifacts is not None:
            artifacts = run_state.artifacts
        else:
            write_artifacts_cls = cast(Any, WriteArtifacts)
            artifacts = cast(WriteArtifacts, write_artifacts_cls())
        run_artifacts_cls = cast(Any, RunArtifacts)
        run_artifacts = cast(
            RunArtifacts,
            run_artifacts_cls(
                output_dir=output_dir,
                logs_directory=resolved_logs_directory,
                write_artifacts=artifacts,
                qc_metrics_path=qc_metrics_path,
            ),
        )
        return RunResult(
            success=success,
            rows=rows,
            artifacts=run_artifacts,
            duration_ms=run_state.durations,
            error=run_state.error,
            metadata=metadata,
        )


class RunMetadataBuilder:
    """Конструктор метаданных запуска пайплайна."""

    def __init__(
        self, config: Mapping[str, Any] | Any, pipeline_code: str
    ) -> None:
        self.pipeline_code = pipeline_code
        self._git_commit = self._resolve_git_commit()
        self._config_hash = self._compute_config_hash(config)

    @property
    def git_commit(self) -> str | None:
        return self._git_commit

    @property
    def config_hash(self) -> str | None:
        return self._config_hash

    def build(
        self,
        context: StageContextProtocol,
        stages: Iterable[StageProtocol],
        durations: Mapping[str, int],
        run_tag: str | None,
        mode: str | None,
    ) -> dict[str, Any]:
        """Build the metadata dictionary."""
        metadata: dict[str, Any] = {
            "stage_plan": [stage.name for stage in stages],
            "extract_metadata": context.metadata,
            "git_commit": self._git_commit,
            "config_hash": self._config_hash,
            "pipeline": self.pipeline_code,
            "run_tag": run_tag,
            "mode": mode,
            "duration_seconds": sum(durations.values()) / 1000,
        }
        artifacts = context.artifact_store.get()
        any_artifacts = cast(Any, artifacts)
        if any_artifacts.data_path:
            metadata["output_path"] = str(any_artifacts.data_path)
        pipeline_metadata = self._collect_pipeline_metadata(context)
        if pipeline_metadata:
            metadata = self._merge_metadata(metadata, pipeline_metadata)
        return metadata

    def _collect_pipeline_metadata(
        self, context: StageContextProtocol
    ) -> Mapping[str, Any]:  # pragma: no cover - thin adapter
        pipeline = getattr(context, "pipeline", None)
        if pipeline is None:
            return {}
        builder = getattr(pipeline, "build_pipeline_metadata", None)
        if callable(builder):
            try:
                extra = builder(context)
            except TypeError:
                extra = builder()
            if isinstance(extra, Mapping):
                return extra
            try:
                return dict(extra)
            except Exception:  # noqa: BLE001  # pylint: disable=broad-exception-caught
                return {}
        return {}

    @staticmethod
    def _merge_metadata(
        base: dict[str, Any], extra: Mapping[str, Any]
    ) -> dict[str, Any]:  # pragma: no cover - pure function
        merged = dict(base)
        for key, value in extra.items():
            if (
                key == "extract_metadata"
                and isinstance(value, Mapping)
                and isinstance(base.get(key), Mapping)
            ):
                combined = dict(cast(Mapping[str, Any], base[key]))
                combined.update(value)
                merged[key] = combined
            else:
                merged[key] = value
        return merged

    def _resolve_git_commit(self) -> str | None:
        try:
            completed = subprocess.run(
                ["git", "rev-parse", "HEAD"],
                capture_output=True,
                check=True,
                text=True,
            )
        except (subprocess.SubprocessError, OSError):
            return None
        return completed.stdout.strip() or None

    def _compute_config_hash(
        self, config: Mapping[str, Any] | Any
    ) -> str | None:
        try:
            payload: Mapping[str, Any]
            if isinstance(config, Mapping):
                payload = dict(config)
            elif hasattr(config, "__dict__"):
                payload = dict(config.__dict__)
            else:
                return None
            serialized = json.dumps(
                payload,
                sort_keys=True,
                default=str,
            ).encode("utf-8")
            return hashlib.sha256(serialized).hexdigest()
        except (TypeError, ValueError):
            return None


def default_artifact_planner_factory() -> ArtifactPlanner:
    """Create a default artifact planner."""
    return DefaultArtifactPlanner()


def default_artifact_service_factory(
    artifact_planner: ArtifactPlanner | None = None,
) -> ArtifactService:
    """Create a default artifact service."""
    return ArtifactService(artifact_planner or DefaultArtifactPlanner())


def default_orchestration_service_factory(
    stage_plan_executor: StagePlanExecutor | None = None,
    artifact_service: ArtifactService | None = None,
) -> Callable[[OrchestrationCoordinatorProtocol], OrchestrationService]:
    """Create a factory for the default orchestration service."""
    def _factory(
        coordinator: OrchestrationCoordinatorProtocol,
    ) -> OrchestrationService:
        executor = stage_plan_executor or getattr(
            coordinator, "stage_plan_executor", None
        )
        artifacts = artifact_service or getattr(
            coordinator, "artifact_service", None
        )
        return OrchestrationService(
            stage_plan_executor=executor or StagePlanExecutor(),
            artifact_service=artifacts or default_artifact_service_factory(),
        )

    return _factory


def default_metadata_service_factory(
    config: Mapping[str, Any] | Any | None = None,
    pipeline_code: str | None = None,
) -> Callable[[PipelineBaseProtocol], MetadataService]:
    """Create a factory for the default metadata service."""
    def _factory(pipeline: PipelineBaseProtocol) -> MetadataService:
        resolved_config = (
            config if config is not None else getattr(pipeline, "config", {})
        )
        raw_code = pipeline_code or pipeline.pipeline_code
        resolved_code = str(raw_code)
        builder = RunMetadataBuilder(resolved_config, resolved_code)
        return MetadataService(builder=builder)

    return _factory


@dataclass(slots=True)
class ArtifactRuntimeService:
    """Pipeline-level artifact planning helper."""

    artifact_planner: ArtifactPlanner
    artifact_service: ArtifactService

    def plan_run_artifacts(
        self,
        output_dir: Path,
        pipeline_code: str,
        run_tag: str | None,
        mode: str | None,
    ) -> tuple[Path, WriteArtifacts]:
        """Plan artifacts for the run (delegates to artifact service)."""
        return self.artifact_service.plan_run_artifacts(
            output_dir, pipeline_code, run_tag, mode
        )


def default_artifact_runtime_service_factory(
    artifact_planner: ArtifactPlanner | None = None,
) -> Callable[[PipelineBaseProtocol], ArtifactRuntimeService]:
    """Create a factory for the default artifact runtime service."""
    def _factory(_: PipelineBaseProtocol) -> ArtifactRuntimeService:
        planner = artifact_planner or default_artifact_planner_factory()
        return ArtifactRuntimeService(
            artifact_planner=planner,
            artifact_service=default_artifact_service_factory(planner),
        )

    return _factory


def default_metadata_runtime_service_factory(
    *,
    config: Mapping[str, Any] | Any | None = None,
    pipeline_code: str | None = None,
    metadata_service: MetadataService | None = None,
    metadata_service_factory: Callable[
        [MetadataCoordinator], MetadataService
    ]
    | None = None,
    run_metadata_builder: RunMetadataBuilder | None = None,
    logs_directory_resolver: Callable[[Path], Path] | None = None,
) -> Callable[[MetadataCoordinator], MetadataRuntimeService]:
    """Create a factory for the default metadata runtime service."""
    def _factory(coordinator: MetadataCoordinator) -> MetadataRuntimeService:
        if metadata_service is not None:
            resolved_service = metadata_service
        elif metadata_service_factory is not None:
            resolved_service = metadata_service_factory(coordinator)
        else:
            resolved_config = (
                config
                if config is not None
                else getattr(coordinator, "config", {})
            )
            resolved_code = str(
                pipeline_code or getattr(coordinator, "pipeline_code", "")
            )
            builder = run_metadata_builder or RunMetadataBuilder(
                resolved_config, resolved_code
            )
            resolved_service = MetadataService(builder=builder)
        resolver = logs_directory_resolver or getattr(
            coordinator, "logs_directory_resolver"
        )
        return MetadataRuntimeService(
            metadata_service=resolved_service,
            logs_directory_resolver=resolver,
        )

    return _factory


@dataclass(slots=True)
class ContextBuilder:
    """Builder responsible for assembling :class:`StageContext`."""

    pipeline: PipelineBaseProtocol
    config_provider: Callable[[str], Any]

    def build(
        self,
        *,
        execution: ExecutionContext,
        domain: DomainContext,
        infrastructure: InfrastructureContext,
        artifacts: ArtifactContext,
    ) -> StageContext:
        """Build a StageContext from the provided components."""
        return StageContext(
            execution=execution,
            domain=domain,
            infrastructure=infrastructure,
            artifacts=artifacts,
            config_provider=self.config_provider,
        )


def default_context_builder_factory(
    *, config_provider: Callable[[str], Any] | None = None
) -> Callable[[PipelineBaseProtocol], ContextBuilder]:
    """Create a factory for the default context builder."""
    def _factory(pipeline: PipelineBaseProtocol) -> ContextBuilder:
        provider = config_provider or getattr(
            pipeline, "_build_config_provider"
        )()
        return ContextBuilder(pipeline=pipeline, config_provider=provider)

    return _factory


__all__ = [
    "ArtifactService",
    "ArtifactPlanner",
    "DefaultArtifactPlanner",
    "DefaultValidationService",
    "DefaultWriteService",
    "OrchestrationService",
    "MetadataService",
    "QCOrchestrator",
    "RunMetadataBuilder",
    "QCExecutorAdapter",
    "QCService",
    "ValidationService",
    "WriteService",
    "default_artifact_service_factory",
    "default_artifact_planner_factory",
    "default_metadata_service_factory",
    "default_artifact_runtime_service_factory",
    "default_context_builder_factory",
    "default_metadata_runtime_service_factory",
    "default_qc_runtime_service_factory",
    "default_orchestration_service_factory",
    "default_qc_service_factory",
    "default_validation_service_factory",
    "default_write_service_factory",
    "StagePlanExecutor",
    "ArtifactRuntimeService",
    "ContextBuilder",
    "MetadataRuntimeService",
    "QCRuntimeService",
]
