from __future__ import annotations

"""Runtime services used by pipeline stage plans."""

import hashlib
import json
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Protocol

import pandas as pd
import pandera as pa

from bioetl.core.io import ArtifactWriter
from bioetl.core.pipeline.types import (
    PipelineBaseProtocol,
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


class ValidationService(Protocol):
    """Protocol for validating DataFrame results."""

    def empty_frame(self) -> pd.DataFrame:
        ...

    def validate(
        self,
        df: pd.DataFrame,
        *,
        pipeline: PipelineBaseProtocol,
        options: StageExecutionOptions,
    ) -> pd.DataFrame:
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
        ...

    def write_metadata(
        self, output_dir: Path, artifacts: WriteArtifacts, df: pd.DataFrame | None, *, dry_run: bool
    ) -> None:
        ...


class StagePlanExecutor:
    """Ответственный за исполнение плана стадий и подсчет длительностей."""

    def __init__(self, qc_orchestrator: "QCOrchestrator | None" = None) -> None:
        self.qc_orchestrator = qc_orchestrator

    def execute(
        self,
        stages: Iterable[StageCommand],
        context: StageContext,
        options: StageExecutionOptions,
        runtime_context: StageRuntimeContext | None = None,
    ) -> tuple[dict[str, int], str | None]:
        logger = context.logger
        durations: dict[str, int] = {}
        error: str | None = None
        runtime_context = runtime_context or StageRuntimeContext(context=context, options=options)
        runtime_context.context = context
        runtime_context.options = options

        for stage in stages:
            started = time.perf_counter()
            if logger:
                logger.info("STAGE_RUN_START", stage=stage.name)
            try:
                result = stage.execute(runtime_context)
                if isinstance(result.output, pd.DataFrame):
                    context.data_bucket.set(result.output)
                if stage.name == "extract" and isinstance(result.output, pd.DataFrame):
                    context.metadata["extract_rows"] = int(result.output.shape[0])
                if (
                    not options.dry_run
                    and options.sample is not None
                    and options.sample > 0
                    and isinstance(context.current_df, pd.DataFrame)
                    and stage.name in ("extract", "transform", "validate")
                ):
                    context.current_df = context.current_df.head(options.sample)
                if stage.name == "save_results" and hasattr(result.output, "artifacts"):
                    artifacts = result.output.artifacts  # type: ignore[attr-defined]
                    if isinstance(artifacts, WriteArtifacts):
                        context.artifact_store.set(artifacts)
            except Exception as exc:  # pragma: no cover - surfaced via RunResult
                error = str(exc)
                if logger:
                    logger.error("STAGE_RUN_ERROR", stage=stage.name, error=error)
                break
            finally:
                duration_ms = int((time.perf_counter() - started) * 1000)
                durations[stage.name] = duration_ms
                if logger:
                    logger.info("STAGE_RUN_END", stage=stage.name, duration_ms=duration_ms)

        return durations, error


@dataclass(slots=True)
class ArtifactService:
    """Service responsible for deterministic artifact planning."""

    artifact_planner: ArtifactPlanner

    def plan_run_artifacts(
        self, output_dir: Path, pipeline_code: str, run_tag: str | None, mode: str | None
    ) -> tuple[Path, WriteArtifacts]:
        return self.artifact_planner.plan(output_dir, pipeline_code, run_tag, mode)


@dataclass(slots=True)
class OrchestrationService:
    """Оркестрация стадий и планирование артефактов."""

    stage_plan_executor: StagePlanExecutor
    artifact_service: ArtifactService

    def execute(
        self,
        stages: Iterable[StageCommand],
        context: StageContext,
        options: StageExecutionOptions,
        runtime_context: StageRuntimeContext | None = None,
    ) -> tuple[dict[str, int], str | None]:
        return self.stage_plan_executor.execute(stages, context, options, runtime_context)

    def plan_run_artifacts(
        self, output_dir: Path, pipeline_code: str, run_tag: str | None, mode: str | None
    ) -> tuple[Path, WriteArtifacts]:
        return self.artifact_service.plan_run_artifacts(output_dir, pipeline_code, run_tag, mode)


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
        if self.validator is None:
            return pd.DataFrame()
        columns = {name: pd.Series(dtype=str(schema.dtype)) for name, schema in self.validator.columns.items()}
        return pd.DataFrame(columns)

    def validate(
        self,
        df: pd.DataFrame,
        *,
        pipeline: PipelineBaseProtocol,
        options: StageExecutionOptions,
    ) -> pd.DataFrame:
        frame = df if self.validator is None else self.validator.validate(df)
        validate_hook = getattr(pipeline, "validate", None)
        if callable(validate_hook):
            frame = validate_hook(frame, options)
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
        output_dir = artifacts.data_path.parent if artifacts.data_path else runtime.context.output_dir
        return self.artifact_writer.write(
            df,
            artifacts,
            output_dir=output_dir,
            dry_run=options.dry_run,
            extended=options.extended,
        )

    def write_metadata(
        self, output_dir: Path, artifacts: WriteArtifacts, df: pd.DataFrame | None, *, dry_run: bool
    ) -> None:
        self.artifact_writer._write_metadata(output_dir, artifacts, df, dry_run=dry_run)


def default_validation_service_factory(pipeline: PipelineBaseProtocol) -> ValidationService:
    return DefaultValidationService(getattr(pipeline, "validator", None))


def default_write_service_factory(pipeline: PipelineBaseProtocol) -> WriteService:
    artifact_writer = ArtifactWriter(
        pipeline_code=pipeline.pipeline_code,
        run_id=pipeline.run_id,
        git_commit=getattr(pipeline, "_git_commit", None),
        config_hash=getattr(pipeline, "_config_hash", None),
    )
    return DefaultWriteService(artifact_writer)


class ArtifactPlanner:
    """Base class responsible for deterministic artifact planning."""

    def plan(
        self, output_dir: Path, pipeline_code: str, run_tag: str | None, mode: str | None
    ) -> tuple[Path, WriteArtifacts]:
        raise NotImplementedError


class DefaultArtifactPlanner(ArtifactPlanner):
    """Simple planner that writes directly into ``output_dir``."""

    def plan(
        self, output_dir: Path, pipeline_code: str, run_tag: str | None, mode: str | None
    ) -> tuple[Path, WriteArtifacts]:
        output_dir.mkdir(parents=True, exist_ok=True)
        artifacts = WriteArtifacts(data_path=output_dir / f"{pipeline_code}.csv")
        return output_dir, artifacts


class QCExecutorAdapter:
    """Thin wrapper over :class:`QCMetricsExecutor` with artifact wiring."""

    def __init__(self, *, executor_factory: Callable[[], QCMetricsExecutor] | None = None) -> None:
        self.executor_factory = executor_factory

    def execute(
        self,
        context: StageContext,
        plan: QCPlan,
        artifacts: WriteArtifacts | None = None,
    ) -> Path | None:
        current_df = context.data_bucket.get()
        if current_df is None:
            return None

        dataset_artifacts = artifacts or context.artifact_store.get()
        dataset_name = (
            dataset_artifacts.data_path.stem if dataset_artifacts and dataset_artifacts.data_path else "dataset"
        )
        executor_factory = self.executor_factory or QCMetricsExecutor
        executor = executor_factory()
        quality_report, metrics_payload = executor.execute(current_df, plan, dataset_name=dataset_name)
        if quality_report.empty and not metrics_payload:
            return None

        qc_dir = context.output_dir / "qc"
        qc_dir.mkdir(parents=True, exist_ok=True)
        quality_path = qc_dir / f"{dataset_name}_quality_report.csv"
        metrics_path = qc_dir / f"{dataset_name}_qc_metrics.json"
        quality_report.to_csv(quality_path, index=False)
        metrics_path.write_text(json.dumps(metrics_payload, indent=2))
        dataset_artifacts.quality_report_path = quality_path
        dataset_artifacts.qc_summary_path = metrics_path
        context.artifact_store.set(dataset_artifacts)
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

    def execute(self, context: StageContext, options: StageExecutionOptions) -> Path | None:
        if self.enabled is False or not options.include_qc_metrics:
            return None
        resolved_plan = self._resolve_plan(context, options)
        if not resolved_plan.enabled:
            return None
        artifacts = context.artifact_store.get()
        return self.adapter.execute(context, resolved_plan, artifacts)

    def _resolve_plan(self, context: StageContext, options: StageExecutionOptions) -> QCPlan:
        base_plan = self.plan or getattr(context.pipeline, "qc_plan", None) or QCPlan.with_default_metrics()
        thresholds = {**base_plan.thresholds, **self.thresholds}
        resolved_dry_run = self.dry_run if self.dry_run is not None else options.dry_run
        plan_updates: dict[str, Mapping[str, float] | bool] = {"dry_run": resolved_dry_run, "thresholds": thresholds}
        return base_plan.model_copy(update=plan_updates)


@dataclass(slots=True)
class QCOrchestrator:
    """Orchestrates QC execution and error handling."""

    qc_service: QCService

    def run(self, context: StageContext, options: StageExecutionOptions) -> tuple[Path | None, str | None]:
        try:
            return self.qc_service.execute(context, options), None
        except Exception as exc:  # pragma: no cover - surfaced via RunResult
            return None, str(exc)


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
        context: StageContext,
        stage_plan: Iterable[StageProtocol],
        durations: Mapping[str, int],
        run_tag: str | None,
        mode: str | None,
    ) -> dict[str, Any]:
        return self.builder.build(context, stage_plan, durations, run_tag, mode)

    def build_for_run(
        self,
        context: StageContext,
        stage_plan: Iterable[StageProtocol],
        durations: Mapping[str, int],
        run_tag: str | None,
        mode: str | None,
        *,
        rows: int,
        qc_metrics_path: Path | None,
    ) -> dict[str, Any]:
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


class RunMetadataBuilder:
    """Конструктор метаданных запуска пайплайна."""

    def __init__(self, config: Mapping[str, Any] | Any, pipeline_code: str) -> None:
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
        context: StageContext,
        stages: Iterable[StageProtocol],
        durations: Mapping[str, int],
        run_tag: str | None,
        mode: str | None,
    ) -> dict[str, Any]:
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
        if artifacts.data_path:
            metadata["output_path"] = str(artifacts.data_path)
        return metadata

    def _resolve_git_commit(self) -> str | None:
        try:
            completed = subprocess.run(
                ["git", "rev-parse", "HEAD"], capture_output=True, check=True, text=True
            )
        except Exception:
            return None
        return completed.stdout.strip() or None

    def _compute_config_hash(self, config: Mapping[str, Any] | Any) -> str | None:
        try:
            payload: Mapping[str, Any]
            if isinstance(config, Mapping):
                payload = dict(config)
            elif hasattr(config, "__dict__"):
                payload = dict(config.__dict__)
            else:
                return None
            serialized = json.dumps(payload, sort_keys=True, default=str).encode("utf-8")
            return hashlib.sha256(serialized).hexdigest()
        except Exception:
            return None


def default_artifact_planner_factory() -> ArtifactPlanner:
    return DefaultArtifactPlanner()


def default_artifact_service_factory(artifact_planner: ArtifactPlanner | None = None) -> ArtifactService:
    return ArtifactService(artifact_planner or DefaultArtifactPlanner())


def default_orchestration_service_factory(
    stage_plan_executor: StagePlanExecutor | None = None,
    artifact_service: ArtifactService | None = None,
) -> Callable[[PipelineBaseProtocol], OrchestrationService]:
    def _factory(_: PipelineBaseProtocol) -> OrchestrationService:
        return OrchestrationService(
            stage_plan_executor=stage_plan_executor or StagePlanExecutor(),
            artifact_service=artifact_service or default_artifact_service_factory(),
        )

    return _factory


def default_qc_service_factory(
    *,
    qc_plan: QCPlan | None = None,
    executor_factory: Callable[[], QCMetricsExecutor] | None = None,
    qc_thresholds: Mapping[str, float] | None = None,
    qc_dry_run: bool | None = None,
    qc_enabled: bool | None = None,
) -> Callable[[PipelineBaseProtocol], QCService]:
    def _factory(_: PipelineBaseProtocol) -> QCService:
        return QCService(
            QCExecutorAdapter(executor_factory=executor_factory),
            enabled=qc_enabled,
            plan=qc_plan,
            dry_run=qc_dry_run,
            thresholds=qc_thresholds,
        )

    return _factory


def default_metadata_service_factory(
    config: Mapping[str, Any] | Any | None = None, pipeline_code: str | None = None
) -> Callable[[PipelineBaseProtocol], MetadataService]:
    def _factory(pipeline: PipelineBaseProtocol) -> MetadataService:
        resolved_config = config if config is not None else getattr(pipeline, "config", {})
        resolved_code = pipeline_code or pipeline.pipeline_code
        return MetadataService(builder=RunMetadataBuilder(resolved_config, resolved_code))

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
    "default_orchestration_service_factory",
    "default_qc_service_factory",
    "default_validation_service_factory",
    "default_write_service_factory",
    "StagePlanExecutor",
]
