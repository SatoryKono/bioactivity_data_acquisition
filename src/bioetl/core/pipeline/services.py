from __future__ import annotations

"""Runtime services used by pipeline stage plans."""

import hashlib
import json
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Protocol

import pandas as pd
import pandera as pa

from bioetl.core.io import ArtifactWriter
from bioetl.core.pipeline.types import (
    PipelineBaseProtocol,
    Stage,
    StageContext,
    StageContextProtocol,
    StageExecutionOptions,
    StageRuntimeContext,
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

    def __init__(
        self,
        *,
        executor_factory: Callable[[], QCMetricsExecutor] | None = None,
        qc_plan: QCPlan | None = None,
    ) -> None:
        self.executor_factory = executor_factory
        self.qc_plan = qc_plan

    def execute(
        self, context: StageContext, options: StageExecutionOptions, artifacts: WriteArtifacts
    ) -> Path | None:
        if context.current_df is None or not options.include_qc_metrics:
            return None

        plan = self.qc_plan or getattr(context.pipeline, "qc_plan", None) or QCPlan.with_default_metrics()
        if options.dry_run:
            plan = plan.model_copy(update={"dry_run": True})

        dataset_name = artifacts.data_path.stem if artifacts and artifacts.data_path else "dataset"
        executor_factory = self.executor_factory or QCMetricsExecutor
        executor = executor_factory()
        quality_report, metrics_payload = executor.execute(
            context.current_df, plan, dataset_name=dataset_name
        )
        if quality_report.empty and not metrics_payload:
            return None

        qc_dir = context.output_dir / "qc"
        qc_dir.mkdir(parents=True, exist_ok=True)
        quality_path = qc_dir / f"{dataset_name}_quality_report.csv"
        metrics_path = qc_dir / f"{dataset_name}_qc_metrics.json"
        quality_report.to_csv(quality_path, index=False)
        metrics_path.write_text(json.dumps(metrics_payload, indent=2))
        artifacts.quality_report_path = quality_path
        artifacts.qc_summary_path = metrics_path
        context.artifacts = artifacts
        return metrics_path


class QCService:
    """Service wrapper around QC execution pipeline."""

    def __init__(self, adapter: QCExecutorAdapter | None = None) -> None:
        self.adapter = adapter or QCExecutorAdapter()

    def execute(self, context: StageContext, options: StageExecutionOptions) -> Path | None:
        artifacts = context.artifacts or WriteArtifacts()
        return self.adapter.execute(context, options, artifacts)


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
        stage_plan: Iterable[Stage],
        durations: Mapping[str, int],
        run_tag: str | None,
        mode: str | None,
    ) -> dict[str, Any]:
        return self.builder.build(context, stage_plan, durations, run_tag, mode)

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
        stages: Iterable[Stage],
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
        if context.artifacts and context.artifacts.data_path:
            metadata["output_path"] = str(context.artifacts.data_path)
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


def default_qc_service_factory(
    *, qc_plan: QCPlan | None = None, executor_factory: Callable[[], QCMetricsExecutor] | None = None
) -> QCService:
    return QCService(QCExecutorAdapter(executor_factory=executor_factory, qc_plan=qc_plan))


def default_metadata_service_factory(
    config: Mapping[str, Any] | Any, pipeline_code: str
) -> MetadataService:
    return MetadataService(builder=RunMetadataBuilder(config, pipeline_code))


__all__ = [
    "ArtifactPlanner",
    "DefaultArtifactPlanner",
    "DefaultValidationService",
    "DefaultWriteService",
    "MetadataService",
    "RunMetadataBuilder",
    "QCExecutorAdapter",
    "QCService",
    "ValidationService",
    "WriteService",
    "default_artifact_planner_factory",
    "default_metadata_service_factory",
    "default_qc_service_factory",
    "default_validation_service_factory",
    "default_write_service_factory",
]
