from __future__ import annotations

"""Runtime services used by pipeline stage plans."""

import json
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

    def build(
        self,
        context: StageContext,
        stage_plan: Iterable[Stage],
        durations: Mapping[str, int],
        run_tag: str | None,
        mode: str | None,
    ) -> dict[str, Any]:
        return self.builder.build(context, stage_plan, durations, run_tag, mode)


__all__ = [
    "ArtifactPlanner",
    "DefaultArtifactPlanner",
    "DefaultValidationService",
    "DefaultWriteService",
    "MetadataService",
    "QCExecutorAdapter",
    "QCService",
    "ValidationService",
    "WriteService",
    "default_validation_service_factory",
    "default_write_service_factory",
]
