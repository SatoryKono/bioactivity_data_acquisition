"""Shared runtime primitives for orchestrating ETL pipelines."""
from __future__ import annotations

import hashlib
import json
import subprocess
import time
import uuid
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping

import pandas as pd
import yaml

from bioetl.core.logging import UnifiedLogger
from bioetl.core.pipeline.definition import PipelineDefinition
from bioetl.core.pipeline.factory import StageFactory
from bioetl.core.pipeline.types import (
    PipelineBaseProtocol,
    RunArtifacts,
    RunResult,
    Stage,
    StageContext,
    StageDescriptor,
    StageExecutionOptions,
    StageRuntimeContext,
    WriteArtifacts,
)
from bioetl.core.pipeline.services import ValidationService, WriteService
from bioetl.qc.executor import QCMetricsExecutor
from bioetl.qc.plan import QCPlan


class QCExecutorAdapter:
    """Obвязка над :class:`QCMetricsExecutor` с обработкой артефактов."""

    def __init__(
        self,
        *,
        executor_factory: Callable[[], QCMetricsExecutor] | None = None,
        qc_plan: QCPlan | None = None,
    ) -> None:
        self.executor_factory = executor_factory
        self.qc_plan = qc_plan

    def execute(
        self,
        context: StageContextProtocol,
        runtime: StageRuntimeContext,
        artifacts: WriteArtifacts,
    ) -> Path | None:
        """Выполнить QC-метрики и вернуть путь до json-отчета."""

        dataframe = runtime.context.current_df
        if dataframe is None or not runtime.options.include_qc_metrics:
            return None

        plan = self.qc_plan or QCPlan.with_default_metrics()
        if runtime.options.dry_run:
            plan = plan.model_copy(update={"dry_run": True})

        dataset_name = artifacts.data_path.stem if artifacts and artifacts.data_path else "dataset"
        executor_factory = self.executor_factory or QCMetricsExecutor
        executor = executor_factory()
        quality_report, metrics_payload = executor.execute(dataframe, plan, dataset_name=dataset_name)
        if quality_report.empty and not metrics_payload:
            return None

        output_dir = runtime.context.output_dir
        qc_dir = output_dir / "qc"
        qc_dir.mkdir(parents=True, exist_ok=True)
        quality_path = qc_dir / f"{dataset_name}_quality_report.csv"
        metrics_path = qc_dir / f"{dataset_name}_qc_metrics.json"
        quality_report.to_csv(quality_path, index=False)
        metrics_path.write_text(json.dumps(metrics_payload, indent=2))
        if artifacts:
            artifacts.quality_report_path = quality_path
            artifacts.qc_summary_path = metrics_path
            runtime.context.artifacts = artifacts
        return metrics_path


class StagePlanExecutor:
    """Ответственный за исполнение плана стадий и подсчет длительностей."""

    def __init__(self, qc_adapter: QCExecutorAdapter) -> None:
        self.qc_adapter = qc_adapter

    def execute(
        self,
        stages: Iterable[Stage],
        context: StageContext,
        options: StageExecutionOptions,
        *,
        include_qc_metrics: bool,
    ) -> tuple[dict[str, int], str | None, Path | None]:
        logger = context.logger
        durations: dict[str, int] = {}
        error: str | None = None
        artifacts = context.artifacts or WriteArtifacts()
        if not context.artifacts:
            context.artifacts = artifacts
        qc_metrics_path: Path | None = None

        runtime_context = StageRuntimeContext(context=context, options=options)

        for stage in stages:
            started = time.perf_counter()
            if logger:
                logger.info("STAGE_RUN_START", stage=stage.name)
            try:
                result = stage.execute(runtime_context)
                if stage.name == "extract" and isinstance(result.output, pd.DataFrame):
                    context.metadata["extract_rows"] = int(result.output.shape[0])
                if stage.name == "save_results" and hasattr(result.output, "artifacts"):
                    artifacts = result.output.artifacts  # type: ignore[attr-defined]
                    if isinstance(artifacts, WriteArtifacts):
                        context.artifacts = artifacts
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

        if error is None and include_qc_metrics:
            try:
                qc_metrics_path = self.qc_adapter.execute(context, runtime_context, artifacts)
            except Exception as exc:  # pragma: no cover - surfaced via RunResult
                error = str(exc)
                if logger:
                    logger.error("QC_METRICS_ERROR", error=error)

        return durations, error, qc_metrics_path


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
            serialized = yaml.safe_dump(payload, sort_keys=True).encode("utf-8")
            return hashlib.sha256(serialized).hexdigest()
        except Exception:
            return None


class PipelineRuntimeBase(ABC, PipelineBaseProtocol):
    """Common runtime for pipelines that orchestrate ETL stage plans."""

    deterministic_folder_prefix: str = "_"

    def __init__(
        self,
        config: Mapping[str, Any] | Any,
        pipeline_definition: PipelineDefinition | None = None,
        *,
        run_id: str | None = None,
        validator: Any | None = None,
        validation_service_factory: Callable[["PipelineRuntimeBase"], ValidationService]
        | None = None,
        write_service_factory: Callable[["PipelineRuntimeBase"], WriteService] | None = None,
        qc_executor_adapter: QCExecutorAdapter | None = None,
        qc_executor_factory: Callable[[], QCMetricsExecutor] | None = None,
        qc_plan: QCPlan | None = None,
        stage_plan_executor: StagePlanExecutor | None = None,
        artifact_planner: ArtifactPlanner | None = None,
        qc_service: QCService | None = None,
        metadata_service: MetadataService | None = None,
        run_metadata_builder: RunMetadataBuilder | None = None,
    ) -> None:
        self.config = config
        self.pipeline_definition = pipeline_definition or self._build_default_definition(config)
        self.run_id = run_id or uuid.uuid4().hex
        self.validator = validator
        self.pipeline_code = self._resolve_pipeline_code(config, self.pipeline_definition)
        materialization = getattr(config, "materialization", None)
        root = getattr(materialization, "root", None)
        self.output_root = Path(root) if root else Path.cwd()
        self.logs_directory = self.output_root.parent / "logs" / self.pipeline_code
        self.qc_executor_adapter = qc_executor_adapter or QCExecutorAdapter(
            executor_factory=qc_executor_factory, qc_plan=qc_plan
        )
        self.stage_plan_executor = stage_plan_executor or StagePlanExecutor(self.qc_executor_adapter)
        self.run_metadata_builder = run_metadata_builder or RunMetadataBuilder(config, self.pipeline_code)
        self.qc_plan = qc_plan
        self.dry_run = False
        self._git_commit = self.run_metadata_builder.git_commit
        self._config_hash = self.run_metadata_builder.config_hash
        self.validation_service = (
            validation_service_factory(self)
            if validation_service_factory is not None
            else None
        )
        self.write_service = (
            write_service_factory(self) if write_service_factory is not None else None
        )

    # Lifecycle -----------------------------------------------------------
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
        if dry_run is not None:
            self.dry_run = dry_run

        logger = UnifiedLogger.get(self.__class__.__name__).bind(
            run_id=self.run_id, pipeline=self.pipeline_code
        )
        options = StageExecutionOptions(
            run_tag=run_tag,
            mode=mode,
            extended=extended,
            dry_run=self.dry_run,
            sample=sample,
            limit=limit,
            include_qc_metrics=include_qc_metrics,
            fail_on_schema_drift=fail_on_schema_drift,
        )

        logger.info("STAGE_RUN_START", stage="prepare_run")
        self.prepare_run(options)

        target_dir, artifacts = self.plan_run_artifacts(output_dir, run_tag, mode)
        stage_context = StageContext(
            logger=logger,
            request_id=self.run_id,
            config=self.config,
            output_dir=target_dir,
            artifacts=artifacts,
        )

        stage_descriptors = self.build_stage_plan(stage_context, options)
        stage_factory = self.create_stage_factory()
        stages = stage_factory.build(stage_descriptors, stage_context)
        self.stage_plan = stages
        durations, error, qc_path = self.stage_plan_executor.execute(
            stages,
            stage_context,
            options,
            include_qc_metrics=include_qc_metrics,
        )

        if options.extended and self.dry_run:
            metadata_writer = None
            if self.write_service is not None:
                metadata_writer = getattr(self.write_service, "write_metadata", None)
                if callable(metadata_writer):
                    metadata_writer(target_dir, artifacts, stage_context.current_df, dry_run=True)
            if metadata_writer is None:
                legacy_writer = getattr(self, "_write_metadata", None)
                if callable(legacy_writer):  # pragma: no cover - defensive
                    legacy_writer(target_dir, stage_context.current_df)

        result_frame = stage_context.current_df
        rows = 0 if not isinstance(result_frame, pd.DataFrame) else int(result_frame.shape[0])
        success = error is None
        metadata = self.build_run_metadata(stage_context, stages, durations, run_tag, mode)
        metadata["rows"] = rows
        if qc_path is not None:
            metadata["qc_metrics_path"] = str(qc_path)

        run_result = RunResult(
            success=success,
            rows=rows,
            artifacts=RunArtifacts(
                output_dir=target_dir,
                logs_directory=self.resolve_logs_directory(target_dir),
                write_artifacts=stage_context.artifacts or WriteArtifacts(),
                qc_metrics_path=qc_path,
            ),
            duration_ms=durations,
            error=error,
            metadata=metadata,
        )
        self.finalize_run(run_result)
        logger.info("STAGE_RUN_END", stage="pipeline", success=success)
        return run_result

    # Hooks ---------------------------------------------------------------
    def prepare_run(self, options: StageExecutionOptions) -> None:  # pragma: no cover - optional hook
        """Вызывается перед началом extract."""

    def finalize_run(self, run_result: RunResult) -> None:  # pragma: no cover
        """Вызывается после завершения write."""

    # Planning ------------------------------------------------------------
    def create_stage_factory(self) -> StageFactory:
        return StageFactory(self.pipeline_definition)

    @abstractmethod
    def build_stage_plan(
        self, context: StageContext, options: StageExecutionOptions
    ) -> tuple[StageDescriptor, ...]:
        """Construct a deterministic stage descriptor plan for the pipeline."""

        return self.stage_factory.build(context, options)

    def plan_run_artifacts(
        self, output_dir: Path, run_tag: str | None, mode: str | None
    ) -> tuple[Path, WriteArtifacts]:
        return self.artifact_planner.plan(output_dir, self.pipeline_code, run_tag, mode)

    def build_run_stem(self, run_tag: str | None, mode: str | None) -> str:
        suffix = [self.pipeline_code]
        if mode:
            suffix.append(mode)
        if run_tag:
            suffix.append(run_tag)
        return self.deterministic_folder_prefix + "-".join(suffix)

    def build_run_metadata(
        self,
        context: StageContext,
        stage_plan: Iterable[Stage],
        durations: Mapping[str, int],
        run_tag: str | None,
        mode: str | None,
    ) -> dict[str, Any]:
        return self.run_metadata_builder.build(
            context, stage_plan, durations, run_tag, mode
        )

    def resolve_logs_directory(self, output_dir: Path) -> Path:
        return output_dir / "logs"

    # Metadata ------------------------------------------------------------
    def _resolve_pipeline_code(
        self, config: Mapping[str, Any] | Any, definition: PipelineDefinition | None
    ) -> str:
        pipeline_name = None
        if definition is not None:
            pipeline_name = getattr(definition, "metadata", None)
            if pipeline_name and isinstance(pipeline_name, Mapping):
                pipeline_name = pipeline_name.get("name")
            elif hasattr(definition, "name"):
                pipeline_name = getattr(definition, "name")

        if pipeline_name:
            return str(pipeline_name)

        pipeline = getattr(config, "pipeline", None)
        if pipeline is not None and getattr(pipeline, "name", None):
            return str(pipeline.name)
        return self.__class__.__name__

    def _build_default_definition(self, config: Mapping[str, Any] | Any) -> PipelineDefinition:
        pipeline = getattr(config, "pipeline", None)
        name = getattr(pipeline, "name", None) if pipeline is not None else None
        resolved_name = str(name) if name else self.__class__.__name__
        return PipelineDefinition(name=resolved_name, runtime_factory=self.__class__)

    # Status --------------------------------------------------------------
    def stop(self) -> None:  # pragma: no cover - lifecycle hook
        """Gracefully stop pipeline execution if supported."""

    def status(self) -> Mapping[str, Any]:  # pragma: no cover - lifecycle hook
        """Return runtime status details."""
        return {}


__all__ = [
    "PipelineRuntimeBase",
    "StagePlanExecutor",
    "RunMetadataBuilder",
]
