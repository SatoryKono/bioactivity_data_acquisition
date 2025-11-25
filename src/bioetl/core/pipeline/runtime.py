"""Shared runtime primitives for orchestrating ETL pipelines."""
from __future__ import annotations

import hashlib
import subprocess
import time
import uuid
from abc import ABC
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping

import pandas as pd
import yaml

from bioetl.core.logging import UnifiedLogger
from bioetl.core.pipeline.definition import PipelineDefinition
from bioetl.core.pipeline.factory import StageFactory
from bioetl.core.pipeline.services import ArtifactPlanner, DefaultArtifactPlanner, MetadataService, QCService
from bioetl.core.pipeline.types import (
    PipelineBaseProtocol,
    RunArtifacts,
    RunResult,
    Stage,
    StageContext,
    StageExecutionOptions,
    WriteArtifacts,
)


class StagePlanExecutor:
    """Ответственный за исполнение плана стадий и подсчет длительностей."""

    def execute(
        self,
        stage_plan: Iterable[Stage],
        context: StageContext,
        options: StageExecutionOptions,
    ) -> tuple[dict[str, int], str | None]:
        logger = context.logger
        durations: dict[str, int] = {}
        error: str | None = None
        artifacts = context.artifacts or WriteArtifacts()

        for command in stage_plan:
            started = time.perf_counter()
            if logger:
                logger.info("STAGE_RUN_START", stage=command.name)
            try:
                result = command.run(context, options)
                if command.name == "extract" and isinstance(result, pd.DataFrame):
                    context.metadata["extract_rows"] = int(result.shape[0])
                if command.name == "save_results" and hasattr(result, "artifacts"):
                    artifacts = result.artifacts  # type: ignore[attr-defined]
                    if isinstance(artifacts, WriteArtifacts):
                        context.artifacts = artifacts
            except Exception as exc:  # pragma: no cover - surfaced via RunResult
                error = str(exc)
                if logger:
                    logger.error("STAGE_RUN_ERROR", stage=command.name, error=error)
                break
            finally:
                duration_ms = int((time.perf_counter() - started) * 1000)
                durations[command.name] = duration_ms
                if logger:
                    logger.info("STAGE_RUN_END", stage=command.name, duration_ms=duration_ms)
        return durations, error


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
        stage_plan: Iterable[Stage],
        durations: Mapping[str, int],
        run_tag: str | None,
        mode: str | None,
    ) -> dict[str, Any]:
        metadata: dict[str, Any] = {
            "stage_plan": [cmd.name for cmd in stage_plan],
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
        pipeline_definition: PipelineDefinition,
        *,
        run_id: str | None = None,
        validator: Any | None = None,
        stage_factory: StageFactory | None = None,
        stage_plan_executor: StagePlanExecutor | None = None,
        artifact_planner: ArtifactPlanner | None = None,
        qc_service: QCService | None = None,
        metadata_service: MetadataService | None = None,
        run_metadata_builder: RunMetadataBuilder | None = None,
    ) -> None:
        self.config = config
        self.pipeline_definition = pipeline_definition
        self.run_id = run_id or uuid.uuid4().hex
        self.validator = validator
        self.pipeline_code = self._resolve_pipeline_code(config, pipeline_definition)
        self.stage_factory = stage_factory or StageFactory(pipeline_definition)
        self.stage_plan_executor = stage_plan_executor or StagePlanExecutor()
        self.artifact_planner = artifact_planner or DefaultArtifactPlanner()
        builder = run_metadata_builder or RunMetadataBuilder(config, self.pipeline_code)
        self.metadata_service = metadata_service or MetadataService(builder)
        self.qc_service = qc_service or QCService()
        self.dry_run = False
        self._git_commit = builder.git_commit
        self._config_hash = builder.config_hash

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
        context = StageContext(
            pipeline=self,  # type: ignore[arg-type]
            output_dir=target_dir,
            logger=logger,
            run_id=self.run_id,
            run_tag=run_tag,
            mode=mode,
            descriptor=None,
            artifacts=artifacts,
        )

        stage_plan = self.build_stage_plan(context, options)
        self.stage_plan = stage_plan
        durations, error = self.stage_plan_executor.execute(stage_plan, context, options)
        qc_path: Path | None = None
        if error is None and include_qc_metrics:
            qc_path = self.qc_service.execute(context, options)

        if options.extended and self.dry_run:
            metadata_writer = getattr(self, "_write_metadata", None)
            if callable(metadata_writer):  # pragma: no cover - defensive
                metadata_writer(target_dir, context.current_df)

        rows = 0 if context.current_df is None else int(context.current_df.shape[0])
        success = error is None
        metadata = self.build_run_metadata(context, stage_plan, durations, run_tag, mode)
        metadata["rows"] = rows
        if qc_path is not None:
            metadata["qc_metrics_path"] = str(qc_path)

        run_result = RunResult(
            success=success,
            rows=rows,
            artifacts=RunArtifacts(
                output_dir=target_dir,
                logs_directory=self.resolve_logs_directory(target_dir),
                write_artifacts=context.artifacts or WriteArtifacts(),
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
    def build_stage_plan(
        self, context: StageContext, options: StageExecutionOptions
    ) -> tuple[Stage, ...]:
        """Construct a deterministic stage plan for the pipeline."""

        return self.stage_factory.build(context, options)

    def plan_run_artifacts(
        self, output_dir: Path, run_tag: str | None, mode: str | None
    ) -> tuple[Path, WriteArtifacts]:
        return self.artifact_planner.plan(output_dir, self.pipeline_code, run_tag, mode)

    def build_run_metadata(
        self,
        context: StageContext,
        stage_plan: Iterable[Stage],
        durations: Mapping[str, int],
        run_tag: str | None,
        mode: str | None,
    ) -> dict[str, Any]:
        return self.metadata_service.build(context, stage_plan, durations, run_tag, mode)

    def resolve_logs_directory(self, output_dir: Path) -> Path:
        return output_dir / "logs"

    # Metadata ------------------------------------------------------------
    def _resolve_pipeline_code(self, config: Mapping[str, Any] | Any, definition: PipelineDefinition) -> str:
        pipeline_name = definition.metadata.get("name") if definition.metadata else None
        if pipeline_name:
            return str(pipeline_name)
        pipeline = getattr(config, "pipeline", None)
        if pipeline is not None and getattr(pipeline, "name", None):
            return str(pipeline.name)
        return self.__class__.__name__

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
