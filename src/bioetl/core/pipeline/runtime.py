"""Shared runtime primitives for orchestrating ETL pipelines."""
from __future__ import annotations

import hashlib
import json
import subprocess
import time
import uuid
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Iterable, Mapping

import pandas as pd
import yaml

from bioetl.core.logging import UnifiedLogger
from bioetl.core.pipeline.types import (
    PipelineBaseProtocol,
    PipelineStageCommand,
    RunArtifacts,
    RunResult,
    StageContext,
    StageExecutionOptions,
    WriteArtifacts,
)


class QCMetricsExecutor:
    """Placeholder QC executor to keep orchestration hook observable."""

    def execute(self, df: pd.DataFrame, output_dir: Path) -> Path:
        output_dir.mkdir(parents=True, exist_ok=True)
        qc_path = output_dir / "qc_metrics.json"
        qc_payload = {"rows": int(df.shape[0])}
        qc_path.write_text(json.dumps(qc_payload, indent=2))
        return qc_path


def _execute_stage_plan(
    stage_plan: Iterable[PipelineStageCommand],
    context: StageContext,
    options: StageExecutionOptions,
    *,
    include_qc_metrics: bool,
) -> tuple[dict[str, int], str | None, Path | None]:
    logger = context.logger
    durations: dict[str, int] = {}
    error: str | None = None
    artifacts = context.artifacts or WriteArtifacts()
    qc_metrics_path: Path | None = None

    for command in stage_plan:
        started = time.perf_counter()
        if logger:
            logger.info("STAGE_RUN_START", stage=command.name)
        try:
            result = command.handler(context, options)
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

    if error is None and include_qc_metrics and context.current_df is not None:
        qc_metrics_path = QCMetricsExecutor().execute(context.current_df, context.output_dir)
        context.artifacts = artifacts

    return durations, error, qc_metrics_path


class PipelineRuntimeBase(ABC, PipelineBaseProtocol):
    """Common runtime for pipelines that orchestrate ETL stage plans."""

    deterministic_folder_prefix: str = "_"

    def __init__(
        self,
        config: Mapping[str, Any] | Any,
        *,
        run_id: str | None = None,
        validator: Any | None = None,
    ) -> None:
        self.config = config
        self.run_id = run_id or uuid.uuid4().hex
        self.validator = validator
        self.dry_run = False
        self.pipeline_code = self._resolve_pipeline_code(config)
        self._git_commit = self._resolve_git_commit()
        self._config_hash = self._compute_config_hash(config)

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
        durations, error, qc_path = _execute_stage_plan(
            stage_plan, context, options, include_qc_metrics=include_qc_metrics
        )

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
    @abstractmethod
    def build_stage_plan(
        self, context: StageContext, options: StageExecutionOptions
    ) -> tuple[PipelineStageCommand, ...]:
        """Construct a deterministic stage plan for the pipeline."""

    def plan_run_artifacts(
        self, output_dir: Path, run_tag: str | None, mode: str | None
    ) -> tuple[Path, WriteArtifacts]:
        output_dir.mkdir(parents=True, exist_ok=True)
        artifacts = WriteArtifacts(data_path=output_dir / f"{self.pipeline_code}.csv")
        return output_dir, artifacts

    def build_run_metadata(
        self,
        context: StageContext,
        stage_plan: Iterable[PipelineStageCommand],
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

    def resolve_logs_directory(self, output_dir: Path) -> Path:
        return output_dir / "logs"

    # Metadata ------------------------------------------------------------
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

    def _resolve_pipeline_code(self, config: Mapping[str, Any] | Any) -> str:
        pipeline = getattr(config, "pipeline", None)
        if pipeline is not None and getattr(pipeline, "name", None):
            return str(pipeline.name)
        return self.__class__.__name__


__all__ = ["PipelineRuntimeBase", "QCMetricsExecutor", "_execute_stage_plan"]
