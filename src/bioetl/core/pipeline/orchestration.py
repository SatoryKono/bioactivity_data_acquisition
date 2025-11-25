"""Orchestration primitives for unified pipeline lifecycle."""
from __future__ import annotations

import hashlib
import json
import subprocess
import time
from abc import ABC
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import pandas as pd
import yaml

from bioetl.core.logging import UnifiedLogger
from bioetl.core.pipeline.factory import StageFactory
from bioetl.core.pipeline.types import (
    PipelineConfig,
    PipelineStageCommand,
    PipelineStagesProtocol,
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


class PipelineBaseCommon(ABC, PipelineStagesProtocol):
    """Shared orchestration helpers for ETL pipelines."""

    deterministic_folder_prefix: str = "_"

    def __init__(self, config: PipelineConfig, run_id: str) -> None:
        self._init_common(config, run_id)

    def _init_common(self, config: PipelineConfig, run_id: str) -> None:
        """Initialize shared pipeline state."""

        self.config = config
        self.run_id = run_id
        self.pipeline_code = config.pipeline.name
        self.output_root = Path(config.materialization.root)
        self.logs_directory = self.output_root.parent / "logs" / self.pipeline_code
        self.stage_plan: tuple[PipelineStageCommand, ...] = ()
        self._git_commit = self._resolve_git_commit()
        self._config_hash = self._compute_config_hash(config)

    # Hook methods -----------------------------------------------------
    def pre_transform(self, df: pd.DataFrame) -> pd.DataFrame:
        return df

    def domain_enrich(self, df: pd.DataFrame) -> pd.DataFrame:
        return df

    def post_transform(self, df: pd.DataFrame) -> pd.DataFrame:
        return df

    def augment_metadata(self, metadata: dict[str, Any]) -> dict[str, Any]:
        return metadata

    # Factory helpers --------------------------------------------------
    def create_stage_factory(self) -> StageFactory:
        return StageFactory(self)

    # Orchestration ----------------------------------------------------
    def run(
        self,
        output_dir: Path,
        *,
        run_tag: str | None = None,
        mode: str | None = None,
        extended: bool = False,
        dry_run: bool = False,
        sample: int | None = None,
        limit: int | None = None,
        include_qc_metrics: bool = False,
        fail_on_schema_drift: bool = True,
    ) -> RunResult:
        logger = UnifiedLogger.get(self.__class__.__name__).bind(
            run_id=self.run_id, pipeline=self.pipeline_code
        )
        options = StageExecutionOptions(
            run_tag=run_tag,
            mode=mode,
            extended=extended,
            dry_run=dry_run,
            sample=sample,
            limit=limit,
            include_qc_metrics=include_qc_metrics,
            fail_on_schema_drift=fail_on_schema_drift,
        )

        logger.info("STAGE_RUN_START", stage="prepare_run")
        self.prepare_run(options)

        target_dir, artifacts = self.plan_run_artifacts(output_dir, run_tag, mode)
        context = StageContext(
            pipeline=self,
            output_dir=target_dir,
            logger=logger,
            run_id=self.run_id,
            run_tag=run_tag,
            mode=mode,
            descriptor=None,
            artifacts=artifacts,
        )

        factory = self.create_stage_factory()
        self.stage_plan = factory.build(options)
        durations, error, qc_path = _execute_stage_plan(
            self.stage_plan, context, options, include_qc_metrics=include_qc_metrics
        )

        rows = 0 if context.current_df is None else int(context.current_df.shape[0])
        success = error is None
        metadata = self.augment_metadata(
            {
                "stage_plan": [cmd.name for cmd in self.stage_plan],
                "extract_metadata": context.metadata,
                "started_at": datetime.now(timezone.utc).isoformat(),
                "git_commit": self._git_commit,
                "config_hash": self._config_hash,
            }
        )

        run_result = RunResult(
            success=success,
            rows=rows,
            artifacts=RunArtifacts(
                output_dir=target_dir,
                logs_directory=self.logs_directory,
                write_artifacts=context.artifacts,
                qc_metrics_path=qc_path,
            ),
            duration_ms=durations,
            error=error,
            metadata=metadata,
        )
        self.finalize_run(run_result)
        logger.info("STAGE_RUN_END", stage="pipeline", success=success)
        return run_result

    # Deterministic layout ---------------------------------------------
    def build_run_stem(self, run_tag: str | None, mode: str | None) -> str:
        suffix = [self.pipeline_code]
        if mode:
            suffix.append(mode)
        if run_tag:
            suffix.append(run_tag)
        return self.deterministic_folder_prefix + "-".join(suffix)

    def plan_run_artifacts(
        self, output_dir: Path, run_tag: str | None, mode: str | None
    ) -> tuple[Path, WriteArtifacts]:
        run_stem = self.build_run_stem(run_tag, mode)
        target_dir = output_dir / run_stem
        target_dir.mkdir(parents=True, exist_ok=True)
        artifacts = WriteArtifacts(data_path=target_dir / f"{self.pipeline_code}.csv")
        return target_dir, artifacts

    # Metadata helpers --------------------------------------------------
    def _resolve_git_commit(self) -> str | None:
        try:
            completed = subprocess.run(
                ["git", "rev-parse", "HEAD"], capture_output=True, check=True, text=True
            )
        except Exception:  # pragma: no cover - defensive resolution
            return None
        return completed.stdout.strip() or None

    def _compute_config_hash(self, config: PipelineConfig) -> str | None:
        try:
            serialized = yaml.safe_dump(config.__dict__, sort_keys=True).encode("utf-8")
            return hashlib.sha256(serialized).hexdigest()
        except Exception:  # pragma: no cover - defensive hashing
            return None


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


__all__ = ["PipelineBaseCommon", "QCMetricsExecutor"]
