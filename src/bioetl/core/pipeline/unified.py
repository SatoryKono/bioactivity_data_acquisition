from __future__ import annotations

import hashlib
import json
import subprocess
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Generic, Mapping, Sequence, TypeVar

import pandas as pd
import pandera as pa
import yaml

from bioetl.core.logging import UnifiedLogger
from bioetl.core.io.artifacts import RunArtifacts
from bioetl.core.pipeline.orchestration import _execute_stage_plan
from bioetl.core.pipeline.types import (
    PipelineStageCommand,
    RunResult,
    StageContext,
    StageExecutionOptions,
    WriteArtifacts,
    WriteResult,
)


@dataclass(slots=True)
class BatchExtractionStats:
    """Сводные статистики по батчевой выборке."""

    rows: int
    api_calls: int
    cache_hits: int
    success_count: int
    fallback_count: int
    error_count: int
    duration_seconds: float


class PipelineBase(ABC):
    """Интерфейс стадий пайплайна."""

    def __init__(
        self,
        config: Mapping[str, Any],
        *,
        run_id: str | None = None,
        validator: pa.DataFrameSchema | None = None,
    ) -> None:
        self.config = config
        self.run_id = run_id or uuid.uuid4().hex
        self.validator = validator
        self.dry_run = False

    @property
    def pipeline_name(self) -> str:
        return self.__class__.__name__

    @abstractmethod
    def extract(self, descriptor: Any | None, options: StageExecutionOptions) -> pd.DataFrame:
        ...

    @abstractmethod
    def transform(self, df: pd.DataFrame, options: StageExecutionOptions) -> pd.DataFrame:
        ...

    @abstractmethod
    def validate(self, df: pd.DataFrame, options: StageExecutionOptions) -> pd.DataFrame:
        ...

    @abstractmethod
    def save_results(
        self, df: pd.DataFrame, artifacts: WriteArtifacts, options: StageExecutionOptions
    ) -> WriteResult:
        ...

    @abstractmethod
    def run(self, output_dir: Path, **kwargs: Any) -> RunResult:
        ...

    # Hooks ---------------------------------------------------------------
    def prepare_run(self, options: StageExecutionOptions) -> None:  # pragma: no cover - optional hook
        """Вызывается перед началом extract."""

    def finalize_run(self, result: RunResult) -> None:  # pragma: no cover
        """Вызывается после завершения write."""


class UnifiedPipelineBase(PipelineBase):
    """Базовая реализация общего жизненного цикла ETL."""

    def __init__(
        self,
        config: Mapping[str, Any],
        *,
        run_id: str | None = None,
        validator: pa.DataFrameSchema | None = None,
    ) -> None:
        super().__init__(config, run_id=run_id, validator=validator)
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
        limit: int | None = None,
        sample: int | None = None,
        include_qc_metrics: bool = False,
        fail_on_schema_drift: bool = True,
    ) -> RunResult:
        if dry_run is not None:
            self.dry_run = dry_run

        logger = UnifiedLogger.get(self.__class__.__name__).bind(
            run_id=self.run_id, pipeline=self.pipeline_name
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

        output_dir.mkdir(parents=True, exist_ok=True)
        artifacts = WriteArtifacts(data_path=output_dir / f"{self.pipeline_name}.csv")
        context = StageContext(
            pipeline=self,  # type: ignore[arg-type]
            output_dir=output_dir,
            logger=logger,
            run_id=self.run_id,
            run_tag=run_tag,
            mode=mode,
            descriptor=None,
            artifacts=artifacts,
        )

        def _run_extract(stage_context: StageContext, exec_options: StageExecutionOptions) -> pd.DataFrame:
            if exec_options.dry_run:
                frame = self._empty_frame_from_schema()
            else:
                frame = self.extract(stage_context.descriptor, exec_options)
            if exec_options.limit is not None and frame is not None:
                frame = frame.head(exec_options.limit)
            if exec_options.sample is not None and frame is not None and not frame.empty:
                frame = frame.sample(min(exec_options.sample, len(frame)), random_state=0)
            stage_context.current_df = frame
            return frame

        def _run_transform(stage_context: StageContext, exec_options: StageExecutionOptions) -> pd.DataFrame:
            if stage_context.current_df is None:
                raise RuntimeError("transform stage requires extracted data")
            stage_context.current_df = self.transform(stage_context.current_df, exec_options)
            return stage_context.current_df

        def _run_validate(stage_context: StageContext, exec_options: StageExecutionOptions) -> pd.DataFrame:
            if stage_context.current_df is None:
                raise RuntimeError("validate stage requires transformed data")
            frame = self._validate_with_schema(stage_context.current_df)
            frame = self.validate(frame, exec_options)
            stage_context.current_df = self._sort_dataframe(frame)
            return stage_context.current_df

        def _run_save_results(
            stage_context: StageContext, exec_options: StageExecutionOptions
        ) -> WriteResult:
            if stage_context.current_df is None:
                raise RuntimeError("save_results stage requires validated data")
            artifacts = stage_context.artifacts or WriteArtifacts(
                data_path=stage_context.output_dir / f"{self.pipeline_name}.csv"
            )
            stage_context.artifacts = artifacts
            result = self.save_results(stage_context.current_df, artifacts, exec_options)
            if result.artifacts:
                stage_context.artifacts = result.artifacts
            stage_context.metadata.setdefault("write_result", result)
            return result

        stage_plan = (
            PipelineStageCommand("extract", _run_extract),
            PipelineStageCommand("transform", _run_transform),
            PipelineStageCommand("validate", _run_validate),
            PipelineStageCommand("save_results", _run_save_results),
        )

        if options.dry_run:
            stage_plan = tuple(cmd for cmd in stage_plan if cmd.name != "save_results")

        durations, error, qc_path = _execute_stage_plan(
            stage_plan, context, options, include_qc_metrics=include_qc_metrics
        )

        if options.dry_run and options.extended:
            self._write_metadata(output_dir, context.current_df)

        success = error is None
        rows = 0 if context.current_df is None else int(context.current_df.shape[0])
        metadata: dict[str, Any] = {
            "stage_plan": [cmd.name for cmd in stage_plan],
            "extract_metadata": context.metadata,
            "git_commit": self._git_commit,
            "config_hash": self._config_hash,
            "pipeline": self.pipeline_name,
            "run_tag": run_tag,
            "mode": mode,
            "duration_seconds": sum(durations.values()) / 1000,
        }
        if context.artifacts and context.artifacts.data_path:
            metadata["output_path"] = str(context.artifacts.data_path)
        if qc_path is not None:
            metadata["qc_metrics_path"] = str(qc_path)
        run_result = RunResult(
            success=success,
            rows=rows,
            artifacts=RunArtifacts(
                output_dir=output_dir,
                logs_directory=output_dir / "logs",
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

    # Stage helpers ------------------------------------------------------
    def _validate_with_schema(self, df: pd.DataFrame) -> pd.DataFrame:
        if self.validator is None:
            return df
        return self.validator.validate(df)

    def _sort_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        columns = sorted(df.columns)
        return df.loc[:, columns].sort_values(by=columns).reset_index(drop=True)

    def _empty_frame_from_schema(self) -> pd.DataFrame:
        if self.validator is None:
            return pd.DataFrame()
        columns = {name: pd.Series(dtype=str(schema.dtype)) for name, schema in self.validator.columns.items()}
        return pd.DataFrame(columns)

    # Metadata -----------------------------------------------------------
    def _write_metadata(self, output_dir: Path, df: pd.DataFrame | None) -> None:
        meta_path = output_dir / "meta.yaml"
        manifest_path = output_dir / "run_manifest.json"
        payload = {
            "run_id": self.run_id,
            "pipeline": self.pipeline_name,
            "git_commit": self._git_commit,
            "config_hash": self._config_hash,
            "rows": 0 if df is None else int(df.shape[0]),
            "columns": [] if df is None else list(df.columns),
            "dry_run": self.dry_run,
        }
        meta_path.write_text(yaml.safe_dump(payload, allow_unicode=True))
        manifest = {
            "run_id": self.run_id,
            "artifacts": {
                "meta": meta_path.name,
            },
            "metrics": payload,
        }
        manifest_path.write_text(json.dumps(manifest, indent=2))

    # Utils --------------------------------------------------------------
    def _resolve_git_commit(self) -> str | None:
        try:
            completed = subprocess.run(
                ["git", "rev-parse", "HEAD"], capture_output=True, check=True, text=True
            )
        except Exception:
            return None
        return completed.stdout.strip() or None

    def _compute_config_hash(self, config: Mapping[str, Any]) -> str:
        serialized = yaml.safe_dump(dict(config), sort_keys=True).encode("utf-8")
        return hashlib.sha256(serialized).hexdigest()

    # Default save_results ------------------------------------------------
    def save_results(
        self, df: pd.DataFrame, artifacts: WriteArtifacts, options: StageExecutionOptions
    ) -> WriteResult:
        output_dir = artifacts.data_path.parent if artifacts.data_path else Path.cwd()
        output_dir.mkdir(parents=True, exist_ok=True)
        dataset_path = artifacts.data_path or output_dir / f"{self.pipeline_name}.csv"
        artifacts.data_path = dataset_path
        tmp_path = dataset_path.with_suffix(dataset_path.suffix + ".tmp")
        df.to_csv(tmp_path, index=False)
        tmp_path.replace(dataset_path)
        if options.extended:
            self._write_metadata(output_dir, df)
        return WriteResult(rows=int(df.shape[0]), artifacts=artifacts)


ChemblPipelineT = TypeVar("ChemblPipelineT", bound="ChemblPipelineBase")


class ChemblExtractionDescriptor(Generic[ChemblPipelineT]):
    """Описание извлечения сущности ChEMBL."""

    def __init__(
        self,
        *,
        build_context: Callable[[ChemblPipelineT], Mapping[str, Any]],
        fetcher_factory: Callable[[Mapping[str, Any]], Callable[[Sequence[str] | None], Any]],
        finalizer_factory: Callable[[Mapping[str, Any]], Callable[[pd.DataFrame], pd.DataFrame]],
    ) -> None:
        self.build_context = build_context
        self.fetcher_factory = fetcher_factory
        self.finalizer_factory = finalizer_factory


class CircuitBreakerOpenError(RuntimeError):
    """Исключение, сигнализирующее о срабатывании circuit breaker."""


class ChemblPipelineBase(UnifiedPipelineBase):
    """Базовый пайплайн для ChEMBL с общей логикой выгрузки дескрипторов."""

    def __init__(self, config: Mapping[str, Any], *, run_id: str | None = None) -> None:
        super().__init__(config, run_id=run_id)
        self._chembl_release: str | None = None

    def resolve_chembl_release(self, chembl_client: Any) -> str:
        if self._chembl_release:
            return self._chembl_release
        status = chembl_client.status()
        release = status.get("chembl_release") if isinstance(status, Mapping) else None
        if not release:
            raise RuntimeError("Не удалось определить chembl_release")
        self._chembl_release = str(release)
        return self._chembl_release

    def run_descriptor_extraction(
        self,
        descriptor: ChemblExtractionDescriptor[ChemblPipelineT],
        ids: Sequence[str] | None,
        *,
        summary_event: str,
        metadata_filters: Mapping[str, Any] | None = None,
        fetch_mode: str = "default",
        **batch_kwargs: Any,
    ) -> tuple[pd.DataFrame, BatchExtractionStats]:
        context = dict(descriptor.build_context(self))
        if metadata_filters:
            context["metadata_filters"] = metadata_filters
        context["fetch_mode"] = fetch_mode
        chembl_client = context.get("chembl_client")
        if chembl_client is not None:
            context["chembl_release"] = self.resolve_chembl_release(chembl_client)

        if self.dry_run:
            empty = pd.DataFrame()
            stats = BatchExtractionStats(
                rows=0,
                api_calls=0,
                cache_hits=0,
                success_count=0,
                fallback_count=0,
                error_count=0,
                duration_seconds=0.0,
            )
            return empty, stats

        fetcher = descriptor.fetcher_factory(context)
        finalizer = descriptor.finalizer_factory(context)
        batch_size = min(int(batch_kwargs.get("batch_size", 25)), 25)
        from bioetl.pipelines.chembl.batch_executor import execute_batch_extraction

        dataframe, stats = execute_batch_extraction(fetcher, ids=ids, batch_size=batch_size)
        dataframe = finalizer(dataframe)
        stats.rows = int(dataframe.shape[0])
        return dataframe, stats

