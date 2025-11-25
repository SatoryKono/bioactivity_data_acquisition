from __future__ import annotations

import time
from abc import abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Generic, Mapping, Sequence, TypeVar

import pandas as pd
import pandera as pa

from bioetl.core.io import ArtifactWriter
from bioetl.core.pipeline.runtime import PipelineRuntimeBase
from bioetl.core.pipeline.stage_plan import build_default_stage_plan
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


class PipelineBase(PipelineRuntimeBase):
    """Интерфейс стадий пайплайна."""

    def __init__(
        self,
        config: Mapping[str, Any],
        *,
        run_id: str | None = None,
        validator: pa.DataFrameSchema | None = None,
        artifact_writer: ArtifactWriter | None = None,
    ) -> None:
        super().__init__(config, run_id=run_id, validator=validator)
        self.artifact_writer = artifact_writer or ArtifactWriter(
            pipeline_code=self.pipeline_code,
            run_id=self.run_id,
            git_commit=self._git_commit,
            config_hash=self._config_hash,
        )

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

    @property
    def pipeline_name(self) -> str:
        return self.pipeline_code

    # Hooks ---------------------------------------------------------------
    def prepare_run(self, options: StageExecutionOptions) -> None:  # pragma: no cover - optional hook
        """Вызывается перед началом extract."""

    def finalize_run(self, result: RunResult) -> None:  # pragma: no cover
        """Вызывается после завершения write."""


class UnifiedPipelineBase(PipelineBase):
    """Базовая реализация общего жизненного цикла ETL."""

    def build_stage_plan(
        self, context: StageContext, options: StageExecutionOptions
    ) -> tuple[PipelineStageCommand, ...]:
        return build_default_stage_plan(self, context, options)

    # Stage helpers ------------------------------------------------------
    def _validate_with_schema(self, df: pd.DataFrame) -> pd.DataFrame:
        if self.validator is None:
            return df
        return self.validator.validate(df)

    def _empty_frame_from_schema(self) -> pd.DataFrame:
        if self.validator is None:
            return pd.DataFrame()
        columns = {name: pd.Series(dtype=str(schema.dtype)) for name, schema in self.validator.columns.items()}
        return pd.DataFrame(columns)

    # Default save_results ------------------------------------------------
    def save_results(
        self, df: pd.DataFrame, artifacts: WriteArtifacts, options: StageExecutionOptions
    ) -> WriteResult:
        output_dir = artifacts.data_path.parent if artifacts.data_path else Path.cwd()
        return self.artifact_writer.write(
            df,
            artifacts,
            output_dir=output_dir,
            dry_run=self.dry_run,
            extended=options.extended,
        )


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
        from bioetl.pipelines.chembl.batch_executor import (
            execute_chembl_batches,
        )

        dataframe, stats = execute_chembl_batches(
            fetcher,
            ids,
            batch_size=batch_kwargs.get("batch_size"),
        )

        finalize_start = time.perf_counter()
        dataframe = finalizer(dataframe)
        stats.rows = int(dataframe.shape[0])
        stats.duration_seconds += time.perf_counter() - finalize_start
        return dataframe, stats

