from __future__ import annotations

"""Сервис, инкапсулирующий общие шаги извлечения ChEMBL."""

import time
from typing import Any, Mapping, Sequence

import pandas as pd

from bioetl.core.pipeline.unified import BatchExtractionStats
from bioetl.pipelines.chembl.batch_executor import execute_chembl_batches


class ChemblExtractionService:
    """Работает с контекстом выборки, релизом ChEMBL и финализацией выборок."""

    def __init__(self) -> None:
        self._chembl_release: str | None = None

    @property
    def chembl_release(self) -> str | None:
        return self._chembl_release

    def resolve_chembl_release(self, chembl_client: Any) -> str:
        if self._chembl_release:
            return self._chembl_release
        status = chembl_client.status()
        release = status.get("chembl_release") if isinstance(status, Mapping) else None
        if not release:
            raise RuntimeError("Не удалось определить chembl_release")
        self._chembl_release = str(release)
        return self._chembl_release

    def build_context(
        self,
        descriptor: Any,
        pipeline: Any,
        *,
        metadata_filters: Mapping[str, Any] | None = None,
        fetch_mode: str = "default",
    ) -> dict[str, Any]:
        context = dict(descriptor.build_context(pipeline))
        if metadata_filters:
            context["metadata_filters"] = metadata_filters
        context["fetch_mode"] = fetch_mode
        chembl_client = context.get("chembl_client")
        if chembl_client is not None:
            context["chembl_release"] = self.resolve_chembl_release(chembl_client)
        return context

    def finalize_dataframe(
        self,
        dataframe: pd.DataFrame,
        finalizer: Any,
        stats: BatchExtractionStats,
    ) -> tuple[pd.DataFrame, BatchExtractionStats]:
        finalize_start = time.perf_counter()
        dataframe = finalizer(dataframe)
        stats.rows = int(dataframe.shape[0])
        stats.duration_seconds += time.perf_counter() - finalize_start
        return dataframe, stats

    def run_descriptor_extraction(
        self,
        pipeline: Any,
        descriptor: Any,
        ids: Sequence[str] | None,
        *,
        summary_event: str,
        metadata_filters: Mapping[str, Any] | None = None,
        fetch_mode: str = "default",
        **batch_kwargs: Any,
    ) -> tuple[pd.DataFrame, BatchExtractionStats]:
        context = self.build_context(
            descriptor, pipeline, metadata_filters=metadata_filters, fetch_mode=fetch_mode
        )

        if pipeline.dry_run:
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

        dataframe, stats = execute_chembl_batches(
            fetcher,
            ids,
            batch_size=batch_kwargs.get("batch_size"),
        )

        dataframe, stats = self.finalize_dataframe(dataframe, finalizer, stats)
        return dataframe, stats


__all__ = ["ChemblExtractionService"]
