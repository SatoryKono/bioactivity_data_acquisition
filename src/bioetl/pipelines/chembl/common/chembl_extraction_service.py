from __future__ import annotations

"""Сервис, инкапсулирующий общие шаги извлечения ChEMBL."""

import time
from collections.abc import Iterable
from typing import Any, Callable, Mapping, Sequence

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

    def _normalize_ids(
        self, ids: Sequence[str] | None, context: Mapping[str, Any]
    ) -> list[str] | None:
        if ids:
            return [str(item) for item in ids]
        context_ids = context.get("ids")
        if isinstance(context_ids, Sequence) and not isinstance(
            context_ids, (str, bytes, bytearray)
        ):
            return [str(item) for item in context_ids if str(item)]
        return None

    @staticmethod
    def _resolve_page_size(context: Mapping[str, Any], default: int = 1000) -> int:
        page_size = context.get("page_size")
        if isinstance(page_size, int) and page_size > 0:
            return page_size
        return default

    def _build_client_fetcher(self, chembl_client: Any, *, page_size: int) -> Callable[[Sequence[str] | None], Any]:
        def fetch(batch: Sequence[str] | None):
            if batch:
                return chembl_client.fetch_by_ids(batch)
            result = chembl_client.fetch_page(page_size=page_size)
            return list(result) if isinstance(result, Iterable) else result

        return fetch

    def _resolve_fetcher(
        self,
        descriptor: Any,
        context: Mapping[str, Any],
        *,
        page_size: int,
    ) -> Callable[[Sequence[str] | None], Any]:
        factory = getattr(descriptor, "fetcher_factory", None)
        if callable(factory):
            fetcher = factory(context)
            if callable(fetcher):
                return fetcher

        chembl_client = context.get("chembl_client")
        if chembl_client is None:
            raise RuntimeError("chembl_client is required to build a fetcher")
        return self._build_client_fetcher(chembl_client, page_size=page_size)

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

        ids_to_fetch = self._normalize_ids(ids, context)
        page_size = self._resolve_page_size(context)
        fetcher = self._resolve_fetcher(descriptor, context, page_size=page_size)
        finalizer = descriptor.finalizer_factory(context)

        dataframe, stats = execute_chembl_batches(
            fetcher,
            ids_to_fetch,
            batch_size=batch_kwargs.get("batch_size"),
        )

        dataframe, stats = self.finalize_dataframe(dataframe, finalizer, stats)
        return dataframe, stats


__all__ = ["ChemblExtractionService"]
