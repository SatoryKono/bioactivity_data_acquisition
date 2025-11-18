"""Batch extraction strategies used by ChemblPipelineBase.

This module keeps the strategy interfaces separate from
``ChemblPipelineBase`` so that batching logic can be unit-tested in
isolation.  Pipelines can override the strategy factory when they need to
customise normalisation, sizing or fetching semantics.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import time
from collections.abc import Callable, Iterable, Mapping, Sequence
from typing import Any, Protocol, TypeAlias

import pandas as pd
from structlog.stdlib import BoundLogger

from bioetl.chembl.common.descriptor import (
    BatchExtractionContext,
    BatchExtractionStats,
    FetcherCallable,
    FinalizeCallable,
    FinalizeContextCallable,
)
from bioetl.pipelines.base import PipelineBase

TransformCallable: TypeAlias = Callable[[Mapping[str, Any], BatchExtractionContext], Mapping[str, Any]]
NormalizerCallable: TypeAlias = Callable[[Any], tuple[str | None, Any]]
SortKeyCallable: TypeAlias = Callable[[tuple[str, Any]], Any]


@dataclass(slots=True)
class IdNormalizationResult:
    """Result of identifier normalization."""

    unique_ids: tuple[str, ...]
    metadata: dict[str, Any]
    stats: BatchExtractionStats


class IdNormalizationStrategy(Protocol):
    """Normalise raw identifiers before extraction."""

    def normalize(
        self,
        ids: Sequence[Any],
        *,
        limit: int | None,
        id_normalizer: NormalizerCallable | None,
        sort_key: SortKeyCallable | None,
    ) -> IdNormalizationResult:
        """Return canonical ID ordering together with metadata."""


@dataclass(slots=True)
class BatchSizingResult:
    """Batch sizing outcome used by :class:`BatchExtractionContext`."""

    batch_size: int
    chunk_size: int


class BatchSizingStrategy(Protocol):
    """Calculate effective batch and chunk sizes."""

    def calculate(
        self,
        *,
        pipeline: PipelineBase,
        requested_batch_size: int | None,
        requested_chunk_size: int | None,
        max_batch_size: int | None,
    ) -> BatchSizingResult:
        """Return the sizes to be stored on :class:`BatchExtractionContext`."""


class FetchStrategy(Protocol):
    """Execute the extraction plan using the provided fetcher."""

    def fetch(
        self,
        plan: BatchExtractionPlan,
    ) -> tuple[pd.DataFrame, BatchExtractionStats]:
        """Return the dataframe together with extraction stats."""


@dataclass(slots=True)
class BatchExtractionPlan:
    """Encapsulate all inputs required to execute batch extraction."""

    pipeline: PipelineBase
    fetcher: FetcherCallable
    transform_item: TransformCallable | None
    empty_frame_factory: Callable[[], pd.DataFrame] | None
    finalize: FinalizeCallable | None
    finalize_context: FinalizeContextCallable | None
    stats_attribute: str | None
    id_column: str
    limit: int | None
    started_at: float
    fetch_strategy: FetchStrategy
    context: BatchExtractionContext

    def execute(self) -> tuple[pd.DataFrame, BatchExtractionStats]:
        """Execute the plan and persist stats on the pipeline when requested."""

        stats = self.context.stats
        try:
            dataframe, stats = self.fetch_strategy.fetch(self)
            return dataframe, stats
        finally:
            if self.finalize_context is not None:
                self.finalize_context(self.context)

            if self.stats_attribute and hasattr(self.pipeline, self.stats_attribute):
                override = self.context.extra.get("stats_attribute_override")
                payload = override if override is not None else stats.as_dict()
                setattr(self.pipeline, self.stats_attribute, payload)


class SimpleNormalizationStrategy(IdNormalizationStrategy):
    """Deduplicate, sort and limit identifiers deterministically."""

    def normalize(
        self,
        ids: Sequence[Any],
        *,
        limit: int | None,
        id_normalizer: NormalizerCallable | None,
        sort_key: SortKeyCallable | None,
    ) -> IdNormalizationResult:
        if id_normalizer is not None:
            normalizer = id_normalizer
        else:
            normalizer = self._default_normalizer

        canonical_pairs: list[tuple[str, Any]] = []
        seen: set[str] = set()
        for raw in ids:
            canonical, metadata = normalizer(raw)
            if canonical is None:
                continue
            if canonical in seen:
                continue
            seen.add(canonical)
            canonical_pairs.append((canonical, metadata))

        if sort_key is not None:
            canonical_pairs.sort(key=sort_key)
        else:
            canonical_pairs.sort(key=lambda pair: pair[0])

        if limit is not None:
            effective_limit = max(int(limit), 0)
            canonical_pairs = canonical_pairs[:effective_limit]

        unique_ids = tuple(identifier for identifier, _ in canonical_pairs)
        metadata_map = {identifier: payload for identifier, payload in canonical_pairs}
        stats = BatchExtractionStats(requested=len(unique_ids))
        return IdNormalizationResult(unique_ids=unique_ids, metadata=metadata_map, stats=stats)

    @staticmethod
    def _default_normalizer(raw: Any) -> tuple[str | None, Any]:
        if raw is None:
            return None, None
        candidate = str(raw).strip()
        if not candidate:
            return None, None
        return candidate, None


class SimpleBatchSizingStrategy(BatchSizingStrategy):
    """Clamp user-provided values to positive integers and caps."""

    def calculate(
        self,
        *,
        pipeline: PipelineBase,
        requested_batch_size: int | None,
        requested_chunk_size: int | None,
        max_batch_size: int | None,
    ) -> BatchSizingResult:
        batch_size_value = requested_batch_size
        if batch_size_value is None:
            resolve_source_config = getattr(pipeline, "_resolve_source_config", None)
            resolve_batch_size = getattr(pipeline, "_resolve_batch_size", None)
            if callable(resolve_source_config) and callable(resolve_batch_size):
                try:
                    source_config = resolve_source_config("chembl")
                    batch_size_value = resolve_batch_size(source_config)
                except Exception:
                    batch_size_value = None

        batch_size = int(batch_size_value) if batch_size_value else 25
        batch_size = max(batch_size, 1)
        if max_batch_size is not None:
            batch_size = min(batch_size, int(max_batch_size))

        if requested_chunk_size is None:
            chunk_size = batch_size
        else:
            chunk_size = max(int(requested_chunk_size), 1)
            if max_batch_size is not None:
                chunk_size = min(chunk_size, int(max_batch_size))

        return BatchSizingResult(batch_size=batch_size, chunk_size=chunk_size)


class BaseFetchStrategy:
    """Shared helpers for fetch strategies."""

    def _finalize_records(
        self,
        plan: BatchExtractionPlan,
        records: list[Mapping[str, Any]],
    ) -> pd.DataFrame:
        if records:
            dataframe = pd.DataFrame.from_records(records)
        else:
            dataframe = (
                plan.empty_frame_factory()
                if plan.empty_frame_factory is not None
                else pd.DataFrame({plan.id_column: pd.Series(dtype="string")})
            )

        if not dataframe.empty and plan.id_column in dataframe.columns:
            dataframe = dataframe.sort_values(plan.id_column).reset_index(drop=True)

        if plan.finalize is not None:
            dataframe = plan.finalize(dataframe, plan.context)

        return dataframe

    def _apply_transform(
        self,
        plan: BatchExtractionPlan,
        item: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        record = dict(item)
        if plan.transform_item is not None:
            return dict(plan.transform_item(record, plan.context))
        return record

    def _apply_summary(
        self,
        plan: BatchExtractionPlan,
        summary: Mapping[str, Any],
    ) -> None:
        context = plan.context
        stats = context.stats
        context.extra.setdefault("delegated_summary", dict(summary))
        if "batches" in summary and isinstance(summary.get("batches"), int):
            stats.batches = int(summary["batches"])
        if "api_calls" in summary and isinstance(summary.get("api_calls"), int):
            stats.api_calls = int(summary["api_calls"])
        if "cache_hits" in summary and isinstance(summary.get("cache_hits"), int):
            stats.cache_hits = int(summary["cache_hits"])
        extra_payload = {
            key: value
            for key, value in summary.items()
            if key not in {"batches", "api_calls", "cache_hits"}
        }
        if extra_payload:
            stats.set_extra(**extra_payload)


class DefaultFetchStrategy(BaseFetchStrategy, FetchStrategy):
    """Iterate over chunks of IDs and build the dataframe."""

    def fetch(
        self,
        plan: BatchExtractionPlan,
    ) -> tuple[pd.DataFrame, BatchExtractionStats]:
        context = plan.context
        stats = context.stats
        records: list[Mapping[str, Any]] = []
        delegated_summary: Mapping[str, Any] | None = None

        if not context.ids:
            dataframe = self._finalize_records(plan, records)
            stats.rows = int(dataframe.shape[0])
            stats.duration_ms = (time.perf_counter() - plan.started_at) * 1000.0
            return dataframe, stats

        for start_idx in range(0, len(context.ids), context.chunk_size):
            batch_ids = tuple(context.ids[start_idx : start_idx + context.chunk_size])
            context.increment_batches()
            fetch_output = plan.fetcher(batch_ids, context)
            batch_iter: Iterable[Mapping[str, Any]]
            summary: Mapping[str, Any] | None = None
            if isinstance(fetch_output, tuple) and fetch_output:
                batch_iter = fetch_output[0]
                if len(fetch_output) > 1 and isinstance(fetch_output[1], Mapping):
                    summary = fetch_output[1]
            else:
                batch_iter = fetch_output

            for item in batch_iter:
                record = self._apply_transform(plan, item)
                records.append(record)
                if plan.limit is not None and len(records) >= int(plan.limit):
                    break

            if summary is not None:
                delegated_summary = summary

            if plan.limit is not None and len(records) >= int(plan.limit):
                break

        if plan.limit is not None and len(records) > int(plan.limit):
            records = records[: int(plan.limit)]

        dataframe = self._finalize_records(plan, records)
        duration_ms = (time.perf_counter() - plan.started_at) * 1000.0
        stats.rows = int(dataframe.shape[0])
        stats.duration_ms = duration_ms

        if delegated_summary is not None:
            self._apply_summary(plan, delegated_summary)
        return dataframe, stats


class DelegatedFetchStrategy(BaseFetchStrategy, FetchStrategy):
    """Defer batching logic to the fetcher and consume a single payload."""

    def fetch(
        self,
        plan: BatchExtractionPlan,
    ) -> tuple[pd.DataFrame, BatchExtractionStats]:
        context = plan.context
        stats = context.stats
        if not context.ids:
            dataframe = self._finalize_records(plan, [])
            stats.rows = int(dataframe.shape[0])
            stats.duration_ms = (time.perf_counter() - plan.started_at) * 1000.0
            return dataframe, stats

        fetch_result = plan.fetcher(context.ids, context)
        items_iter: Iterable[Mapping[str, Any]]
        payload: Any | None = None
        if isinstance(fetch_result, tuple) and fetch_result:
            items_iter = fetch_result[0]
            payload = fetch_result[1] if len(fetch_result) > 1 else None
        else:
            items_iter = fetch_result

        records: list[Mapping[str, Any]] = []
        for item in items_iter:
            record = self._apply_transform(plan, item)
            records.append(record)
            if plan.limit is not None and len(records) >= int(plan.limit):
                break

        if plan.limit is not None and len(records) > int(plan.limit):
            records = records[: int(plan.limit)]

        dataframe = self._finalize_records(plan, records)
        duration_ms = (time.perf_counter() - plan.started_at) * 1000.0
        stats.rows = int(dataframe.shape[0])
        stats.duration_ms = duration_ms

        if isinstance(payload, BatchExtractionStats):
            stats.batches = payload.batches
            stats.api_calls = payload.api_calls
            stats.cache_hits = payload.cache_hits
            stats.set_extra(**payload.extra)
        elif isinstance(payload, Mapping):
            self._apply_summary(plan, payload)

        return dataframe, stats


@dataclass
class BatchExtractionDirector:
    """Assemble a :class:`BatchExtractionPlan` using the configured strategies."""

    normalization_strategy: IdNormalizationStrategy
    batch_sizing_strategy: BatchSizingStrategy
    fetch_strategies: Mapping[str, FetchStrategy]

    def build_plan(
        self,
        *,
        pipeline: PipelineBase,
        ids: Sequence[Any],
        id_column: str,
        select_fields: tuple[str, ...],
        fetcher: FetcherCallable,
        fetch_mode: str,
        limit: int | None,
        batch_size: int | None,
        chunk_size: int | None,
        max_batch_size: int | None,
        id_normalizer: NormalizerCallable | None,
        sort_key: SortKeyCallable | None,
        transform_item: TransformCallable | None,
        finalize: FinalizeCallable | None,
        finalize_context: FinalizeContextCallable | None,
        empty_frame_factory: Callable[[], pd.DataFrame] | None,
        stats_attribute: str | None,
        log: BoundLogger,
        started_at: float,
    ) -> BatchExtractionPlan:
        normalization = self.normalization_strategy.normalize(
            ids,
            limit=limit,
            id_normalizer=id_normalizer,
            sort_key=sort_key,
        )

        sizing = self.batch_sizing_strategy.calculate(
            pipeline=pipeline,
            requested_batch_size=batch_size,
            requested_chunk_size=chunk_size,
            max_batch_size=max_batch_size,
        )

        limit_value = None
        if limit is not None:
            limit_value = max(int(limit), 0)

        context = BatchExtractionContext(
            ids=normalization.unique_ids,
            id_column=id_column,
            select_fields=select_fields,
            limit=limit_value,
            batch_size=sizing.batch_size,
            chunk_size=sizing.chunk_size,
            stats=normalization.stats,
            log=log,
            metadata=dict(normalization.metadata),
        )
        context.extra.setdefault("id_metadata", context.metadata)

        fetch_strategy = self.fetch_strategies.get(fetch_mode)
        if fetch_strategy is None:
            msg = f"Unsupported fetch mode: {fetch_mode}"
            raise ValueError(msg)

        return BatchExtractionPlan(
            pipeline=pipeline,
            fetcher=fetcher,
            transform_item=transform_item,
            empty_frame_factory=empty_frame_factory,
            finalize=finalize,
            finalize_context=finalize_context,
            stats_attribute=stats_attribute,
            id_column=id_column,
            limit=limit_value,
            started_at=started_at,
            fetch_strategy=fetch_strategy,
            context=context,
        )


@dataclass(slots=True)
class DescriptorStrategyFactory:
    """Create directors with the default strategy implementations."""

    normalization_strategy: IdNormalizationStrategy = field(
        default_factory=SimpleNormalizationStrategy
    )
    batch_sizing_strategy: BatchSizingStrategy = field(
        default_factory=SimpleBatchSizingStrategy
    )
    fetch_strategies: Mapping[str, FetchStrategy] = field(
        default_factory=lambda: {
            "default": DefaultFetchStrategy(),
            "delegated": DelegatedFetchStrategy(),
        }
    )

    def build_plan(
        self,
        *,
        pipeline: PipelineBase,
        ids: Sequence[Any],
        id_column: str,
        select_fields: tuple[str, ...],
        fetcher: FetcherCallable,
        fetch_mode: str,
        limit: int | None,
        batch_size: int | None,
        chunk_size: int | None,
        max_batch_size: int | None,
        id_normalizer: NormalizerCallable | None,
        sort_key: SortKeyCallable | None,
        transform_item: TransformCallable | None,
        finalize: FinalizeCallable | None,
        finalize_context: FinalizeContextCallable | None,
        empty_frame_factory: Callable[[], pd.DataFrame] | None,
        stats_attribute: str | None,
        log: BoundLogger,
        started_at: float,
    ) -> BatchExtractionPlan:
        director = BatchExtractionDirector(
            normalization_strategy=self.normalization_strategy,
            batch_sizing_strategy=self.batch_sizing_strategy,
            fetch_strategies=self.fetch_strategies,
        )
        return director.build_plan(
            pipeline=pipeline,
            ids=ids,
            id_column=id_column,
            select_fields=select_fields,
            fetcher=fetcher,
            fetch_mode=fetch_mode,
            limit=limit,
            batch_size=batch_size,
            chunk_size=chunk_size,
            max_batch_size=max_batch_size,
            id_normalizer=id_normalizer,
            sort_key=sort_key,
            transform_item=transform_item,
            finalize=finalize,
            finalize_context=finalize_context,
            empty_frame_factory=empty_frame_factory,
            stats_attribute=stats_attribute,
            log=log,
            started_at=started_at,
        )
