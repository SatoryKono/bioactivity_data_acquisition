"""Reusable mixins shared by the unified pipeline base.

Each mixin documents the expectations it has from the concrete pipeline
class.  This keeps responsibilities discoverable and prevents each pipeline
from re‑implementing boilerplate lifecycle hooks.
"""

from __future__ import annotations

import json
import time
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager, suppress
from dataclasses import dataclass, field
from typing import Any, Literal

import pandas as pd
from structlog.stdlib import BoundLogger

from bioetl.chembl.common.descriptor import (
    BatchExtractionContext,
    ChemblExtractionDescriptor,
    DryRunHandler,
    FetcherFactory,
    FinalizeCallable,
    FinalizeContextCallable,
    FinalizeContextFactory,
    FinalizeFactory,
)
from bioetl.chembl.common.normalize import normalize_identifiers
from bioetl.core.http import UnifiedAPIClient
from bioetl.core.logging import LogEvents
from bioetl.core.pipeline import RunResult
from bioetl.core.schema import (
    IdentifierRule,
    StringRule,
    normalize_string_columns,
)
from bioetl.core.schema.normalizers import IdentifierStats, StringStats
from bioetl.pipelines.mixins.enrichment_engine import EnrichmentScenarioEngine
from bioetl.pipelines.mixins.flatten_nested import FlattenNestedMixin, FlattenSpec


class LoggingMixin:
    """Provide structured stage logging helpers.

    Contract
    --------
    Concrete pipelines are expected to expose ``_make_pipeline_logger`` and
    ``_stage_durations_ms`` attributes, both of which are initialised by
    :class:`bioetl.pipelines.base.PipelineBase`.  The mixin exposes
    :meth:`stage_logger` and :meth:`logger_for` so that downstream mixins can
    rely on a consistent logging surface.
    """

    @contextmanager
    def stage_logger(
        self,
        stage: str,
        *,
        component: str | None = None,
        rows: int | None = None,
        **extra: Any,
    ) -> Iterator[BoundLogger]:
        """Yield a logger bound to the given stage and capture timings."""
        log = self.logger_for(stage=stage, component=component, **extra)
        start = time.perf_counter()
        log.info("stage_started", rows=rows)
        try:
            yield log
        finally:
            duration_ms = (time.perf_counter() - start) * 1000.0
            if getattr(self, "_stage_durations_ms", None) is not None:
                self._stage_durations_ms[stage] = duration_ms  # type: ignore[attr-defined]
            log.info("stage_completed", duration_ms=duration_ms, rows=rows)

    def logger_for(
        self,
        *,
        stage: str | None = None,
        component: str | None = None,
        **extra: Any,
    ) -> BoundLogger:
        """Return a logger bound to the pipeline and stage context."""
        return self._make_pipeline_logger(stage=stage, component=component, **extra)


class ReleaseHandshakeMixin:
    """Provide handshake helpers against the ChEMBL status endpoint.

    Contract
    --------
    ``logger_for`` and ``record_extract_metadata`` are required and are
    implemented by :class:`bioetl.pipelines.base.PipelineBase`.  Pipelines may
    call :meth:`perform_handshake` with the desired endpoint; caching and
    telemetry are handled by the mixin so the pipelines no longer need to wire
    logging or metadata updates manually.
    """

    _handshake_cache: dict[str, tuple[float, Mapping[str, Any]]]

    def perform_handshake(
        self,
        chembl_client: Any,
        endpoint: str,
        *,
        enabled: bool = True,
        ttl_seconds: int = 3600,
    ) -> Mapping[str, Any]:
        """Fetch and cache the ChEMBL status payload for the endpoint."""
        if not enabled:
            self.logger_for(stage="handshake").info("handshake_skipped", endpoint=endpoint)
            return {}

        cache: dict[str, tuple[float, Mapping[str, Any]]] = getattr(self, "_handshake_cache", {})
        now = time.monotonic()
        cached = cache.get(endpoint)
        if cached and cached[0] > now:
            return cached[1]

        log = self.logger_for(stage="handshake")
        started = time.perf_counter()
        log.info("handshake_started", endpoint=endpoint)

        status_payload: Mapping[str, Any]
        handshake_method = getattr(chembl_client, "handshake", None)
        if callable(handshake_method):
            status_payload = handshake_method(endpoint=endpoint)
        else:
            response = chembl_client.get(endpoint)
            status_payload = response.json()

        duration_ms = (time.perf_counter() - started) * 1000.0
        log.info("handshake_completed", duration_ms=duration_ms)

        self.record_extract_metadata(
            chembl_release=status_payload.get("chembl_release"),
            api_version=status_payload.get("api_version"),
        )

        cache[endpoint] = (now + float(ttl_seconds), status_payload)
        self._handshake_cache = cache
        return status_payload


class PaginatedExtractorMixin:
    """Utility helpers for paginated extractions.

    Contract
    --------
    The mixin relies on :meth:`stage_logger` for telemetry and therefore should
    be composed with :class:`LoggingMixin`.  Pipelines may optionally implement
    ``on_page`` to observe pagination metadata (for example, to capture rate
    limiting hints or diagnostics per page).
    """

    def iterate_pages(
        self,
        client: UnifiedAPIClient,
        endpoint: str,
        params: Mapping[str, Any],
        page_size: int,
        *,
        items_key: str = "results",
    ) -> Iterator[tuple[int, list[Mapping[str, Any]]]]:
        """Yield result batches produced by the paginated endpoint."""
        next_endpoint = endpoint
        page_index = 0
        while next_endpoint:
            page_params = dict(params)
            page_params.setdefault("limit", page_size)
            with self.stage_logger("extract", component="pagination", page=page_index) as log:
                log.info("page_started", page=page_index)
                response = client.get(next_endpoint, params=page_params)
                payload = response.json()
                items = list(payload.get(items_key, []))
                log.info(
                    "page_completed",
                    page=page_index,
                    rows=len(items),
                    endpoint=next_endpoint,
                )
                yield page_index, items
                if hasattr(self, "on_page"):
                    with suppress(AttributeError):
                        self.on_page(page_index, payload.get("page_meta", {}))
            next_endpoint = payload.get("page_meta", {}).get("next")
            page_index += 1
            if not items:
                break

    def run_batched_extraction(self, *args: Any, **kwargs: Any) -> Any:
        """Delegate to :class:`ChemblPipelineBase` batching helpers."""
        return super().run_batched_extraction(*args, **kwargs)


class SchemaValidationMixin:
    """Wrapper around the base validation routine with logging hooks.

    Contract
    --------
    ``config.validation`` and ``_resolve_schema_entry`` must be present on the
    pipeline (both are provided by :class:`bioetl.pipelines.base.PipelineBase`).
    The mixin defers the heavy lifting to :meth:`PipelineBase.validate` but adds
    logging via :meth:`stage_logger`.
    """

    def load_validation_schema(self) -> Any:
        """Return the configured output schema entry when available."""
        schema_identifier = self.config.validation.schema_out
        if not schema_identifier:
            return None
        expected_version = getattr(self.config.validation, "schema_out_version", None)
        allow_migration = bool(getattr(self.config.validation, "allow_schema_migration", False))
        schema_entry, _, _ = self._resolve_schema_entry(
            schema_identifier,
            expected_version=expected_version,
            dataset_role="primary",
            allow_migration=allow_migration,
        )
        return schema_entry

    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        """Wrap ``PipelineBase.validate`` with stage logging."""
        with self.stage_logger("validate", rows=len(df)):
            return super().validate(df)


class RecordNormalizationMixin:
    """Provide reusable identifier and string normalization helpers."""

    identifier_log_event: LogEvents | None = LogEvents.IDENTIFIERS_NORMALIZED
    string_log_event: LogEvents | None = LogEvents.STRING_FIELDS_NORMALIZED

    def identifier_rules(self) -> Sequence[IdentifierRule]:  # pragma: no cover - hook
        return ()

    def string_rules(self) -> Mapping[str, StringRule]:  # pragma: no cover - hook
        return {}

    def preprocess_identifier_columns(
        self, df: pd.DataFrame, log: BoundLogger
    ) -> pd.DataFrame:  # pragma: no cover - optional hook
        return df

    def preprocess_string_columns(
        self, df: pd.DataFrame, log: BoundLogger
    ) -> pd.DataFrame:  # pragma: no cover - optional hook
        return df

    def postprocess_identifier_columns(
        self,
        df: pd.DataFrame,
        stats: IdentifierStats,
        log: BoundLogger,
    ) -> pd.DataFrame:
        if stats.has_changes and self.identifier_log_event is not None:
            log.debug(
                self.identifier_log_event,
                normalized_count=stats.normalized,
                invalid_count=stats.invalid,
                columns=list(stats.per_column.keys()),
            )
        return df

    def postprocess_string_columns(
        self,
        df: pd.DataFrame,
        stats: StringStats,
        log: BoundLogger,
    ) -> pd.DataFrame:
        if stats.has_changes and self.string_log_event is not None:
            log.debug(
                self.string_log_event,
                columns=list(stats.per_column.keys()),
                rows_processed=stats.processed,
            )
        return df

    def _normalize_identifiers(self, df: pd.DataFrame, log: BoundLogger) -> pd.DataFrame:
        working_df = df.copy()
        working_df = self.preprocess_identifier_columns(working_df, log)
        rules = tuple(self.identifier_rules())
        normalized_df, stats = normalize_identifiers(working_df, rules, copy=False)
        return self.postprocess_identifier_columns(normalized_df, stats, log)

    def _normalize_string_fields(self, df: pd.DataFrame, log: BoundLogger) -> pd.DataFrame:
        working_df = df.copy()
        working_df = self.preprocess_string_columns(working_df, log)
        rules = dict(self.string_rules())
        normalized_df, stats = normalize_string_columns(working_df, rules, copy=False)
        return self.postprocess_string_columns(normalized_df, stats, log)


@dataclass(frozen=True)
class NestedColumnSpec:
    column: str
    target_column: str | None = None
    serializer: Callable[..., str | None] | None = None
    pass_logger: bool = False
    preserve_null_like: bool = True
    empty_string_to_null: bool = False
    drop_source_column: bool = False
    failure_event: LogEvents = LogEvents.NESTED_SERIALIZATION_FAILED


class NestedSerializerMixin:
    """Serialize nested columns described by :class:`NestedColumnSpec`."""

    nested_log_event: LogEvents | None = LogEvents.ARRAY_FIELDS_SERIALIZED

    def nested_column_specs(self) -> Sequence[NestedColumnSpec]:  # pragma: no cover - hook
        return ()

    def preprocess_nested_columns(
        self, df: pd.DataFrame, log: BoundLogger
    ) -> pd.DataFrame:  # pragma: no cover - optional hook
        return df

    def _serialize_nested_columns(self, df: pd.DataFrame, log: BoundLogger) -> pd.DataFrame:
        specs = tuple(self.nested_column_specs())
        if not specs:
            return df

        working_df = self.preprocess_nested_columns(df.copy(), log)
        serialized_columns: list[str] = []

        for spec in specs:
            source_column = spec.column
            if source_column not in working_df.columns:
                continue

            target_column = spec.target_column or source_column
            original_series = working_df[source_column]

            try:
                na_mask = original_series.isna() if spec.preserve_null_like else None
            except Exception:
                na_mask = None

            serialized_values: list[Any] = []
            for index, value in original_series.items():
                if na_mask is not None and bool(na_mask.loc[index]):
                    serialized_values.append(pd.NA)
                    continue
                serialized_values.append(
                    self._serialize_nested_value(spec, value, index, log)
                )

            working_df[target_column] = pd.Series(
                serialized_values,
                index=original_series.index,
                dtype="object",
            )

            if spec.empty_string_to_null:
                column_as_string = working_df[target_column].astype("string")
                empty_mask = column_as_string.eq("")
                if bool(empty_mask.any()):
                    working_df.loc[empty_mask, target_column] = pd.NA

            if spec.drop_source_column and target_column != source_column:
                working_df = working_df.drop(columns=[source_column])

            serialized_columns.append(target_column)

        if serialized_columns and self.nested_log_event is not None:
            log.debug(self.nested_log_event, columns=serialized_columns)

        return working_df

    def _serialize_nested_value(
        self,
        spec: NestedColumnSpec,
        value: Any,
        index: Any,
        log: BoundLogger,
    ) -> str | None:
        if value is None:
            return None

        serializer = spec.serializer or self._default_nested_serializer

        try:
            if spec.pass_logger:
                return serializer(value, log)
            return serializer(value)
        except Exception as exc:  # pragma: no cover
            log.warning(
                spec.failure_event,
                column=spec.column,
                index=index,
                error=str(exc),
            )
            return None

    @staticmethod
    def _default_nested_serializer(value: Any) -> str | None:
        if isinstance(value, str):
            return value
        if isinstance(value, Mapping):
            return json.dumps(value, ensure_ascii=False, sort_keys=True)
        if isinstance(value, Sequence) and not isinstance(value, (bytes, str)):
            return json.dumps(list(value), ensure_ascii=False, sort_keys=True)
        if isinstance(value, (int, float, bool)):
            return json.dumps(value, ensure_ascii=False, sort_keys=True)
        return None


@dataclass(slots=True)
class BatchIdExtractionPlan:
    """Describe how :meth:`BatchIdExtractionMixin.extract_by_ids` should run."""

    source_config: Any
    summary_event: str
    dry_run_event: str | None = None
    metadata_filters: Mapping[str, Any] | None = None
    summary_extra: Mapping[str, Any] | None = None
    dry_run_handler: DryRunHandler | None = None
    fetcher_factory: FetcherFactory | None = None
    finalize_callable: FinalizeCallable | None = None
    finalize_factory: FinalizeFactory | None = None
    finalize_context_callable: FinalizeContextCallable | None = None
    finalize_context_factory: FinalizeContextFactory | None = None
    run_kwargs: dict[str, Any] = field(default_factory=dict)


class BatchIdExtractionMixin:
    """Provide a shared ``extract_by_ids`` implementation for ChEMBL pipelines.

    Миксин инкапсулирует контракт ID-батчей: он строит
    :class:`BatchIdExtractionPlan` на основе дескриптора, конфигурации источника
    и CLI-флагов, после чего вызывает ``run_descriptor_extraction`` из
    :class:`ChemblPipelineBase`. Конкретные пайплайны переопределяют только
    `id_extraction_*`-хуки (batch size, лимиты, выбор полей, finalize-фабрики),
    сохраняя единое логирование и Dry-run режим. Результаты автоматически
    публикуются через ``record_extract_metadata`` и доступны в `meta.yaml`.
    """
    """Provide a shared ``extract_by_ids`` implementation for ChEMBL pipelines."""

    id_extraction_summary_event: str | None = None
    id_extraction_dry_run_event: str | None = None
    id_chunk_size_cap: int | None = None
    id_max_batch_size: int | None = None

    def extract_by_ids(self, ids: Sequence[str]) -> pd.DataFrame:
        """Execute descriptor-driven ID extraction with configurable hooks."""

        descriptor = self.build_descriptor()
        plan = self.id_extraction_defaults(descriptor)
        fetcher_factory = plan.fetcher_factory or self.id_extraction_fetcher_factory(
            descriptor,
            plan.source_config,
        )
        finalize_callable = plan.finalize_callable
        if finalize_callable is None:
            finalize_callable = self.id_extraction_finalize_callable()
        finalize_factory = plan.finalize_factory or self.id_extraction_finalize_factory(
            descriptor,
            plan.source_config,
        )
        finalize_context_callable = plan.finalize_context_callable
        if finalize_context_callable is None:
            finalize_context_callable = self.id_extraction_finalize_context_callable()
        finalize_context_factory = plan.finalize_context_factory or self.id_extraction_finalize_context_factory(
            descriptor,
            plan.source_config,
        )

        dataframe, _ = self.run_descriptor_extraction(
            descriptor,
            ids,
            source_config=plan.source_config,
            summary_event=plan.summary_event,
            dry_run_event=plan.dry_run_event,
            dry_run_handler=plan.dry_run_handler,
            fetcher_factory=fetcher_factory,
            finalize=finalize_callable,
            finalize_factory=finalize_factory,
            finalize_context=finalize_context_callable,
            finalize_context_factory=finalize_context_factory,
            metadata_filters=plan.metadata_filters,
            summary_extra=plan.summary_extra,
            **plan.run_kwargs,
        )

        self.post_id_extraction(ids, dataframe, plan)
        return dataframe

    def id_extraction_defaults(
        self,
        descriptor: ChemblExtractionDescriptor[Any],
    ) -> BatchIdExtractionPlan:
        """Return the default execution plan for ID-based extractions."""

        source_raw = self._resolve_source_config(descriptor.source_name)
        typed_source_config = descriptor.source_config_factory(source_raw)
        select_fields = self.id_extraction_select_fields(
            source_raw,
            typed_source_config,
            descriptor,
        )
        limit = self.id_extraction_limit()
        metadata_filters = self.id_extraction_metadata_filters(
            select_fields,
            typed_source_config,
            descriptor,
        )
        summary_extra = self.id_extraction_summary_extra(
            limit,
            typed_source_config,
            descriptor,
        )
        batch_size = self.id_extraction_batch_size(typed_source_config, descriptor)
        chunk_size = self.id_extraction_chunk_size(batch_size, descriptor)
        max_batch_size = self.id_extraction_max_batch_size_value(batch_size, descriptor)
        run_kwargs: dict[str, Any] = {
            "batch_size": batch_size,
            "chunk_size": chunk_size,
            "max_batch_size": max_batch_size,
            "limit": limit,
            "select_fields": select_fields or None,
            "fetch_mode": self.id_extraction_fetch_mode(),
        }
        stats_attribute = self.id_extraction_stats_attribute()
        if stats_attribute:
            run_kwargs["stats_attribute"] = stats_attribute
        id_normalizer = self.id_extraction_id_normalizer()
        if id_normalizer:
            run_kwargs["id_normalizer"] = id_normalizer
        sort_key = self.id_extraction_sort_key()
        if sort_key:
            run_kwargs["sort_key"] = sort_key
        empty_frame_factory = self.id_extraction_empty_frame_factory(
            descriptor,
            typed_source_config,
        )
        if empty_frame_factory is not None:
            run_kwargs["empty_frame_factory"] = empty_frame_factory
        extra_kwargs = self.id_extraction_run_kwargs(descriptor, typed_source_config)
        if extra_kwargs:
            run_kwargs.update(extra_kwargs)

        summary_event = self.id_extraction_summary_event
        if not summary_event:
            msg = "id_extraction_summary_event must be defined"
            raise RuntimeError(msg)

        return BatchIdExtractionPlan(
            source_config=typed_source_config,
            summary_event=summary_event,
            dry_run_event=self.id_extraction_dry_run_event,
            metadata_filters=metadata_filters,
            summary_extra=summary_extra,
            dry_run_handler=self.id_extraction_dry_run_handler(
                descriptor,
                typed_source_config,
            ),
            fetcher_factory=self.id_extraction_fetcher_factory(
                descriptor,
                typed_source_config,
            ),
            finalize_callable=self.id_extraction_finalize_callable(),
            finalize_factory=self.id_extraction_finalize_factory(
                descriptor,
                typed_source_config,
            ),
            finalize_context_callable=self.id_extraction_finalize_context_callable(),
            finalize_context_factory=self.id_extraction_finalize_context_factory(
                descriptor,
                typed_source_config,
            ),
            run_kwargs=run_kwargs,
        )

    def id_extraction_select_fields(
        self,
        source_config: Any,
        typed_source_config: Any,
        descriptor: ChemblExtractionDescriptor[Any],
    ) -> list[str] | None:
        """Return select_fields merged with descriptor-required columns."""

        resolved = self._resolve_select_fields(
            source_config,
            default_fields=descriptor.default_select_fields,
        )
        return self._merge_select_fields(resolved, descriptor.must_have_fields)

    def id_extraction_metadata_filters(
        self,
        select_fields: Sequence[str] | None,
        typed_source_config: Any,
        descriptor: ChemblExtractionDescriptor[Any],
    ) -> Mapping[str, Any] | None:
        """Return metadata filters passed to the descriptor context."""

        if select_fields:
            return {"select_fields": list(select_fields)}
        return None

    def id_extraction_summary_extra(
        self,
        limit: int | None,
        typed_source_config: Any,
        descriptor: ChemblExtractionDescriptor[Any],
    ) -> Mapping[str, Any] | None:
        """Return additional summary payload for extract-by-ids."""

        if limit is None:
            return None
        return {"limit": limit}

    def id_extraction_limit(self) -> int | None:
        """Return CLI-provided limit for ID extraction."""

        return self.config.cli.limit

    def id_extraction_batch_size(
        self,
        typed_source_config: Any,
        descriptor: ChemblExtractionDescriptor[Any],
    ) -> int:
        """Resolve batch size from the source configuration."""

        return self._resolve_batch_size(typed_source_config)

    def id_extraction_chunk_size(
        self,
        batch_size: int,
        descriptor: ChemblExtractionDescriptor[Any],
    ) -> int:
        """Return chunk size used when splitting ID batches."""

        cap_candidates = [
            value
            for value in (
                self.id_chunk_size_cap,
                descriptor.hard_page_size_cap,
                batch_size,
            )
            if isinstance(value, int) and value > 0
        ]
        cap = min(cap_candidates) if cap_candidates else batch_size
        return max(1, min(batch_size, cap))

    def id_extraction_max_batch_size_value(
        self,
        batch_size: int,
        descriptor: ChemblExtractionDescriptor[Any],
    ) -> int:
        """Return the upper bound enforced for batch sizes."""

        candidates = [
            value
            for value in (
                self.id_max_batch_size,
                descriptor.hard_page_size_cap,
                batch_size,
            )
            if isinstance(value, int) and value > 0
        ]
        return min(candidates) if candidates else batch_size

    def id_extraction_stats_attribute(self) -> str | None:
        """Return attribute name that receives :class:`BatchExtractionStats`."""

        return None

    def id_extraction_id_normalizer(
        self,
    ) -> Callable[[Any], tuple[str | None, Any]] | None:
        """Return optional ID normalizer passed to batch extraction."""

        return None

    def id_extraction_sort_key(
        self,
    ) -> Callable[[tuple[str, Any]], Any] | None:
        """Return an optional sort key for canonical ID pairs."""

        return None

    def id_extraction_empty_frame_factory(
        self,
        descriptor: ChemblExtractionDescriptor[Any],
        typed_source_config: Any,
    ) -> Callable[[], pd.DataFrame] | None:
        """Return a factory producing empty frames for dry-runs and cache hits."""

        return None

    def id_extraction_run_kwargs(
        self,
        descriptor: ChemblExtractionDescriptor[Any],
        typed_source_config: Any,
    ) -> Mapping[str, Any]:
        """Allow subclasses to inject additional keyword arguments."""

        return {}

    def id_extraction_fetch_mode(self) -> Literal["default", "delegated"]:
        """Return the fetch mode used by :meth:`run_batched_extraction`."""

        return "default"

    def id_extraction_dry_run_handler(
        self,
        descriptor: ChemblExtractionDescriptor[Any],
        typed_source_config: Any,
    ) -> DryRunHandler | None:
        """Return optional dry-run handler overriding descriptor defaults."""

        return None

    def id_extraction_fetcher_factory(
        self,
        descriptor: ChemblExtractionDescriptor[Any],
        typed_source_config: Any,
    ) -> FetcherFactory | None:
        """Return custom fetcher factory when default iterate_by_ids is insufficient."""

        return None

    def id_extraction_finalize_callable(self) -> FinalizeCallable | None:
        """Return callable invoked once all batches are materialised."""

        def finalize(dataframe: pd.DataFrame, context: BatchExtractionContext) -> pd.DataFrame:
            return self.finalize_batch(dataframe, context)

        return finalize

    def id_extraction_finalize_factory(
        self,
        descriptor: ChemblExtractionDescriptor[Any],
        typed_source_config: Any,
    ) -> FinalizeFactory | None:
        """Allow subclasses to create finalize factories that access the context."""

        return None

    def id_extraction_finalize_context_callable(self) -> FinalizeContextCallable | None:
        """Return callable invoked after batch extraction completes."""

        def finalize_context(context: BatchExtractionContext) -> None:
            self.finalize_batch_context(context)

        return finalize_context

    def id_extraction_finalize_context_factory(
        self,
        descriptor: ChemblExtractionDescriptor[Any],
        typed_source_config: Any,
    ) -> FinalizeContextFactory | None:
        """Allow subclasses to access the descriptor context during teardown."""

        return None

    def finalize_batch(
        self,
        dataframe: pd.DataFrame,
        context: BatchExtractionContext,
    ) -> pd.DataFrame:  # pragma: no cover - overridable hook
        """Hook executed after batch extraction before returning the dataframe."""

        _ = context
        return dataframe

    def finalize_batch_context(
        self,
        context: BatchExtractionContext,
    ) -> None:  # pragma: no cover - overridable hook
        """Hook executed when :class:`BatchExtractionContext` is disposed."""

        _ = context

    def post_id_extraction(
        self,
        ids: Sequence[str],
        dataframe: pd.DataFrame,
        plan: BatchIdExtractionPlan,
    ) -> None:  # pragma: no cover - overridable hook
        """Observe extraction results before returning control to the caller."""

        _ = (ids, dataframe, plan)


class TransformMixin:
    """Provide a default transform lifecycle that normalises payloads.

    Contract
    --------
    Pipelines are expected to expose ``_output_column_order`` and
    ``_normalize_and_enforce_schema`` (supplied by
    :class:`bioetl.chembl.common.descriptor.ChemblPipelineBase`) together with
    ``stage_logger`` from :class:`LoggingMixin`. Hooks such as ``pre_transform``,
    ``domain_enrich`` and ``post_transform`` can be overridden to customise the
    lifecycle, while the mixin guarantees that schema enforcement, identifier
    normalization and logging are executed consistently.
    """

    def pre_transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Prepare a working copy before schema normalisation."""
        return df

    def post_transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Finalise the dataset after enrichment."""
        return df

    def domain_enrich(self, df: pd.DataFrame) -> pd.DataFrame:
        """Enrich the dataset with domain-specific metadata."""
        return df

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Execute the default transform lifecycle with logging."""
        with self.stage_logger("transform", rows=len(df)) as log:
            working_df = self.pre_transform(df.copy())
            column_order = getattr(self, "_output_column_order", tuple(working_df.columns))
            working_df = self._normalize_and_enforce_schema(  # type: ignore[attr-defined]
                working_df,
                column_order,
                log,
                order_columns=True,
            )
            enriched = self.domain_enrich(working_df)
            return self.post_transform(enriched)


class IOArtifactsMixin:
    """Bridge deterministic IO helpers with the pipeline run template.

    Contract
    --------
    Requires ``save_results`` from :class:`bioetl.pipelines.base.PipelineBase`.
    The mixin exposes ``save_results`` для всех ChEMBL-пайплайнов, чтобы они могли
    сохранять артефакты (основной CSV, correlation report, QC метрики)
    детерминированно и в едином формате `RunResult`. Любые дополнительные опции
    (``extended``, ``include_correlation``, ``include_qc_metrics``) передаются
    напрямую в базовый класс, что делает CLI-флаги и unit-тесты переиспользуемыми.
    """

    def save_results(
        self,
        df: pd.DataFrame,
        output_dir: Any,
        *,
        extended: bool = False,
        include_correlation: bool = False,
        include_qc_metrics: bool = False,
    ) -> RunResult:
        """Persist the dataframe using :meth:`PipelineBase.save_results`."""
        run_result: RunResult = super().save_results(
            df,
            output_dir,
            extended=extended,
            include_correlation=include_correlation,
            include_qc_metrics=include_qc_metrics,
        )
        return run_result
