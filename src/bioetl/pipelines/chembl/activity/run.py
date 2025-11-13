"""Activity pipeline implementation for ChEMBL."""

from __future__ import annotations

import hashlib
import json
import re
import time
from collections.abc import Callable, Iterable, Mapping, Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, cast
from urllib.parse import urlparse

import pandas as pd
import pandera.errors
from requests.exceptions import RequestException
from structlog.stdlib import BoundLogger

from bioetl.clients.client_chembl_common import ChemblClient
from bioetl.clients.entities.client_activity import ChemblActivityClient
from bioetl.config import ActivitySourceConfig, PipelineConfig
from bioetl.core import UnifiedLogger
from bioetl.core.http.api_client import CircuitBreakerOpenError
from bioetl.core.logging import LogEvents
from bioetl.core.schema import (
    IdentifierRule,
    StringRule,
    format_failure_cases,
    normalize_identifier_columns,
    normalize_string_columns,
    summarize_schema_errors,
)
from bioetl.core.utils import join_activity_with_molecule
from bioetl.qc.report import build_quality_report as build_default_quality_report
from bioetl.schemas.chembl_activity_schema import (
    ACTIVITY_PROPERTY_KEYS,
    COLUMN_ORDER,
    RELATIONS,
    ActivitySchema,
)

from ...base import RunResult
from ..common.descriptor import (
    BatchExtractionContext,
    ChemblExtractionContext,
    ChemblExtractionDescriptor,
    ChemblPipelineBase,
)
from .normalize import (
    enrich_with_assay,
    enrich_with_compound_record,
    enrich_with_data_validity,
)

API_ACTIVITY_FIELDS: tuple[str, ...] = (
    "activity_id",
    "assay_chembl_id",
    "testitem_chembl_id",
    "molecule_chembl_id",
    "target_chembl_id",
    "document_chembl_id",
    "type",
    "relation",
    "value",
    "units",
    "standard_type",
    "standard_relation",
    "standard_value",
    "standard_units",
    "standard_text_value",
    "standard_flag",
    "upper_value",
    "lower_value",
    "pchembl_value",
    "activity_comment",
    "bao_endpoint",
    "bao_format",
    "bao_label",
    "canonical_smiles",
    "ligand_efficiency",
    "target_organism",
    "target_tax_id",
    "data_validity_comment",
    "potential_duplicate",
    "activity_properties",
    # New fields
    "assay_type",
    "assay_description",
    "assay_organism",
    "assay_tax_id",
    "parent_molecule_chembl_id",
    "molecule_pref_name",
    "record_id",
    "src_id",
    "target_pref_name",
    "text_value",
    "standard_upper_value",
    "uo_units",
    "qudt_units",
    # data_validity_description is retrieved separately via fetch_data_validity_lookup().
    "curated_by",  # Used to compute curated: (curated_by IS NOT NULL) -> bool.
)


class ChemblActivityPipeline(ChemblPipelineBase):
    """ETL pipeline extracting activity records from the ChEMBL API."""

    actor = "activity_chembl"

    def __init__(self, config: PipelineConfig, run_id: str) -> None:
        super().__init__(config, run_id)
        self._chembl_release: str | None = None
        self._last_batch_extract_stats: dict[str, Any] | None = None
        self._required_vocab_ids: Callable[[str], Iterable[str]] = (
            self._vocabulary_service.required_ids
        )
        self._standard_types_cache: frozenset[str] | None = None

    @property
    def chembl_release(self) -> str | None:
        """Return the cached ChEMBL release captured during extraction."""

        return self._chembl_release

    def extract(self, *args: object, **kwargs: object) -> pd.DataFrame:
        """Fetch activity payloads from ChEMBL using the unified HTTP client.

        Checks for input_file in config.cli.input_file and calls extract_by_ids()
        if present, otherwise calls extract_all().
        """
        log = UnifiedLogger.get(__name__).bind(component=f"{self.pipeline_code}.extract")

        def legacy_activity_ids(bound_log: BoundLogger) -> Sequence[str] | None:
            payload_activity_ids = kwargs.get("activity_ids")
            if payload_activity_ids is None:
                return None

            bound_log.warning(LogEvents.CHEMBL_ACTIVITY_DEPRECATED_KWARGS,
                message="Using activity_ids in kwargs is deprecated. Use --input-file instead.",
            )
            if isinstance(payload_activity_ids, Sequence) and not isinstance(
                payload_activity_ids, (str, bytes)
            ):
                sequence_ids: Sequence[str | int] = cast(
                    Sequence[str | int], payload_activity_ids
                )
                return [str(id_val) for id_val in sequence_ids]

            return [str(payload_activity_ids)]

        return self._dispatch_extract_mode(
            log,
            event_name="chembl_activity.extract_mode",
            batch_callback=self.extract_by_ids,
            full_callback=self.extract_all,
            id_column_name="activity_id",
            legacy_id_resolver=legacy_activity_ids,
            legacy_source="deprecated_kwargs",
        )

    def extract_all(self) -> pd.DataFrame:
        """Extract all activity records from ChEMBL using the shared iterator."""

        return self.run_extract_all(self._build_activity_descriptor())

    def extract_by_ids(self, ids: Sequence[str]) -> pd.DataFrame:
        """Extract activity records by a specific list of IDs using batch extraction.

        Parameters
        ----------
        ids:
            Sequence of activity_id values to extract (as strings or integers).

        Returns
        -------
        pd.DataFrame:
            DataFrame containing extracted activity records.
        """
        log = UnifiedLogger.get(__name__).bind(component=f"{self.pipeline_code}.extract")
        stage_start = time.perf_counter()

        (
            source_config,
            chembl_client,
            activity_iterator,
            select_fields,
        ) = self._prepare_activity_iteration()

        limit = self.config.cli.limit
        invalid_ids: list[Any] = []

        def normalize_activity_id(raw: Any) -> tuple[str | None, Any]:
            if pd.isna(raw):
                return None, None
            try:
                if isinstance(raw, str):
                    candidate = raw.strip()
                    if not candidate:
                        return None, None
                    numeric_id = int(float(candidate)) if "." in candidate else int(candidate)
                elif isinstance(raw, (int, float)):
                    numeric_id = int(raw)
                else:
                    numeric_id = int(raw)
            except (TypeError, ValueError):
                invalid_ids.append(raw)
                return None, None
            return str(numeric_id), int(numeric_id)

        def delegated_fetch(
            canonical_ids: Sequence[str],
            context: BatchExtractionContext,
        ) -> tuple[Sequence[Mapping[str, Any]], Mapping[str, Any]]:
            numeric_map = context.metadata
            normalized_ids: list[tuple[int, str]] = []
            for identifier in canonical_ids:
                numeric_value = numeric_map.get(identifier)
                if numeric_value is None:
                    continue
                normalized_ids.append((int(numeric_value), identifier))
            if not normalized_ids:
                summary = {
                    "total_activities": 0,
                    "success": 0,
                    "fallback": 0,
                    "errors": 0,
                    "api_calls": 0,
                    "cache_hits": 0,
                    "batches": 0,
                    "duration_ms": 0.0,
                    "success_rate": 0.0,
                }
                context.extra["delegated_summary"] = summary
                return [], summary

            records, summary = self._collect_records_by_ids(
                normalized_ids,
                activity_iterator,
                select_fields=context.select_fields or None,
            )
            context.extra["delegated_summary"] = summary
            return records, summary

        def finalize_dataframe(
            dataframe: pd.DataFrame, context: BatchExtractionContext
        ) -> pd.DataFrame:
            dataframe = self._ensure_comment_fields(dataframe, log)
            dataframe = self._extract_data_validity_descriptions(dataframe, chembl_client, log)
            dataframe = self._extract_assay_fields(dataframe, chembl_client, log)
            self._log_validity_comments_metrics(dataframe, log)
            return dataframe

        def empty_activity_frame() -> pd.DataFrame:
            return pd.DataFrame({"activity_id": pd.Series(dtype="Int64")})

        def finalize_context(context: BatchExtractionContext) -> None:
            summary = context.extra.get("delegated_summary")
            if isinstance(summary, Mapping):
                summary_dict = dict(summary)
                context.extra["stats_attribute_override"] = summary_dict
                self._last_batch_extract_stats = summary_dict
            else:
                context.extra["stats_attribute_override"] = context.stats.as_dict()
                self._last_batch_extract_stats = context.stats.as_dict()

        dataframe, stats = self.run_batched_extraction(
            ids,
            id_column="activity_id",
            fetcher=delegated_fetch,
            select_fields=select_fields,
            batch_size=source_config.batch_size,
            max_batch_size=25,
            limit=limit,
            metadata_filters={
                "select_fields": list(select_fields) if select_fields else None,
            },
            chembl_release=self._chembl_release,
            id_normalizer=normalize_activity_id,
            sort_key=lambda pair: int(pair[0]),
            finalize=finalize_dataframe,
            finalize_context=finalize_context,
            stats_attribute="_last_batch_extract_stats",
            fetch_mode="delegated",
            empty_frame_factory=empty_activity_frame,
        )

        if invalid_ids:
            log.warning(LogEvents.CHEMBL_ACTIVITY_INVALID_ACTIVITY_IDS,
                invalid_count=len(invalid_ids),
            )

        duration_ms = (time.perf_counter() - stage_start) * 1000.0
        batch_stats = self._last_batch_extract_stats or {}
        log.info(LogEvents.CHEMBL_ACTIVITY_EXTRACT_BY_IDS_SUMMARY,
            rows=int(dataframe.shape[0]),
            requested=len(ids),
            duration_ms=duration_ms,
            chembl_release=self._chembl_release,
            batches=batch_stats.get("batches"),
            api_calls=batch_stats.get("api_calls"),
            cache_hits=batch_stats.get("cache_hits"),
        )
        return dataframe

    def _prepare_activity_iteration(
        self,
        *,
        client_name: str = "chembl_activity_client",
    ) -> tuple[ActivitySourceConfig, ChemblClient, ChemblActivityClient, list[str]]:
        """Construct reusable ChEMBL clients and iterator for activity extraction."""

        log = UnifiedLogger.get(__name__).bind(component=f"{self.pipeline_code}.extract")

        source_raw = self._resolve_source_config("chembl")
        source_config = ActivitySourceConfig.from_source_config(source_raw)
        bundle = self.build_chembl_entity_bundle(
            "activity",
            source_name="chembl",
            source_config=source_config,
        )
        if client_name:
            self.register_client(client_name, bundle.api_client)
        chembl_client = bundle.chembl_client
        activity_iterator = cast(ChemblActivityClient, bundle.entity_client)
        if activity_iterator is None:
            msg = "Фабрика вернула пустой клиент для сущности 'activity'"
            raise RuntimeError(msg)
        self._chembl_release = self.fetch_chembl_release(chembl_client, log)

        select_fields = self._resolve_select_fields(
            source_raw,
            default_fields=API_ACTIVITY_FIELDS,
        )

        return source_config, chembl_client, activity_iterator, select_fields

    def _build_activity_enrichment_bundle(
        self,
        entity_name: str,
        *,
        client_name: str,
    ) -> "ChemblClientBundle":
        """Создать бандл клиентов для обогащения активностей."""

        source_raw = self._resolve_source_config("chembl")
        source_config = ActivitySourceConfig.from_source_config(source_raw)
        bundle = self.build_chembl_entity_bundle(
            entity_name,
            source_name="chembl",
            source_config=source_config,
        )
        if client_name not in self._registered_clients:
            self.register_client(client_name, bundle.api_client)
        return bundle

    def _build_activity_descriptor(self) -> ChemblExtractionDescriptor[ChemblActivityPipeline]:
        """Construct the descriptor driving the shared extraction template."""

        def build_context(
            pipeline: ChemblPipelineBase,
            source_config: ActivitySourceConfig,
            log: BoundLogger,
        ) -> ChemblExtractionContext:
            typed_pipeline = cast("ChemblActivityPipeline", pipeline)
            bundle = typed_pipeline.build_chembl_entity_bundle(
                "activity",
                source_name="chembl",
                source_config=source_config,
            )
            chembl_client = bundle.chembl_client
            typed_pipeline._chembl_release = typed_pipeline.fetch_chembl_release(chembl_client, log)
            activity_iterator = cast(ChemblActivityClient, bundle.entity_client)
            if activity_iterator is None:
                msg = "Фабрика вернула пустой клиент для 'activity'"
                raise RuntimeError(msg)
            select_fields = source_config.parameters.select_fields
            return ChemblExtractionContext(
                source_config=source_config,
                iterator=activity_iterator,
                chembl_client=chembl_client,
                select_fields=list(select_fields) if select_fields else None,
                chembl_release=typed_pipeline._chembl_release,
            )

        def empty_frame(
            pipeline: ChemblPipelineBase,
            _: ChemblExtractionContext,
        ) -> pd.DataFrame:
            return pd.DataFrame({"activity_id": pd.Series(dtype="Int64")})

        def post_process(
            pipeline: ChemblActivityPipeline,
            df: pd.DataFrame,
            context: ChemblExtractionContext,
            log: BoundLogger,
        ) -> pd.DataFrame:
            df = pipeline._ensure_comment_fields(df, log)
            chembl_client = cast(ChemblClient, context.chembl_client)
            if chembl_client is not None:
                df = pipeline._extract_data_validity_descriptions(df, chembl_client, log)
            pipeline._log_validity_comments_metrics(df, log)
            return df

        def record_transform(
            pipeline: ChemblActivityPipeline,
            payload: Mapping[str, Any],
            context: ChemblExtractionContext,  # noqa: ARG001
        ) -> Mapping[str, Any]:
            return pipeline._materialize_activity_record(payload)

        return ChemblExtractionDescriptor[ChemblActivityPipeline](
            name="chembl_activity",
            source_name="chembl",
            source_config_factory=ActivitySourceConfig.from_source_config,
            build_context=build_context,
            id_column="activity_id",
            summary_event="chembl_activity.extract_summary",
            must_have_fields=("activity_id",),
            default_select_fields=API_ACTIVITY_FIELDS,
            record_transform=record_transform,
            post_processors=(post_process,),
            sort_by=("activity_id",),
            empty_frame_factory=empty_frame,
        )

    def _materialize_activity_record(
        self,
        payload: Mapping[str, Any],
        *,
        activity_id: int | None = None,
    ) -> dict[str, Any]:
        """Normalize nested fields within an activity payload."""

        record = dict(payload)
        record = self._extract_nested_fields(record)
        record = self._extract_activity_properties_fields(record)
        if activity_id is not None:
            record.setdefault("activity_id", activity_id)
        return record

    def _coerce_activity_dataset(self, dataset: object) -> pd.DataFrame:
        """Convert arbitrary dataset inputs to a DataFrame of activity identifiers."""

        if isinstance(dataset, pd.Series):
            return dataset.to_frame(name="activity_id")
        if isinstance(dataset, pd.DataFrame):
            return dataset
        if isinstance(dataset, Mapping):
            mapping = cast(Mapping[str, Any], dataset)
            return pd.DataFrame([dict(mapping)])
        if isinstance(dataset, Sequence) and not isinstance(dataset, (str, bytes)):
            dataset_list: list[Any] = list(dataset)  # pyright: ignore[reportUnknownArgumentType]
            return pd.DataFrame({"activity_id": dataset_list})

        msg = (
            "ChemblActivityPipeline._extract_from_chembl expects a DataFrame, Series, "
            "mapping, or sequence of activity_id values"
        )
        raise TypeError(msg)

    def _normalize_activity_ids(
        self,
        input_frame: pd.DataFrame,
        *,
        limit: int | None,
        log: BoundLogger,
    ) -> list[tuple[int, str]]:
        """Normalize raw identifier values into deduplicated integer/string pairs."""

        normalized_ids: list[tuple[int, str]] = []
        invalid_ids: list[Any] = []
        seen: set[str] = set()

        for raw_id in input_frame["activity_id"].tolist():
            if pd.isna(raw_id):
                continue
            try:
                if isinstance(raw_id, str):
                    candidate = raw_id.strip()
                    if not candidate:
                        continue
                    numeric_id = int(float(candidate)) if "." in candidate else int(candidate)
                elif isinstance(raw_id, (int, float)):
                    numeric_id = int(raw_id)
                else:
                    numeric_id = int(raw_id)
            except (TypeError, ValueError):
                invalid_ids.append(raw_id)
                continue

            key = str(numeric_id)
            if key not in seen:
                seen.add(key)
                normalized_ids.append((numeric_id, key))

        if invalid_ids:
            log.warning(LogEvents.CHEMBL_ACTIVITY_INVALID_ACTIVITY_IDS,
                invalid_count=len(invalid_ids),
            )

        if limit is not None:
            normalized_ids = normalized_ids[: max(int(limit), 0)]

        return normalized_ids

    def _collect_records_by_ids(
        self,
        normalized_ids: Sequence[tuple[int, str]],
        activity_iterator: ChemblActivityClient,
        *,
        select_fields: Sequence[str] | None,
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        """Iterate over IDs using the shared iterator while preserving cache semantics."""

        log = UnifiedLogger.get(__name__).bind(component=f"{self.pipeline_code}.extract")
        records: list[dict[str, Any]] = []
        success_count = 0
        fallback_count = 0
        error_count = 0
        cache_hits = 0
        api_calls = 0
        total_batches = 0

        key_order = [key for _, key in normalized_ids]
        key_to_numeric = {key: numeric_id for numeric_id, key in normalized_ids}

        for chunk in activity_iterator._chunk_identifiers(
            key_order,
            select_fields=select_fields,
        ):
            total_batches += 1
            batch_start = time.perf_counter()
            from_cache = False
            chunk_records: dict[str, dict[str, Any]] = {}

            try:
                cached_records = self._check_cache(chunk, self._chembl_release)
                if cached_records is not None:
                    from_cache = True
                    cache_hits += len(chunk)
                    chunk_records = cached_records
                else:
                    api_calls += 1
                    fetched_items = list(
                        activity_iterator.iterate_by_ids(
                            chunk,
                            select_fields=select_fields,
                        )
                    )
                    for item in fetched_items:
                        if not isinstance(item, Mapping):
                            continue
                        activity_value = item.get("activity_id")
                        if activity_value is None:
                            continue
                        chunk_records[str(activity_value)] = dict(item)
                    self._store_cache(chunk, chunk_records, self._chembl_release)

                success_in_batch = 0
                for key in chunk:
                    numeric_id = key_to_numeric.get(key)
                    if numeric_id is None:
                        continue
                    record = chunk_records.get(key)
                    if record and not record.get("error"):
                        materialized = self._materialize_activity_record(
                            record,
                            activity_id=numeric_id,
                        )
                        records.append(materialized)
                        success_count += 1
                        success_in_batch += 1
                    else:
                        fallback_record = self._create_fallback_record(numeric_id)
                        records.append(fallback_record)
                        fallback_count += 1
                        error_count += 1

                batch_duration_ms = (time.perf_counter() - batch_start) * 1000.0
                log.debug(LogEvents.CHEMBL_ACTIVITY_BATCH_PROCESSED,
                    batch_size=len(chunk),
                    from_cache=from_cache,
                    success_in_batch=success_in_batch,
                    fallback_in_batch=len(chunk) - success_in_batch,
                    duration_ms=batch_duration_ms,
                )
            except CircuitBreakerOpenError as exc:
                log.warning(LogEvents.CHEMBL_ACTIVITY_BATCH_CIRCUIT_BREAKER,
                    batch_size=len(chunk),
                    error=str(exc),
                )
                for key in chunk:
                    numeric_id = key_to_numeric.get(key)
                    if numeric_id is None:
                        continue
                    records.append(self._create_fallback_record(numeric_id, exc))
                    fallback_count += 1
                    error_count += 1
            except RequestException as exc:
                log.error(LogEvents.CHEMBL_ACTIVITY_BATCH_REQUEST_ERROR,
                    batch_size=len(chunk),
                    error=str(exc),
                )
                for key in chunk:
                    numeric_id = key_to_numeric.get(key)
                    if numeric_id is None:
                        continue
                    records.append(self._create_fallback_record(numeric_id, exc))
                    fallback_count += 1
                    error_count += 1
            except Exception as exc:  # pragma: no cover - defensive path
                log.error(LogEvents.CHEMBL_ACTIVITY_BATCH_UNHANDLED_ERROR,
                    batch_size=len(chunk),
                    error=str(exc),
                    exc_info=True,
                )
                for key in chunk:
                    numeric_id = key_to_numeric.get(key)
                    if numeric_id is None:
                        continue
                    records.append(self._create_fallback_record(numeric_id, exc))
                    fallback_count += 1
                    error_count += 1

        total_records = len(normalized_ids)
        success_rate = (
            float(success_count + fallback_count) / float(total_records) if total_records else 0.0
        )
        summary = {
            "total_activities": total_records,
            "success": success_count,
            "fallback": fallback_count,
            "errors": error_count,
            "api_calls": api_calls,
            "cache_hits": cache_hits,
            "batches": total_batches,
            "duration_ms": 0.0,
            "success_rate": success_rate,
        }

        return records, summary

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform raw activity data by normalizing measurements, identifiers, and data types."""

        log = UnifiedLogger.get(__name__).bind(component=f"{self.pipeline_code}.transform")

        # According to documentation, transform should accept df: pd.DataFrame
        # df is already a pd.DataFrame, so we can use it directly
        df = df.copy()

        df = self._harmonize_identifier_columns(df, log)
        df = self._ensure_schema_columns(df, COLUMN_ORDER, log)

        if df.empty:
            log.debug(LogEvents.TRANSFORM_EMPTY_DATAFRAME)
            return df

        log.info(LogEvents.STAGE_TRANSFORM_START, rows=len(df))

        df = self._normalize_identifiers(df, log)
        df = self._normalize_measurements(df, log)
        df = self._normalize_string_fields(df, log)
        df = self._normalize_nested_structures(df, log)
        df = self._add_row_metadata(df, log)
        df = self._normalize_data_types(df, ActivitySchema, log)

        # Recompute curated from curated_by when curated is empty.
        if "curated" in df.columns or "curated_by" in df.columns:
            if "curated" not in df.columns:
                df["curated"] = pd.NA
            if "curated_by" in df.columns:
                mask = df["curated"].isna()
                df.loc[mask, "curated"] = df.loc[mask, "curated_by"].notna()
            df["curated"] = df["curated"].astype("boolean")

        df = self._validate_foreign_keys(df, log)
        df = self._ensure_schema_columns(df, COLUMN_ORDER, log)

        # Enrichment: compound_record fields
        if self._should_enrich_compound_record():
            df = self._enrich_compound_record(df)

        # Enrichment: assay fields
        if self._should_enrich_assay():
            df = self._enrich_assay(df)

        # Enrichment: molecule fields
        if self._should_enrich_molecule():
            df = self._enrich_molecule(df)

        # Enrichment: data_validity fields
        if self._should_enrich_data_validity():
            df = self._enrich_data_validity(df)

        # Finalize identifier columns BEFORE ordering to ensure all required columns exist
        df = self._finalize_identifier_columns(df, log)
        df = self._order_schema_columns(df, COLUMN_ORDER)
        df = self._finalize_output_columns(df, log)
        df = self._filter_invalid_required_fields(df, log)

        log.info(LogEvents.STAGE_TRANSFORM_FINISH, rows=len(df))
        return df

    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate payload against ActivitySchema with detailed error handling."""

        log = UnifiedLogger.get(__name__).bind(component=f"{self.pipeline_code}.validate")

        if df.empty:
            log.debug(LogEvents.VALIDATE_EMPTY_DATAFRAME)
            return df

        if self.config.validation.strict:
            allowed_columns = set(COLUMN_ORDER)
            extra_columns = [column for column in df.columns if column not in allowed_columns]
            if extra_columns:
                log.debug(LogEvents.DROP_EXTRA_COLUMNS_BEFORE_VALIDATION,
                    extras=extra_columns,
                )
                df = df.drop(columns=extra_columns)

        log.info(LogEvents.STAGE_VALIDATE_START, rows=len(df))

        # Ensure target_tax_id has correct nullable integer type before validation
        if "target_tax_id" in df.columns:
            dtype_name: str = str(df["target_tax_id"].dtype.name)  # pyright: ignore[reportUnknownMemberType,reportUnknownArgumentType]
            if dtype_name != "Int64":
                # Convert to nullable Int64 if not already
                numeric_series: pd.Series[Any] = pd.to_numeric(  # pyright: ignore[reportUnknownMemberType]
                    df["target_tax_id"], errors="coerce"
                )
                df["target_tax_id"] = numeric_series.astype("Int64")

        # Pre-validation checks
        self._check_activity_id_uniqueness(df, log)
        self._check_foreign_key_integrity(df, log)

        # Soft-enum validation for data_validity_comment.
        self._validate_data_validity_comment_soft_enum(df, log)

        # Call base validation with error handling
        # Temporarily disable coercion to preserve nullable Int64 types for target_tax_id
        # Schema has coerce=False, but base.validate uses config.validation.coerce
        original_coerce = self.config.validation.coerce
        try:
            self.config.validation.coerce = False
            validated = super().validate(df)
            log.info(LogEvents.STAGE_VALIDATE_FINISH,
                rows=len(validated),
                schema=self.config.validation.schema_out,
                strict=self.config.validation.strict,
                coerce=original_coerce,  # Log original value for clarity
            )
            return validated
        except pandera.errors.SchemaErrors as exc:
            # Extract detailed error information
            failure_cases_df: pd.DataFrame | None = None
            if hasattr(exc, "failure_cases"):
                failure_cases_df = cast(pd.DataFrame, exc.failure_cases)
            error_count = len(failure_cases_df) if failure_cases_df is not None else 0
            error_summary = summarize_schema_errors(exc)

            log.error(LogEvents.VALIDATION_FAILED,
                error_count=error_count,
                schema=self.config.validation.schema_out,
                strict=self.config.validation.strict,
                coerce=original_coerce,  # Log original value for clarity
                error_summary=error_summary,
                exc_info=True,
            )

            # Log detailed failure cases if available
            if failure_cases_df is not None and not failure_cases_df.empty:
                failure_cases_summary = format_failure_cases(failure_cases_df)
                log.error(LogEvents.VALIDATION_FAILURE_CASES, failure_cases=failure_cases_summary)
                # Log individual errors with row index and activity_id as per documentation
                self._log_detailed_validation_errors(failure_cases_df, df, log)

            msg = (
                f"Validation failed with {error_count} error(s) against schema "
                f"{self.config.validation.schema_out}: {error_summary}"
            )
            raise ValueError(msg) from exc
        except Exception as exc:
            log.error(LogEvents.VALIDATION_ERROR,
                error=str(exc),
                schema=self.config.validation.schema_out,
                exc_info=True,
            )
            raise
        finally:
            self.config.validation.coerce = original_coerce

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _should_enrich_compound_record(self) -> bool:
        """Return True when compound_record enrichment is enabled in the config."""
        if not self.config.chembl:
            return False
        try:
            chembl_section = self.config.chembl
            activity_section: Any = chembl_section.get("activity")
            if not isinstance(activity_section, Mapping):
                return False
            activity_section = cast(Mapping[str, Any], activity_section)
            enrich_section: Any = activity_section.get("enrich")
            if not isinstance(enrich_section, Mapping):
                return False
            enrich_section = cast(Mapping[str, Any], enrich_section)
            compound_record_section: Any = enrich_section.get("compound_record")
            if not isinstance(compound_record_section, Mapping):
                return False
            compound_record_section = cast(Mapping[str, Any], compound_record_section)
            enabled: Any = compound_record_section.get("enabled")
            return bool(enabled) if enabled is not None else False
        except (AttributeError, KeyError, TypeError):
            return False

    def _enrich_compound_record(self, df: pd.DataFrame) -> pd.DataFrame:
        """Enrich the DataFrame with compound_record fields."""
        log = UnifiedLogger.get(__name__).bind(component=f"{self.pipeline_code}.enrich")

        # Retrieve enrichment configuration.
        enrich_cfg: dict[str, Any] = {}
        try:
            if self.config.chembl:
                chembl_section = self.config.chembl
                activity_section: Any = chembl_section.get("activity")
                if isinstance(activity_section, Mapping):
                    activity_section = cast(Mapping[str, Any], activity_section)
                    enrich_section: Any = activity_section.get("enrich")
                    if isinstance(enrich_section, Mapping):
                        enrich_section = cast(Mapping[str, Any], enrich_section)
                        compound_record_section: Any = enrich_section.get("compound_record")
                        if isinstance(compound_record_section, Mapping):
                            compound_record_section = cast(
                                Mapping[str, Any], compound_record_section
                            )
                            enrich_cfg = dict(compound_record_section)
        except (AttributeError, KeyError, TypeError) as exc:
            log.warning(LogEvents.ENRICHMENT_CONFIG_ERROR,
                error=str(exc),
                message="Using default enrichment config",
            )

        bundle = self._build_activity_enrichment_bundle(
            "compound_record",
            client_name="chembl_enrichment_client",
        )
        chembl_client = bundle.chembl_client

        # Invoke the enrichment routine.
        return enrich_with_compound_record(df, chembl_client, enrich_cfg)

    def _should_enrich_assay(self) -> bool:
        """Return True when assay enrichment is enabled in the config."""
        if not self.config.chembl:
            return False
        try:
            chembl_section = self.config.chembl
            activity_section: Any = chembl_section.get("activity")
            if not isinstance(activity_section, Mapping):
                return False
            activity_section = cast(Mapping[str, Any], activity_section)
            enrich_section: Any = activity_section.get("enrich")
            if not isinstance(enrich_section, Mapping):
                return False
            enrich_section = cast(Mapping[str, Any], enrich_section)
            assay_section: Any = enrich_section.get("assay")
            if not isinstance(assay_section, Mapping):
                return False
            assay_section = cast(Mapping[str, Any], assay_section)
            enabled: Any = assay_section.get("enabled")
            return bool(enabled) if enabled is not None else False
        except (AttributeError, KeyError, TypeError):
            return False

    def _enrich_assay(self, df: pd.DataFrame) -> pd.DataFrame:
        """Enrich the DataFrame with assay fields."""
        log = UnifiedLogger.get(__name__).bind(component=f"{self.pipeline_code}.enrich")

        # Retrieve enrichment configuration.
        enrich_cfg: dict[str, Any] = {}
        try:
            if self.config.chembl:
                chembl_section = self.config.chembl
                activity_section: Any = chembl_section.get("activity")
                if isinstance(activity_section, Mapping):
                    activity_section = cast(Mapping[str, Any], activity_section)
                    enrich_section: Any = activity_section.get("enrich")
                    if isinstance(enrich_section, Mapping):
                        enrich_section = cast(Mapping[str, Any], enrich_section)
                        assay_section: Any = enrich_section.get("assay")
                        if isinstance(assay_section, Mapping):
                            assay_section = cast(Mapping[str, Any], assay_section)
                            enrich_cfg = dict(assay_section)
        except (AttributeError, KeyError, TypeError) as exc:
            log.warning(LogEvents.ENRICHMENT_CONFIG_ERROR,
                error=str(exc),
                message="Using default enrichment config",
            )

        bundle = self._build_activity_enrichment_bundle(
            "assay",
            client_name="chembl_enrichment_client",
        )
        chembl_client = bundle.chembl_client

        # Invoke the enrichment routine.
        return enrich_with_assay(df, chembl_client, enrich_cfg)

    def _should_enrich_data_validity(self) -> bool:
        """Return True when data_validity enrichment is enabled in the config."""
        if not self.config.chembl:
            return False
        try:
            chembl_section = self.config.chembl
            activity_section: Any = chembl_section.get("activity")
            if not isinstance(activity_section, Mapping):
                return False
            activity_section = cast(Mapping[str, Any], activity_section)
            enrich_section: Any = activity_section.get("enrich")
            if not isinstance(enrich_section, Mapping):
                return False
            enrich_section = cast(Mapping[str, Any], enrich_section)
            data_validity_section: Any = enrich_section.get("data_validity")
            if not isinstance(data_validity_section, Mapping):
                return False
            data_validity_section = cast(Mapping[str, Any], data_validity_section)
            enabled: Any = data_validity_section.get("enabled")
            return bool(enabled) if enabled is not None else False
        except (AttributeError, KeyError, TypeError):
            return False

    def _enrich_data_validity(self, df: pd.DataFrame) -> pd.DataFrame:
        """Enrich the DataFrame with data_validity_lookup fields."""
        log = UnifiedLogger.get(__name__).bind(component=f"{self.pipeline_code}.enrich")

        # Retrieve enrichment configuration.
        enrich_cfg: dict[str, Any] = {}
        try:
            if self.config.chembl:
                chembl_section = self.config.chembl
                activity_section: Any = chembl_section.get("activity")
                if isinstance(activity_section, Mapping):
                    activity_section = cast(Mapping[str, Any], activity_section)
                    enrich_section: Any = activity_section.get("enrich")
                    if isinstance(enrich_section, Mapping):
                        enrich_section = cast(Mapping[str, Any], enrich_section)
                        data_validity_section: Any = enrich_section.get("data_validity")
                        if isinstance(data_validity_section, Mapping):
                            data_validity_section = cast(Mapping[str, Any], data_validity_section)
                            enrich_cfg = dict(data_validity_section)
        except (AttributeError, KeyError, TypeError) as exc:
            log.warning(LogEvents.ENRICHMENT_CONFIG_ERROR,
                error=str(exc),
                message="Using default enrichment config",
            )

        # If data_validity_description already contains values (not all NA), log an informational message.
        if "data_validity_description" in df.columns:
            non_na_count = int(df["data_validity_description"].notna().sum())
            if non_na_count > 0:
                log.info(
                    "enrichment_data_validity_description_already_filled",
                    non_na_count=non_na_count,
                    total_count=len(df),
                    message="data_validity_description already populated from extract, enrichment will update/overwrite",
                )

        bundle = self._build_activity_enrichment_bundle(
            "data_validity_lookup",
            client_name="chembl_enrichment_client",
        )
        chembl_client = bundle.chembl_client

        # Invoke the enrichment routine (may update/overwrite extract data).
        return enrich_with_data_validity(df, chembl_client, enrich_cfg)

    def _should_enrich_molecule(self) -> bool:
        """Return True when molecule enrichment is enabled in the config."""
        if not self.config.chembl:
            return False
        try:
            chembl_section = self.config.chembl
            activity_section: Any = chembl_section.get("activity")
            if not isinstance(activity_section, Mapping):
                return False
            activity_section = cast(Mapping[str, Any], activity_section)
            enrich_section: Any = activity_section.get("enrich")
            if not isinstance(enrich_section, Mapping):
                return False
            enrich_section = cast(Mapping[str, Any], enrich_section)
            molecule_section: Any = enrich_section.get("molecule")
            if not isinstance(molecule_section, Mapping):
                return False
            molecule_section = cast(Mapping[str, Any], molecule_section)
            enabled: Any = molecule_section.get("enabled")
            return bool(enabled) if enabled is not None else False
        except (AttributeError, KeyError, TypeError):
            return False

    def _enrich_molecule(self, df: pd.DataFrame) -> pd.DataFrame:
        """Enrich the DataFrame with molecule fields via join."""
        log = UnifiedLogger.get(__name__).bind(component=f"{self.pipeline_code}.enrich")

        # Retrieve enrichment configuration.
        enrich_cfg: dict[str, Any] = {}
        try:
            if self.config.chembl:
                chembl_section = self.config.chembl
                activity_section: Any = chembl_section.get("activity")
                if isinstance(activity_section, Mapping):
                    activity_section = cast(Mapping[str, Any], activity_section)
                    enrich_section: Any = activity_section.get("enrich")
                    if isinstance(enrich_section, Mapping):
                        enrich_section = cast(Mapping[str, Any], enrich_section)
                        molecule_section: Any = enrich_section.get("molecule")
                        if isinstance(molecule_section, Mapping):
                            molecule_section = cast(Mapping[str, Any], molecule_section)
                            enrich_cfg = dict(molecule_section)
        except (AttributeError, KeyError, TypeError) as exc:
            log.warning(LogEvents.ENRICHMENT_CONFIG_ERROR,
                error=str(exc),
                message="Using default enrichment config",
            )

        bundle = self._build_activity_enrichment_bundle(
            "molecule",
            client_name="chembl_enrichment_client",
        )
        chembl_client = bundle.chembl_client

        # Execute the join to fetch molecule_name.
        df_join = join_activity_with_molecule(df, chembl_client, enrich_cfg)

        # Coalesce joined molecule_name into molecule_pref_name when missing.
        if "molecule_name" in df_join.columns:
            if "molecule_pref_name" not in df.columns:
                df["molecule_pref_name"] = pd.NA
            # Fill molecule_pref_name using molecule_name where empty.
            mask = df["molecule_pref_name"].isna() | (
                df["molecule_pref_name"].astype("string").str.strip() == ""
            )
            if mask.any():
                # Merge on activity_id to retrieve molecule_name.
                df_merged = df.merge(
                    df_join[["activity_id", "molecule_name"]],
                    on="activity_id",
                    how="left",
                    suffixes=("", "_join"),
                )
                # Populate molecule_pref_name using molecule_name where empty.
                df.loc[mask, "molecule_pref_name"] = df_merged.loc[mask, "molecule_name"]
                log.info(LogEvents.MOLECULE_PREF_NAME_ENRICHED,
                    rows_enriched=int(mask.sum()),
                    total_rows=len(df),
                )

        return df

    def _extract_from_chembl(
        self,
        dataset: object,
        chembl_client: ChemblClient | Any,
        activity_iterator: ChemblActivityClient,
        *,
        limit: int | None = None,
        select_fields: Sequence[str] | None = None,
    ) -> pd.DataFrame:
        """Extract activity records by delegating batching to ``ChemblActivityClient``."""

        log = UnifiedLogger.get(__name__).bind(component=f"{self.pipeline_code}.extract")
        method_start = time.perf_counter()
        self._last_batch_extract_stats = None

        input_frame = self._coerce_activity_dataset(dataset)
        if "activity_id" not in input_frame.columns:
            msg = "Input dataset must contain an 'activity_id' column"
            raise ValueError(msg)

        normalized_ids = self._normalize_activity_ids(
            input_frame,
            limit=limit,
            log=log,
        )

        if not normalized_ids:
            summary: dict[str, Any] = {
                "total_activities": 0,
                "success": 0,
                "fallback": 0,
                "errors": 0,
                "api_calls": 0,
                "cache_hits": 0,
                "batches": 0,
                "duration_ms": 0.0,
                "success_rate": 0.0,
            }
            self._last_batch_extract_stats = summary
            log.info(LogEvents.CHEMBL_ACTIVITY_BATCH_SUMMARY, **summary)
            empty_frame = pd.DataFrame({"activity_id": pd.Series(dtype="Int64")})
            return self._ensure_comment_fields(empty_frame, log)

        records, summary = self._collect_records_by_ids(
            normalized_ids,
            activity_iterator,
            select_fields=select_fields,
        )
        duration_ms = (time.perf_counter() - method_start) * 1000.0
        summary["duration_ms"] = duration_ms
        self._last_batch_extract_stats = summary
        log.info(LogEvents.CHEMBL_ACTIVITY_BATCH_SUMMARY, **summary)

        result_df: pd.DataFrame = pd.DataFrame.from_records(records)  # pyright: ignore[reportUnknownMemberType]; type: ignore
        if result_df.empty:
            result_df = pd.DataFrame({"activity_id": pd.Series(dtype="Int64")})
        elif "activity_id" in result_df.columns:
            result_df = result_df.sort_values("activity_id").reset_index(drop=True)

        result_df = self._ensure_comment_fields(result_df, log)
        result_df = self._extract_data_validity_descriptions(result_df, chembl_client, log)
        result_df = self._extract_assay_fields(result_df, chembl_client, log)
        self._log_validity_comments_metrics(result_df, log)

        return result_df

    def _check_cache(
        self,
        batch_ids: Sequence[str],
        release: str | None,
    ) -> dict[str, dict[str, Any]] | None:
        cache_config = self.config.cache
        if not cache_config.enabled:
            return None

        normalized_ids = [str(identifier) for identifier in batch_ids]
        cache_file = self._cache_file_path(normalized_ids, release)
        if not cache_file.exists():
            return None

        try:
            stat = cache_file.stat()
        except OSError:
            return None

        ttl_seconds = int(cache_config.ttl)
        if ttl_seconds > 0 and (time.time() - stat.st_mtime) > ttl_seconds:
            try:
                cache_file.unlink(missing_ok=True)
            except OSError:
                pass
            return None

        try:
            payload = json.loads(cache_file.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            try:
                cache_file.unlink(missing_ok=True)
            except OSError:
                pass
            return None

        if not isinstance(payload, dict):
            return None

        missing = [identifier for identifier in normalized_ids if identifier not in payload]
        if missing:
            return None

        return {
            identifier: cast(dict[str, Any], payload[identifier]) for identifier in normalized_ids
        }

    def _store_cache(
        self,
        batch_ids: Sequence[str],
        batch_data: Mapping[str, Mapping[str, Any]],
        release: str | None,
    ) -> None:
        cache_config = self.config.cache
        if not cache_config.enabled or not batch_ids or not batch_data:
            return

        normalized_ids = [str(identifier) for identifier in batch_ids]
        cache_file = self._cache_file_path(normalized_ids, release)
        normalized_set = set(normalized_ids)
        data_to_store = {key: batch_data[key] for key in normalized_set if key in batch_data}
        if not data_to_store:
            return

        try:
            cache_file.parent.mkdir(parents=True, exist_ok=True)
            tmp_path = cache_file.with_suffix(cache_file.suffix + ".tmp")
            tmp_path.write_text(
                json.dumps(data_to_store, sort_keys=True, default=str),
                encoding="utf-8",
            )
            tmp_path.replace(cache_file)
        except Exception as exc:  # pragma: no cover - cache best-effort
            log = UnifiedLogger.get(__name__).bind(component=f"{self.pipeline_code}.extract")
            log.debug(LogEvents.CHEMBL_ACTIVITY_CACHE_STORE_FAILED, error=str(exc))

    def _cache_file_path(self, batch_ids: Sequence[str], release: str | None) -> Path:
        directory = self._cache_directory(release)
        cache_key = self._cache_key(batch_ids, release)
        return directory / f"{cache_key}.json"

    def _cache_directory(self, release: str | None) -> Path:
        cache_root = Path(self.config.paths.cache_root)
        directory_name = (self.config.cache.directory or "http_cache").strip() or "http_cache"
        release_component = self._sanitize_cache_component(release or "unknown")
        pipeline_component = self._sanitize_cache_component(self.pipeline_code)
        version_component = self._sanitize_cache_component(
            self.config.pipeline.version or "unknown"
        )
        return (
            cache_root / directory_name / pipeline_component / release_component / version_component
        )

    def _cache_key(self, batch_ids: Sequence[str], release: str | None) -> str:
        payload = {
            "ids": list(batch_ids),
            "release": release or "unknown",
            "pipeline": self.pipeline_code,
            "pipeline_version": self.config.pipeline.version or "unknown",
        }
        raw = json.dumps(payload, sort_keys=True)
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    @staticmethod
    def _sanitize_cache_component(value: str) -> str:
        sanitized = re.sub(r"[^0-9A-Za-z_.-]", "_", value)
        return sanitized or "default"

    def _create_fallback_record(
        self, activity_id: int, error: Exception | None = None
    ) -> dict[str, Any]:
        """Create fallback record enriched with error metadata."""

        base_message = "Fallback: ChEMBL activity unavailable"
        message = f"{base_message} ({error})" if error else base_message
        timestamp = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        metadata: dict[str, Any] = {
            "source_system": "ChEMBL_FALLBACK",
            "error_type": error.__class__.__name__ if error else None,
            "chembl_release": self._chembl_release,
            "run_id": self.run_id,
            "timestamp": timestamp,
        }
        if isinstance(error, RequestException):
            response = getattr(error, "response", None)
            status_code = getattr(response, "status_code", None)
            if status_code is not None:
                metadata["http_status"] = status_code
            metadata["error_message"] = str(error)
        elif error is not None:
            metadata["error_message"] = str(error)

        fallback_properties = [
            {
                "type": "fallback_metadata",
                "relation": None,
                "units": None,
                "value": None,
                "text_value": json.dumps(metadata, sort_keys=True, default=str),
                "result_flag": None,
            }
        ]

        return {
            "activity_id": activity_id,
            "data_validity_comment": message,
            "activity_properties": self._serialize_activity_properties(fallback_properties),
        }

    def _ensure_comment_fields(self, df: pd.DataFrame, log: BoundLogger) -> pd.DataFrame:
        """Ensure comment fields exist in the DataFrame with string dtype defaults.

        Missing columns are created with pd.NA. data_validity_description is populated via
        _extract_data_validity_descriptions().

        Parameters
        ----------
        df:
            DataFrame to inspect and augment.
        log:
            Logger instance.

        Returns
        -------
        pd.DataFrame:
            DataFrame guaranteed to include comment fields.
        """
        if df.empty:
            return df

        required_comment_fields = [
            "activity_comment",
            "data_validity_comment",
            "data_validity_description",
        ]
        missing_fields = [field for field in required_comment_fields if field not in df.columns]

        if missing_fields:
            for field in missing_fields:
                df[field] = pd.Series([pd.NA] * len(df), dtype="string")
            log.debug(LogEvents.COMMENT_FIELDS_ENSURED, fields=missing_fields)

        return df

    def _extract_data_validity_descriptions(
        self, df: pd.DataFrame, client: ChemblClient, log: BoundLogger
    ) -> pd.DataFrame:
        """Populate data_validity_description via DATA_VALIDITY_LOOKUP.

        Collects unique non-empty data_validity_comment values, calls fetch_data_validity_lookup(),
        and left joins the results back to the DataFrame.

        Parameters
        ----------
        df:
            Activity DataFrame containing data_validity_comment.
        client:
            ChemblClient used for API requests.
        log:
            Logger instance.

        Returns
        -------
        pd.DataFrame:
            DataFrame with data_validity_description populated.
        """
        if df.empty:
            return df

        if "data_validity_comment" not in df.columns:
            log.debug(LogEvents.EXTRACT_DATA_VALIDITY_DESCRIPTIONS_SKIPPED,
                reason="data_validity_comment_column_missing",
            )
            return df

        # Collect unique non-empty data_validity_comment values.
        validity_comments: list[str] = []
        for _, row in df.iterrows():
            comment = row.get("data_validity_comment")

            # Skip NaN/None values.
            if pd.isna(comment) or comment is None:
                continue

            # Convert to stripped string.
            comment_str = str(comment).strip()

            if comment_str:
                validity_comments.append(comment_str)

        if not validity_comments:
            log.debug(LogEvents.EXTRACT_DATA_VALIDITY_DESCRIPTIONS_SKIPPED, reason="no_valid_comments")
            # Ensure the column exists with pd.NA.
            if "data_validity_description" not in df.columns:
                df["data_validity_description"] = pd.Series([pd.NA] * len(df), dtype="string")
            return df

        # Fetch descriptions via fetch_data_validity_lookup.
        unique_comments = list(set(validity_comments))
        log.info(LogEvents.EXTRACT_DATA_VALIDITY_DESCRIPTIONS_FETCHING, comments_count=len(unique_comments))

        try:
            records_df = client.fetch_data_validity_lookup(
                comments=unique_comments,
                fields=["data_validity_comment", "description"],
                page_limit=1000,
            )
        except Exception as exc:
            log.warning(
                "extract_data_validity_descriptions_fetch_error",
                error=str(exc),
                exc_info=True,
            )
            # Ensure the column exists with pd.NA.
            if "data_validity_description" not in df.columns:
                df["data_validity_description"] = pd.Series([pd.NA] * len(df), dtype="string")
            return df

        if records_df.empty:
            log.debug(LogEvents.EXTRACT_DATA_VALIDITY_DESCRIPTIONS_NO_RECORDS)
            # Ensure the column exists with pd.NA.
            if "data_validity_description" not in df.columns:
                df["data_validity_description"] = pd.Series([pd.NA] * len(df), dtype="string")
            return df

        df_enrich = (
            records_df.loc[:, ["data_validity_comment", "description"]]
            .drop_duplicates(subset=["data_validity_comment"], keep="first")
            .sort_values(by=["data_validity_comment"], kind="mergesort")
            .rename(columns={"description": "data_validity_description"})
            .reset_index(drop=True)
        )

        # Preserve the original row order via index.
        original_index = df.index.copy()

        # LEFT JOIN back onto the DataFrame by data_validity_comment.
        df_result = df.merge(
            df_enrich,
            on=["data_validity_comment"],
            how="left",
            suffixes=("", "_enrich"),
        )

        # Ensure the column exists (fill NA for missing entries).
        if "data_validity_description" not in df_result.columns:
            df_result["data_validity_description"] = pd.Series(
                [pd.NA] * len(df_result), dtype="string"
            )
        else:
            # Normalize types for existing columns.
            df_result["data_validity_description"] = df_result["data_validity_description"].astype(
                "string"
            )

        # Restore original order.
        df_result = df_result.reindex(original_index)

        log.info(LogEvents.EXTRACT_DATA_VALIDITY_DESCRIPTIONS_COMPLETE,
            comments_requested=len(unique_comments),
            records_fetched=len(records_df),
            rows_enriched=len(df_result),
        )

        return df_result

    def _extract_assay_fields(
        self, df: pd.DataFrame, client: ChemblClient, log: BoundLogger
    ) -> pd.DataFrame:
        """Populate assay_organism and assay_tax_id via ASSAYS lookup.

        Collects unique non-empty assay_chembl_id values from the DataFrame, calls
        fetch_assays_by_ids(), and left joins the results back.

        Parameters
        ----------
        df:
            Activity DataFrame containing assay_chembl_id.
        client:
            ChemblClient used for API requests.
        log:
            Logger instance.

        Returns
        -------
        pd.DataFrame:
            DataFrame with assay_organism and assay_tax_id populated.
        """
        if df.empty:
            return df

        if "assay_chembl_id" not in df.columns:
            log.debug(LogEvents.EXTRACT_ASSAY_FIELDS_SKIPPED, reason="assay_chembl_id_column_missing")
            # Ensure columns exist with pd.NA.
            for col, dtype in (("assay_organism", "string"), ("assay_tax_id", "Int64")):
                if col not in df.columns:
                    df[col] = pd.Series([pd.NA] * len(df), dtype=dtype)
            return df

        # Collect unique non-empty assay_chembl_id values.
        assay_ids: list[str] = []
        for _, row in df.iterrows():
            assay_id = row.get("assay_chembl_id")

            # Skip NaN/None values.
            if pd.isna(assay_id) or assay_id is None:
                continue

            # Normalize to uppercase stripped string.
            assay_id_str = str(assay_id).strip().upper()

            if assay_id_str:
                assay_ids.append(assay_id_str)

        if not assay_ids:
            log.debug(LogEvents.EXTRACT_ASSAY_FIELDS_SKIPPED, reason="no_valid_assay_ids")
            # Ensure columns exist with pd.NA.
            for col, dtype in (("assay_organism", "string"), ("assay_tax_id", "Int64")):
                if col not in df.columns:
                    df[col] = pd.Series([pd.NA] * len(df), dtype=dtype)
            return df

        # Fetch assay data via fetch_assays_by_ids.
        unique_assay_ids = list(set(assay_ids))
        log.info(LogEvents.EXTRACT_ASSAY_FIELDS_FETCHING, assay_ids_count=len(unique_assay_ids))

        try:
            records_df = client.fetch_assays_by_ids(
                ids=unique_assay_ids,
                fields=["assay_chembl_id", "assay_organism", "assay_tax_id"],
                page_limit=1000,
            )
        except Exception as exc:
            log.warning(LogEvents.EXTRACT_ASSAY_FIELDS_FETCH_ERROR,
                error=str(exc),
                exc_info=True,
            )
            # Ensure columns exist with pd.NA.
            for col, dtype in (("assay_organism", "string"), ("assay_tax_id", "Int64")):
                if col not in df.columns:
                    df[col] = pd.Series([pd.NA] * len(df), dtype=dtype)
            return df

        if records_df.empty:
            log.debug(LogEvents.EXTRACT_ASSAY_FIELDS_NO_RECORDS)
            # Ensure columns exist with pd.NA.
            for col, dtype in (("assay_organism", "string"), ("assay_tax_id", "Int64")):
                if col not in df.columns:
                    df[col] = pd.Series([pd.NA] * len(df), dtype=dtype)
            return df

        df_enrich = (
            records_df.loc[:, ["assay_chembl_id", "assay_organism", "assay_tax_id"]]
            .dropna(subset=["assay_chembl_id"])
            .copy()
        )
        df_enrich["assay_chembl_id"] = df_enrich["assay_chembl_id"].astype("string")

        df_enrich["assay_chembl_id"] = (
            df_enrich["assay_chembl_id"].astype("string").str.strip().str.upper()
        )
        df_enrich = (
            df_enrich.drop_duplicates(subset=["assay_chembl_id"], keep="first")
            .sort_values(by=["assay_chembl_id"], kind="mergesort")
            .reset_index(drop=True)
        )

        # Normalize assay_chembl_id in the base DataFrame for joining.
        original_index = df.index.copy()
        df_normalized = df.copy()
        df_normalized["assay_chembl_id_normalized"] = (
            df_normalized["assay_chembl_id"].astype("string").str.strip().str.upper()
        )

        # Perform a LEFT JOIN back onto the DataFrame.
        df_result = df_normalized.merge(
            df_enrich,
            left_on="assay_chembl_id_normalized",
            right_on="assay_chembl_id",
            how="left",
            suffixes=("", "_enrich"),
        )

        # Drop the temporary normalized column.
        df_result = df_result.drop(columns=["assay_chembl_id_normalized"])

        # Coalesce *_enrich columns back into base columns.
        for col in ["assay_organism", "assay_tax_id"]:
            if f"{col}_enrich" in df_result.columns:
                # Use enrichment values when base values are NA.
                if col not in df_result.columns:
                    df_result[col] = df_result[f"{col}_enrich"]
                else:
                    # Replace NA using enrichment values.
                    base_series: pd.Series[Any] = df_result[col]
                    enrich_series: pd.Series[Any] = df_result[f"{col}_enrich"]
                    missing_mask = base_series.isna()
                    if bool(missing_mask.any()):
                        df_result.loc[missing_mask, col] = enrich_series.loc[missing_mask]
                # Remove the *_enrich column.
                df_result = df_result.drop(columns=[f"{col}_enrich"])
        if "assay_chembl_id_enrich" in df_result.columns:
            df_result = df_result.drop(columns=["assay_chembl_id_enrich"])

        # Ensure columns exist, filling with NA where necessary.
        for col, dtype in (("assay_organism", "string"), ("assay_tax_id", "Int64")):
            if col not in df_result.columns:
                df_result[col] = pd.Series([pd.NA] * len(df_result), dtype=dtype)

        # Cast to expected types.
        df_result["assay_organism"] = df_result["assay_organism"].astype("string")

        # Convert assay_tax_id to Int64 with NA handling.
        df_result["assay_tax_id"] = pd.to_numeric(  # pyright: ignore[reportUnknownMemberType]
            df_result["assay_tax_id"], errors="coerce"
        ).astype("Int64")

        # Verify assay_tax_id range (>= 1 or NA).
        mask_valid = df_result["assay_tax_id"].notna()
        if mask_valid.any():
            invalid_mask = mask_valid & (df_result["assay_tax_id"] < 1)
            if invalid_mask.any():
                log.warning(LogEvents.INVALID_ASSAY_TAX_ID_RANGE,
                    count=int(invalid_mask.sum()),
                )
                df_result.loc[invalid_mask, "assay_tax_id"] = pd.NA

        # Restore the original order.
        df_result = df_result.reindex(original_index)

        log.info(LogEvents.EXTRACT_ASSAY_FIELDS_COMPLETE,
            assay_ids_requested=len(unique_assay_ids),
            records_fetched=len(df_enrich),
            rows_enriched=len(df_result),
        )

        return df_result

    def _log_validity_comments_metrics(self, df: pd.DataFrame, log: BoundLogger) -> None:
        """Log coverage metrics for comment fields.

        Calculates:
        - NA share for each of the three comment fields.
        - Top 10 most frequent data_validity_comment values.
        - Count of unknown data_validity_comment values (outside whitelist).

        Parameters
        ----------
        df:
            Activity DataFrame to analyze.
        log:
            Logger instance.
        """
        if df.empty:
            return

        # Compute NA ratios.
        metrics: dict[str, Any] = {}
        comment_fields = ["activity_comment", "data_validity_comment", "data_validity_description"]

        for field in comment_fields:
            if field in df.columns:
                na_count = int(df[field].isna().sum())
                total_count = len(df)
                na_rate = float(na_count) / float(total_count) if total_count > 0 else 0.0
                metrics[f"{field}_na_rate"] = na_rate
                metrics[f"{field}_na_count"] = na_count
                metrics[f"{field}_total_count"] = total_count

        # Compute top 10 values for data_validity_comment.
        non_null_comments_series: pd.Series[str] | None = None
        if "data_validity_comment" in df.columns:
            series_candidate = df["data_validity_comment"].dropna()
            if len(series_candidate) > 0:
                typed_series: pd.Series[str] = series_candidate.astype("string")
                non_null_comments_series = typed_series
                value_counts = typed_series.value_counts().head(10)
                top_10 = {str(key): int(value) for key, value in value_counts.items()}
                metrics["top_10_data_validity_comments"] = top_10

        # Count unknown data_validity_comment entries.
        whitelist = self._get_data_validity_comment_whitelist()
        if whitelist and non_null_comments_series is not None:
            whitelist_set: set[str] = set(whitelist)

            def _is_unknown(value: str) -> bool:
                return value not in whitelist_set

            unknown_mask = non_null_comments_series.map(_is_unknown).astype(bool)
            unknown_count = int(unknown_mask.sum())
            if unknown_count > 0:
                unknown_values = {
                    str(key): int(value)
                    for key, value in non_null_comments_series[unknown_mask]
                    .value_counts()
                    .head(10)
                    .items()
                }
                metrics["unknown_data_validity_comments_count"] = unknown_count
                metrics["unknown_data_validity_comments_samples"] = unknown_values
                log.warning(LogEvents.UNKNOWN_DATA_VALIDITY_COMMENTS_DETECTED.legacy(),
                    unknown_count=unknown_count,
                    samples=unknown_values,
                    whitelist=whitelist,
                )

        if metrics:
            log.info(LogEvents.VALIDITY_COMMENTS_METRICS.legacy(), **metrics)

    def _get_data_validity_comment_whitelist(self) -> list[str]:
        """Load allowed data_validity_comment values from the vocabulary store.

        Raises
        ------
        RuntimeError
            When the dictionary is unavailable or empty.
        """

        try:
            values = sorted(self._required_vocab_ids("data_validity_comment"))
        except RuntimeError as exc:
            UnifiedLogger.get(__name__).bind(
                component=f"{self.pipeline_code}.validation",
                run_id=self.run_id,
            ).error(
                "data_validity_comment_whitelist_unavailable",
                error=str(exc),
            )
            raise

        return values

    def _get_standard_types(self) -> frozenset[str]:
        """Return cached set of allowed standard_type values."""

        if self._standard_types_cache is None:
            values = self._vocabulary_service.required_ids(
                "activity_standard_type",
                allowed_statuses=("active",),
            )
            self._standard_types_cache = frozenset(values)
        return self._standard_types_cache

    def _extract_nested_fields(self, record: dict[str, Any]) -> dict[str, Any]:
        """Extract fields from nested assay and molecule objects."""
        # Extract assay_organism and assay_tax_id from nested assay object
        if "assay" in record and isinstance(record["assay"], Mapping):
            assay = cast(Mapping[str, Any], record["assay"])
            if "organism" in assay:
                record.setdefault("assay_organism", assay["organism"])
            if "tax_id" in assay:
                record.setdefault("assay_tax_id", assay["tax_id"])

        # Extract molecule_pref_name from nested molecule object
        if "molecule" in record and isinstance(record["molecule"], Mapping):
            molecule = cast(Mapping[str, Any], record["molecule"])
            if "pref_name" in molecule:
                record.setdefault("molecule_pref_name", molecule["pref_name"])

        # Extract curated from curated_by: (curated_by IS NOT NULL) -> bool
        if "curated_by" in record:
            curated_by = record.get("curated_by")
            if curated_by is not None and not pd.isna(curated_by):
                record.setdefault("curated", True)
            else:
                record.setdefault("curated", False)
        elif "curated" not in record:
            record.setdefault("curated", None)

        return record

    def _extract_activity_properties_fields(self, record: dict[str, Any]) -> dict[str, Any]:
        """Extract TRUV fields, standard_* fields, and comments from activity_properties as fallback.

        Fields are sourced from activity_properties only when missing in the primary API response
        (ACTIVITIES takes precedence). Extracts original TRUV fields (value, text_value, relation,
        units), standardized fields (standard_upper_value, standard_text_value), range fields
        (upper_value, lower_value), and comments (activity_comment, data_validity_comment).

        Also normalizes and stores activity_properties for serialization, validating TRUV format
        and deduplicating exact duplicates.
        """
        log = UnifiedLogger.get(__name__).bind(
            component=f"{self.pipeline_code}.extract_activity_properties"
        )
        activity_id = record.get("activity_id")

        # Check presence of activity_properties (compatibility with ChEMBL < v24).
        if "activity_properties" not in record:
            log.warning(LogEvents.ACTIVITY_PROPERTIES_MISSING,
                activity_id=activity_id,
                message="activity_properties not found in API response (possible ChEMBL < v24)",
            )
            record["activity_properties"] = None
            return record

        properties = record["activity_properties"]
        if properties is None:
            log.debug(LogEvents.ACTIVITY_PROPERTIES_NULL,
                activity_id=activity_id,
                message="activity_properties is None (possible ChEMBL < v24)",
            )
            return record

        # Parse JSON string if provided.
        if isinstance(properties, str):
            try:
                properties = json.loads(properties)
            except (TypeError, ValueError, json.JSONDecodeError) as exc:
                log.debug(LogEvents.ACTIVITY_PROPERTIES_PARSE_FAILED,
                    error=str(exc),
                    activity_id=record.get("activity_id"),
                )
                return record

        # Ensure we operate on a list of properties.
        if not isinstance(properties, Sequence) or isinstance(properties, (str, bytes)):
            return record

        property_iterable: Iterable[Any] = cast(Iterable[Any], properties)
        property_items: list[Any] = list(property_iterable)

        # Helper for fallback assignment.
        def _set_fallback(key: str, value: Any) -> None:
            """Set fallback value only if key is missing in record and value is not None."""
            if value is not None and record.get(key) is None:
                record[key] = value

        # Helper to check if value is empty (None, empty string, or whitespace)
        def _is_empty(value: Any) -> bool:
            """Check if value is empty (None, empty string, or whitespace)."""
            if value is None:
                return True
            if isinstance(value, str):
                return not value.strip()
            return False

        # Filter valid property items: must be Mapping with type and at least one of value/text_value
        items: list[Mapping[str, Any]] = []
        for property_item in property_items:
            if (
                isinstance(property_item, Mapping)
                and ("type" in property_item)
                and ("value" in property_item or "text_value" in property_item)
            ):
                items.append(cast(Mapping[str, Any], property_item))

        # Helper to check if property represents measured result (result_flag == 1 or True)
        def _is_measured(p: Mapping[str, Any]) -> bool:
            rf = p.get("result_flag")
            return (rf is True) or (isinstance(rf, int) and rf == 1)

        # Sort: measured results (result_flag == 1) first, then others.
        items.sort(key=lambda p: (not _is_measured(p)))

        # Process items: extract TRUV fields, standard_* fields, ranges, and comments as fallback.
        for prop in items:
            val = prop.get("value")
            txt = prop.get("text_value")
            rel = prop.get("relation")
            unt = prop.get("units")
            prop_type = str(prop.get("type", "")).lower()

            # Determine whether value or text_value will be populated.
            will_set_value = val is not None and record.get("value") is None
            will_set_text_value = txt is not None and record.get("text_value") is None

            # Fallback for original/TRUV fields.
            _set_fallback("value", val)
            _set_fallback("text_value", txt)

            # Pull paired fields from the same property when setting value/text_value.
            if will_set_value or will_set_text_value:
                _set_fallback("relation", rel)
                _set_fallback("units", unt)

            # Fallback for units when still unset.
            if unt is not None and record.get("units") is None:
                _set_fallback("units", unt)

            # Fallback for upper_value (types "upper", "upper_value", "upper limit").
            if record.get("upper_value") is None and (
                "upper" in prop_type or prop_type in ("upper_value", "upper limit")
            ):
                if val is not None:
                    _set_fallback("upper_value", val)

            # Fallback for lower_value (types "lower", "lower_value", "lower limit").
            if record.get("lower_value") is None and (
                "lower" in prop_type or prop_type in ("lower_value", "lower limit")
            ):
                if val is not None:
                    _set_fallback("lower_value", val)

            # Fallback for standard_upper_value (types "standard_upper", "standard upper", "standard upper value").
            if record.get("standard_upper_value") is None and (
                "standard_upper" in prop_type
                or prop_type in ("standard upper", "standard upper value")
            ):
                if val is not None:
                    _set_fallback("standard_upper_value", val)

            # Fallback for standard_text_value (types containing both "standard" and "text").
            if (
                record.get("standard_text_value") is None
                and "standard" in prop_type
                and "text" in prop_type
            ):
                if txt is not None:
                    _set_fallback("standard_text_value", txt)
                elif val is not None:
                    _set_fallback("standard_text_value", val)

        # Fallback logic for data_validity_comment: extract from activity_properties when empty.
        current_comment = record.get("data_validity_comment")
        if _is_empty(current_comment):
            # Locate items whose type contains "data_validity"/"validity" and provide text_value or value.
            data_validity_items: list[Mapping[str, Any]] = [
                prop
                for prop in items
                if (
                    "data_validity" in str(prop.get("type", "")).lower()
                    or "validity" in str(prop.get("type", "")).lower()
                )
                and (prop.get("text_value") is not None or prop.get("value") is not None)
            ]

            if data_validity_items:
                # Prioritize entries with result_flag == 1 (measured results).
                measured_items = [p for p in data_validity_items if _is_measured(p)]
                if measured_items:
                    # Use the first measured entry; prefer text_value when non-empty, otherwise value.
                    prop = measured_items[0]
                    text_value = prop.get("text_value")
                    value = prop.get("value")
                    comment_value = None
                    if text_value is not None and not _is_empty(text_value):
                        comment_value = str(text_value).strip()
                    elif value is not None and not _is_empty(value):
                        comment_value = str(value).strip()
                    if comment_value:
                        record["data_validity_comment"] = comment_value
                        log.debug(LogEvents.DATA_VALIDITY_COMMENT_FALLBACK_APPLIED,
                            activity_id=record.get("activity_id"),
                            source="activity_properties",
                            priority="measured",
                            comment_value=comment_value,
                        )
                else:
                    # Use the first available entry; prefer text_value when non-empty, otherwise value.
                    prop = data_validity_items[0]
                    text_value = prop.get("text_value")
                    value = prop.get("value")
                    comment_value = None
                    if text_value is not None and not _is_empty(text_value):
                        comment_value = str(text_value).strip()
                    elif value is not None and not _is_empty(value):
                        comment_value = str(value).strip()
                    if comment_value:
                        record["data_validity_comment"] = comment_value
                        log.debug(LogEvents.DATA_VALIDITY_COMMENT_FALLBACK_APPLIED,
                            activity_id=record.get("activity_id"),
                            source="activity_properties",
                            priority="first",
                            comment_value=comment_value,
                        )
            else:
                # Log when activity_properties exist but data_validity_comment cannot be derived.
                log.debug(LogEvents.DATA_VALIDITY_COMMENT_FALLBACK_NO_ITEMS,
                    activity_id=record.get("activity_id"),
                    activity_properties_count=len(items),
                    has_activity_properties=True,
                )
        else:
            # Log when data_validity_comment is already supplied by the API.
            log.debug(LogEvents.DATA_VALIDITY_COMMENT_FROM_API,
                activity_id=record.get("activity_id"),
                comment_value=current_comment,
            )

        # Normalize and preserve activity_properties for serialization.
        # Retain all properties (only perform TRUV validation).
        normalized_properties = self._normalize_activity_properties_items(property_items, log)
        if normalized_properties is not None:
            # Validate TRUV invariants and log invalid properties.
            validated_properties, validation_stats = self._validate_activity_properties_truv(
                normalized_properties, log, activity_id
            )
            # Deduplicate exact duplicates.
            deduplicated_properties, dedup_stats = self._deduplicate_activity_properties(
                validated_properties, log, activity_id
            )
            # Store normalized, deduplicated properties.
            record["activity_properties"] = deduplicated_properties
            # Log processing statistics.
            log.debug(LogEvents.ACTIVITY_PROPERTIES_PROCESSED,
                activity_id=activity_id,
                original_count=len(property_items),
                normalized_count=len(normalized_properties),
                validated_count=len(validated_properties),
                deduplicated_count=len(deduplicated_properties),
                invalid_count=validation_stats.get("invalid_count", 0),
                duplicates_removed=dedup_stats.get("duplicates_removed", 0),
            )
        else:
            # If normalization fails, keep the original properties.
            record["activity_properties"] = properties
            log.debug(LogEvents.ACTIVITY_PROPERTIES_NORMALIZATION_FAILED,
                activity_id=activity_id,
                message="activity_properties normalization failed, keeping original",
            )

        return record

    @staticmethod
    def _coerce_mapping(payload: Any) -> dict[str, Any]:
        if isinstance(payload, Mapping):
            return cast(dict[str, Any], payload)
        return {}

    @staticmethod
    def _extract_chembl_release(payload: Mapping[str, Any]) -> str | None:
        for key in ("chembl_release", "chembl_db_version", "release", "version"):
            value = payload.get(key)
            if isinstance(value, str) and value.strip():
                return value
            if value is not None:
                return str(value)
        return None

    @staticmethod
    def _extract_page_items(
        payload: Mapping[str, Any],
        items_keys: Sequence[str] | None = None,
    ) -> list[dict[str, Any]]:
        preferred_keys: tuple[str, ...] = ("activities",)
        if items_keys is None:
            combined_keys = preferred_keys + ("data", "items", "results")
        else:
            combined_keys = tuple(dict.fromkeys((*preferred_keys, *items_keys)))
        return ChemblPipelineBase._extract_page_items(payload, combined_keys)

    @staticmethod
    def _next_link(payload: Mapping[str, Any], base_url: str) -> str | None:
        page_meta: Any = payload.get("page_meta")
        if isinstance(page_meta, Mapping):
            next_link_raw: Any = page_meta.get("next")  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
            next_link: str | None = (
                cast(str | None, next_link_raw) if next_link_raw is not None else None
            )
            if isinstance(next_link, str) and next_link:
                # urlparse returns ParseResult with str path when input is str
                base_url_str = str(base_url)
                base_path_parse_result = urlparse(base_url_str)
                base_path_raw = base_path_parse_result.path
                base_path_str = (
                    base_path_raw.decode("utf-8", "ignore")
                    if isinstance(base_path_raw, (bytes, bytearray))
                    else base_path_raw
                )
                base_path: str = base_path_str.rstrip("/")

                # If next_link is a full URL, extract only the relative path
                if next_link.startswith("http://") or next_link.startswith("https://"):
                    parsed = urlparse(next_link)
                    base_parsed = urlparse(base_url_str)

                    # Get paths - urlparse returns str path when input is str
                    parsed_path_raw = parsed.path
                    base_path_raw = base_parsed.path
                    path: str = (
                        parsed_path_raw.decode("utf-8", "ignore")
                        if isinstance(parsed_path_raw, (bytes, bytearray))
                        else parsed_path_raw
                    )
                    base_path_from_url: str = (
                        base_path_raw.decode("utf-8", "ignore")
                        if isinstance(base_path_raw, (bytes, bytearray))
                        else base_path_raw
                    )

                    # Normalize: remove trailing slashes for comparison
                    path_normalized: str = path.rstrip("/")
                    base_path_normalized: str = base_path_from_url.rstrip("/")

                    # Remove base_path prefix from path if it exists
                    # This ensures we only return the endpoint part relative to base_url
                    if base_path_normalized and path_normalized.startswith(base_path_normalized):
                        # Extract the part after base_path
                        relative_path = path_normalized[len(base_path_normalized) :]
                        # If empty, it means paths are identical - this shouldn't happen for pagination
                        if not relative_path:
                            return None
                        # Ensure relative_path starts with /
                        if not relative_path.startswith("/"):
                            relative_path = f"/{relative_path}"
                    else:
                        # If base_path doesn't match, try to extract using /api/data/ pattern
                        # ChEMBL API URLs typically follow: .../chembl/api/data/<endpoint>
                        if "/api/data/" in path:
                            # Extract everything after /api/data/
                            parts = path.split("/api/data/", 1)
                            if len(parts) > 1:
                                relative_path = "/" + parts[1]
                            else:
                                # Shouldn't happen, but fallback
                                relative_path = path
                        else:
                            # Fallback: use the path as-is (shouldn't normally happen)
                            relative_path = path
                            # Ensure it starts with /
                            if not relative_path.startswith("/"):
                                relative_path = f"/{relative_path}"

                    # Add query string if present (preserve original query params)
                    if parsed.query:
                        relative_path = f"{relative_path}?{parsed.query}"

                    return relative_path

                # Handle relative URLs that redundantly include the base path
                if base_path:
                    normalized_base = base_path.lstrip("/")
                    stripped_link = next_link.lstrip("/")

                    if stripped_link.startswith(normalized_base + "/"):
                        stripped_link = stripped_link[len(normalized_base) :]
                    elif stripped_link == normalized_base:
                        stripped_link = ""

                    next_link = stripped_link

                next_link = next_link.lstrip("/")
                if next_link:
                    next_link = f"/{next_link}"
                else:
                    next_link = "/"

                return next_link
        return None

    # ------------------------------------------------------------------
    # Transformation helpers
    # ------------------------------------------------------------------

    def _harmonize_identifier_columns(self, df: pd.DataFrame, log: BoundLogger) -> pd.DataFrame:
        """Ensure canonical identifier columns are present before normalization."""

        df = df.copy()
        actions: list[str] = []

        if "assay_chembl_id" not in df.columns and "assay_id" in df.columns:
            df["assay_chembl_id"] = df["assay_id"]
            actions.append("assay_id->assay_chembl_id")

        if "testitem_chembl_id" not in df.columns:
            if "testitem_id" in df.columns:
                df["testitem_chembl_id"] = df["testitem_id"]
                actions.append("testitem_id->testitem_chembl_id")
            elif "molecule_chembl_id" in df.columns:
                df["testitem_chembl_id"] = df["molecule_chembl_id"]
                actions.append("molecule_chembl_id->testitem_chembl_id")

        if "molecule_chembl_id" not in df.columns and "testitem_chembl_id" in df.columns:
            df["molecule_chembl_id"] = df["testitem_chembl_id"]
            actions.append("testitem_chembl_id->molecule_chembl_id")

        required_columns = [
            "activity_id",
            "assay_chembl_id",
            "testitem_chembl_id",
            "molecule_chembl_id",
        ]
        missing_required = [column for column in required_columns if column not in df.columns]
        if missing_required:
            for column in missing_required:
                df[column] = pd.Series([None] * len(df), dtype="object")
            actions.append(f"created_missing:{','.join(missing_required)}")

        alias_columns = [column for column in ("assay_id", "testitem_id") if column in df.columns]
        if alias_columns:
            df = df.drop(columns=alias_columns)
            actions.append(f"dropped_aliases:{','.join(alias_columns)}")

        if actions:
            log.debug(LogEvents.IDENTIFIER_HARMONIZATION, actions=actions)

        return df

    def _schema_column_specs(self) -> Mapping[str, Mapping[str, Any]]:
        specs = dict(super()._schema_column_specs())
        boolean_columns = ("potential_duplicate", "curated", "removed")
        for column in boolean_columns:
            specs[column] = {"dtype": "boolean", "default": pd.NA}
        return specs

    def _normalize_identifiers(self, df: pd.DataFrame, log: BoundLogger) -> pd.DataFrame:
        """Normalize ChEMBL and BAO identifiers with regex validation."""

        rules = [
            IdentifierRule(
                name="chembl",
                columns=[
                    "assay_chembl_id",
                    "testitem_chembl_id",
                    "molecule_chembl_id",
                    "target_chembl_id",
                    "document_chembl_id",
                ],
                pattern=r"^CHEMBL\d+$",
            ),
            IdentifierRule(
                name="bao",
                columns=["bao_endpoint", "bao_format"],
                pattern=r"^BAO_\d{7}$",
            ),
        ]

        normalized_df, stats = normalize_identifier_columns(df, rules)

        if stats.has_changes:
            log.debug(LogEvents.IDENTIFIERS_NORMALIZED,
                normalized_count=stats.normalized,
                invalid_count=stats.invalid,
                columns=list(stats.per_column.keys()),
            )

        return normalized_df

    def _finalize_identifier_columns(self, df: pd.DataFrame, log: BoundLogger) -> pd.DataFrame:
        """Align identifier columns after normalization and drop aliases."""

        df = df.copy()

        if {"molecule_chembl_id", "testitem_chembl_id"}.issubset(df.columns):
            mismatch_mask = (
                df["molecule_chembl_id"].notna()
                & df["testitem_chembl_id"].notna()
                & (df["molecule_chembl_id"] != df["testitem_chembl_id"])
            )
            if mismatch_mask.any():
                mismatch_count = int(mismatch_mask.sum())
                samples_raw = (
                    df.loc[mismatch_mask, ["molecule_chembl_id", "testitem_chembl_id"]]
                    .drop_duplicates()
                    .head(5)
                    .to_dict("records")  # pyright: ignore[reportUnknownMemberType]
                )
                samples: list[dict[str, Any]] = cast(list[dict[str, Any]], samples_raw)
                log.warning(LogEvents.IDENTIFIER_MISMATCH,
                    count=mismatch_count,
                    samples=samples,
                )
                df.loc[mismatch_mask, "testitem_chembl_id"] = df.loc[
                    mismatch_mask, "molecule_chembl_id"
                ]

        required_columns = [
            "activity_id",
            "assay_chembl_id",
            "testitem_chembl_id",
            "molecule_chembl_id",
        ]
        missing_columns = [column for column in required_columns if column not in df.columns]
        if missing_columns:
            for column in missing_columns:
                if column == "testitem_chembl_id" and "molecule_chembl_id" in df.columns:
                    # testitem_chembl_id should be equal to molecule_chembl_id if missing
                    df[column] = df["molecule_chembl_id"].copy()
                else:
                    df[column] = pd.Series([None] * len(df), dtype="object")
            log.warning(LogEvents.IDENTIFIER_COLUMNS_MISSING, columns=missing_columns)

        return df

    def _finalize_output_columns(self, df: pd.DataFrame, log: BoundLogger) -> pd.DataFrame:
        """Align final column order with schema and drop unexpected fields."""

        df = df.copy()
        expected = list(COLUMN_ORDER)

        extras = [column for column in df.columns if column not in expected]
        if extras:
            df = df.drop(columns=extras)
            log.debug(LogEvents.OUTPUT_COLUMNS_DROPPED, columns=extras)

        missing = [column for column in expected if column not in df.columns]
        if missing:
            for column in missing:
                if column == "testitem_chembl_id" and "molecule_chembl_id" in df.columns:
                    # testitem_chembl_id should be equal to molecule_chembl_id if missing
                    df[column] = df["molecule_chembl_id"].copy()
                else:
                    df[column] = pd.Series([pd.NA] * len(df), dtype="object")
            log.warning(LogEvents.OUTPUT_COLUMNS_MISSING, columns=missing)

        if not expected:
            return df

        return df[expected]

    def _filter_invalid_required_fields(self, df: pd.DataFrame, log: BoundLogger) -> pd.DataFrame:
        """Filter out rows with NULL values in required identifier fields.

        Removes rows where any of the required fields (assay_chembl_id,
        testitem_chembl_id, molecule_chembl_id) are NULL, as these cannot
        pass schema validation.

        Parameters
        ----------
        df:
            DataFrame to filter.
        log:
            Logger instance.

        Returns
        -------
        pd.DataFrame:
            Filtered DataFrame with only rows having all required fields populated.
        """
        df = df.copy()

        if df.empty:
            return df

        required_fields = ["assay_chembl_id", "molecule_chembl_id"]
        missing_fields = [field for field in required_fields if field not in df.columns]
        if missing_fields:
            log.warning(LogEvents.FILTER_SKIPPED_MISSING_COLUMNS,
                missing_columns=missing_fields,
                message="Cannot filter: required columns are missing",
            )
            return df

        # Create mask for rows with all required fields populated
        valid_mask = df["assay_chembl_id"].notna() & df["molecule_chembl_id"].notna()

        invalid_count = int((~valid_mask).sum())
        if invalid_count > 0:
            # Log sample of invalid rows for debugging
            invalid_rows = df[~valid_mask]
            sample_size = min(5, len(invalid_rows))
            sample_activity_ids = (
                invalid_rows["activity_id"].head(sample_size).tolist()
                if "activity_id" in invalid_rows.columns
                else []
            )
            log.warning(LogEvents.FILTERED_INVALID_ROWS,
                filtered_count=invalid_count,
                remaining_count=int(valid_mask.sum()),
                sample_activity_ids=sample_activity_ids,
                message="Rows with NULL in required identifier fields were filtered out",
            )
            df = df[valid_mask].reset_index(drop=True)

        return df

    def _normalize_measurements(self, df: pd.DataFrame, log: BoundLogger) -> pd.DataFrame:
        """Normalize standard_value, standard_units, standard_relation, and standard_type."""

        df = df.copy()
        normalized_count = 0

        if "standard_value" in df.columns:
            mask = df["standard_value"].notna()
            if mask.any():
                # Remove non-numeric characters and handle ranges
                # Convert to string first to handle non-numeric values
                series = df.loc[mask, "standard_value"].astype(str).str.strip()

                # Remove common non-numeric characters (spaces, commas, etc.)
                series = series.str.replace(r"[,\s]", "", regex=True)

                # Handle ranges (e.g., "10-20" -> take first value, "10±5" -> take first value)
                # Extract first numeric value from ranges
                series = series.str.extract(r"([+-]?\d*\.?\d+)", expand=False)

                # Convert to numeric (NaN for empty/invalid values)
                numeric_series_std: pd.Series[Any] = pd.to_numeric(  # pyright: ignore[reportUnknownMemberType]
                    series, errors="coerce"
                )
                df.loc[mask, "standard_value"] = numeric_series_std

                # Check for negative values (should be >= 0)
                negative_mask = mask & (df["standard_value"] < 0)
                if negative_mask.any():
                    log.warning(LogEvents.NEGATIVE_STANDARD_VALUE, count=int(negative_mask.sum()))
                    df.loc[negative_mask, "standard_value"] = None

                normalized_count += int(mask.sum())

        if "standard_relation" in df.columns:
            unicode_to_ascii = {
                "≤": "<=",
                "≥": ">=",
                "≠": "~",
            }
            mask = df["standard_relation"].notna()
            if mask.any():
                series = df.loc[mask, "standard_relation"].astype(str).str.strip()
                for unicode_char, ascii_repl in unicode_to_ascii.items():
                    series = series.str.replace(unicode_char, ascii_repl, regex=False)
                df.loc[mask, "standard_relation"] = series
                invalid_mask = mask & ~df["standard_relation"].isin(RELATIONS)  # pyright: ignore[reportUnknownMemberType]
                if invalid_mask.any():
                    log.warning(LogEvents.INVALID_STANDARD_RELATION, count=int(invalid_mask.sum()))
                    df.loc[invalid_mask, "standard_relation"] = None
                normalized_count += int(mask.sum())

        if "standard_type" in df.columns:
            mask = df["standard_type"].notna()
            if mask.any():
                df.loc[mask, "standard_type"] = (
                    df.loc[mask, "standard_type"].astype(str).str.strip()
                )
                standard_types_set = set(self._get_standard_types())
                invalid_mask = mask & ~df["standard_type"].isin(standard_types_set)  # pyright: ignore[reportUnknownMemberType]
                if invalid_mask.any():
                    log.warning(LogEvents.INVALID_STANDARD_TYPE, count=int(invalid_mask.sum()))
                    df.loc[invalid_mask, "standard_type"] = None
                normalized_count += int(mask.sum())

        if "standard_units" in df.columns:
            # Normalize unit synonyms: nM, µM/μM, mM, %, ratio
            # Map various representations to canonical forms while preserving unit types
            unit_mapping = {
                # nanomolar variants -> nM
                "nanomolar": "nM",
                "nmol": "nM",
                "nm": "nM",
                "NM": "nM",
                # micromolar variants -> μM
                "µM": "μM",
                "uM": "μM",
                "UM": "μM",
                "micromolar": "μM",
                "microM": "μM",
                "umol": "μM",
                # millimolar variants -> mM
                "millimolar": "mM",
                "milliM": "mM",
                "mmol": "mM",
                "MM": "mM",
                # percentage variants -> %
                "percent": "%",
                "pct": "%",
                # ratio variants -> ratio
                "ratios": "ratio",
            }
            mask = df["standard_units"].notna()
            if mask.any():
                series = df.loc[mask, "standard_units"].astype(str).str.strip()
                for old_unit, new_unit in unit_mapping.items():
                    series = series.str.replace(old_unit, new_unit, regex=False, case=False)
                df.loc[mask, "standard_units"] = series
                normalized_count += int(mask.sum())

        if "relation" in df.columns:
            unicode_to_ascii = {
                "≤": "<=",
                "≥": ">=",
                "≠": "~",
            }
            mask = df["relation"].notna()
            if mask.any():
                series = df.loc[mask, "relation"].astype(str).str.strip()
                for unicode_char, ascii_repl in unicode_to_ascii.items():
                    series = series.str.replace(unicode_char, ascii_repl, regex=False)
                df.loc[mask, "relation"] = series
                invalid_mask = mask & ~df["relation"].isin(RELATIONS)  # pyright: ignore[reportUnknownMemberType]
                if invalid_mask.any():
                    log.warning(LogEvents.INVALID_RELATION, count=int(invalid_mask.sum()))
                    df.loc[invalid_mask, "relation"] = None
                normalized_count += int(mask.sum())

        if "standard_upper_value" in df.columns:
            mask = df["standard_upper_value"].notna()
            if mask.any():
                series = df.loc[mask, "standard_upper_value"].astype(str).str.strip()
                series = series.str.replace(r"[,\s]", "", regex=True)
                series = series.str.extract(r"([+-]?\d*\.?\d+)", expand=False)
                numeric_series_std_upper: pd.Series[Any] = pd.to_numeric(  # pyright: ignore[reportUnknownMemberType]
                    series, errors="coerce"
                )
                df.loc[mask, "standard_upper_value"] = numeric_series_std_upper
                negative_mask = mask & (df["standard_upper_value"] < 0)
                if negative_mask.any():
                    log.warning(LogEvents.NEGATIVE_STANDARD_UPPER_VALUE, count=int(negative_mask.sum()))
                    df.loc[negative_mask, "standard_upper_value"] = None
                normalized_count += int(mask.sum())

        if "upper_value" in df.columns:
            mask = df["upper_value"].notna()
            if mask.any():
                series = df.loc[mask, "upper_value"].astype(str).str.strip()
                series = series.str.replace(r"[,\s]", "", regex=True)
                series = series.str.extract(r"([+-]?\d*\.?\d+)", expand=False)
                numeric_series_upper: pd.Series[Any] = pd.to_numeric(  # pyright: ignore[reportUnknownMemberType]
                    series, errors="coerce"
                )
                df.loc[mask, "upper_value"] = numeric_series_upper
                negative_mask = mask & (df["upper_value"] < 0)
                if negative_mask.any():
                    log.warning(LogEvents.NEGATIVE_UPPER_VALUE, count=int(negative_mask.sum()))
                    df.loc[negative_mask, "upper_value"] = None
                normalized_count += int(mask.sum())

        if "lower_value" in df.columns:
            mask = df["lower_value"].notna()
            if mask.any():
                series = df.loc[mask, "lower_value"].astype(str).str.strip()
                series = series.str.replace(r"[,\s]", "", regex=True)
                series = series.str.extract(r"([+-]?\d*\.?\d+)", expand=False)
                numeric_series_lower: pd.Series[Any] = pd.to_numeric(  # pyright: ignore[reportUnknownMemberType]
                    series, errors="coerce"
                )
                df.loc[mask, "lower_value"] = numeric_series_lower
                negative_mask = mask & (df["lower_value"] < 0)
                if negative_mask.any():
                    log.warning(LogEvents.NEGATIVE_LOWER_VALUE, count=int(negative_mask.sum()))
                    df.loc[negative_mask, "lower_value"] = None
                normalized_count += int(mask.sum())

        if normalized_count > 0:
            log.debug(LogEvents.MEASUREMENTS_NORMALIZED, normalized_count=normalized_count)

        return df

    def _normalize_string_fields(self, df: pd.DataFrame, log: BoundLogger) -> pd.DataFrame:
        """Normalize string fields: trim, empty string to null, title-case for organism."""

        working_df = df.copy()

        # Invariant check: data_validity_description populated when data_validity_comment is NA.
        if (
            "data_validity_description" in working_df.columns
            and "data_validity_comment" in working_df.columns
        ):
            invalid_mask = (
                working_df["data_validity_description"].notna()
                & working_df["data_validity_comment"].isna()
            )
            if invalid_mask.any():
                invalid_count = int(invalid_mask.sum())
                log.warning(
                    "invariant_data_validity_description_without_comment",
                    count=invalid_count,
                    message="data_validity_description is filled while data_validity_comment is NA",
                )

        rules: dict[str, StringRule] = {
            "canonical_smiles": StringRule(),
            "bao_label": StringRule(max_length=128),
            "target_organism": StringRule(title_case=True),
            "assay_organism": StringRule(title_case=True),
            "data_validity_comment": StringRule(),
            "data_validity_description": StringRule(),
            "activity_comment": StringRule(),
            "standard_text_value": StringRule(),
            "text_value": StringRule(),
            "type": StringRule(),
            "units": StringRule(),
            "assay_type": StringRule(),
            "assay_description": StringRule(),
            "molecule_pref_name": StringRule(),
            "target_pref_name": StringRule(),
            "uo_units": StringRule(),
            "qudt_units": StringRule(),
        }

        normalized_df, stats = normalize_string_columns(working_df, rules, copy=False)

        if stats.has_changes:
            log.debug(LogEvents.STRING_FIELDS_NORMALIZED,
                columns=list(stats.per_column.keys()),
                rows_processed=stats.processed,
            )

        return normalized_df

    def _normalize_nested_structures(self, df: pd.DataFrame, log: BoundLogger) -> pd.DataFrame:
        """Serialize nested structures (ligand_efficiency, activity_properties) to JSON strings."""

        df = df.copy()

        nested_fields = ["ligand_efficiency", "activity_properties"]

        for field in nested_fields:
            if field not in df.columns:
                continue
            mask = df[field].notna()
            if mask.any():
                serialized: list[Any] = []
                for idx, value in df.loc[mask, field].items():
                    if field == "activity_properties":
                        serialized_value = self._serialize_activity_properties(value, log)
                        serialized.append(serialized_value)
                        continue

                    if isinstance(value, (Mapping, list)):
                        try:
                            serialized.append(json.dumps(value, ensure_ascii=False, sort_keys=True))
                        except (TypeError, ValueError) as exc:
                            log.warning(LogEvents.NESTED_SERIALIZATION_FAILED,
                                field=field,
                                index=idx,
                                error=str(exc),
                            )
                            serialized.append(None)
                    elif isinstance(value, str):
                        try:
                            json.loads(value)
                            serialized.append(value)
                        except (TypeError, ValueError):
                            serialized.append(None)
                    else:
                        serialized.append(None)
                df.loc[mask, field] = pd.Series(
                    serialized, dtype="object", index=df.loc[mask, field].index
                )

        # Diagnostic logging for ligand_efficiency.
        if "standard_value" in df.columns and "ligand_efficiency" in df.columns:
            mask = df["standard_value"].notna() & df["ligand_efficiency"].isna()
            if mask.any():
                log.warning(LogEvents.LIGAND_EFFICIENCY_MISSING_WITH_STANDARD_VALUE,
                    count=int(mask.sum()),
                    message="ligand_efficiency is empty while standard_value exists",
                )

        return df

    def _serialize_activity_properties(
        self, value: Any, log: BoundLogger | None = None
    ) -> str | None:
        """Return normalized JSON for activity_properties or None if not serializable."""

        normalized_items = self._normalize_activity_properties_items(value, log)
        if normalized_items is None:
            return None
        try:
            return json.dumps(normalized_items, ensure_ascii=False, sort_keys=True)
        except (TypeError, ValueError) as exc:
            if log is not None:
                log.warning(LogEvents.ACTIVITY_PROPERTIES_SERIALIZATION_FAILED, error=str(exc))
            return None

    def _normalize_activity_properties_items(
        self, value: Any, log: BoundLogger | None = None
    ) -> list[dict[str, Any]] | None:
        """Coerce activity_properties payloads into a list of constrained dictionaries."""

        if value is None:
            return None

        raw_value = value

        if isinstance(value, str):
            stripped = value.strip()
            if not stripped:
                return []
            try:
                parsed = json.loads(stripped)
            except (TypeError, ValueError):
                fallback_base: dict[str, Any | None] = dict.fromkeys(ACTIVITY_PROPERTY_KEYS, None)
                fallback_base["text_value"] = stripped
                return [fallback_base]
            else:
                value = parsed

        if isinstance(value, Mapping):
            items: list[Any] = [value]
        elif isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
            items = list(value)  # pyright: ignore[reportUnknownArgumentType]
        else:
            if log is not None:
                log.warning(LogEvents.ACTIVITY_PROPERTIES_UNHANDLED_TYPE.legacy(),
                    value_type=type(raw_value).__name__,
                )
            return None

        normalized: list[dict[str, Any]] = []
        for item in items:
            if item is None:
                continue
            if isinstance(item, Mapping):
                # Convert Mapping to dict for explicit typing.
                item_mapping = cast(Mapping[str, Any], item)
                normalized_item: dict[str, Any | None] = {
                    key: item_mapping.get(key) for key in ACTIVITY_PROPERTY_KEYS
                }
                result_flag_value = normalized_item.get("result_flag")
                if isinstance(result_flag_value, int) and result_flag_value in (0, 1):
                    normalized_item["result_flag"] = bool(result_flag_value)
                normalized.append(normalized_item)
            elif isinstance(item, str):
                str_base: dict[str, Any | None] = dict.fromkeys(ACTIVITY_PROPERTY_KEYS, None)
                str_base["text_value"] = item
                normalized.append(str_base)
            else:
                if log is not None:
                    log.warning(LogEvents.ACTIVITY_PROPERTIES_ITEM_UNHANDLED.legacy(),
                        item_type=type(item).__name__,
                    )

        return normalized

    def _validate_activity_properties_truv(
        self,
        properties: list[dict[str, Any]],
        log: BoundLogger,
        activity_id: Any | None = None,
    ) -> tuple[list[dict[str, Any]], dict[str, int]]:
        """Validate TRUV format for activity_properties.

        Ensures invariants:
        - value IS NOT NULL ⇒ text_value IS NULL (and vice versa)
        - relation ∈ ('=', '<', '≤', '>', '≥', '~') or NULL

        Parameters
        ----------
        properties:
            Normalized properties to validate.
        log:
            Logger instance.
        activity_id:
            Optional activity identifier for logging.

        Returns
        -------
        tuple[list[dict[str, Any]], dict[str, int]]:
            Validated properties and validation statistics.
        """
        validated: list[dict[str, Any]] = []
        invalid_count = 0
        invalid_items: list[dict[str, Any]] = []

        for prop in properties:
            is_valid = True
            validation_errors: list[str] = []

            value = prop.get("value")
            text_value = prop.get("text_value")
            relation = prop.get("relation")

            # Validate invariant: value IS NOT NULL ⇒ text_value IS NULL (and vice versa).
            if value is not None and text_value is not None:
                is_valid = False
                validation_errors.append("both value and text_value are not None")
            elif value is None and text_value is None:
                # Both can be None; this is acceptable.
                pass

            # Validate relation: must be allowed or NULL.
            if relation is not None:
                if not isinstance(relation, str):
                    is_valid = False
                    validation_errors.append(f"relation is not a string: {type(relation).__name__}")
                elif relation not in RELATIONS:
                    is_valid = False
                    validation_errors.append(
                        f"relation '{relation}' not in allowed values: {RELATIONS}"
                    )

            # Retain the property; only log invalid ones.
            validated.append(prop)
            if not is_valid:
                invalid_count += 1
                invalid_items.append(prop)
                log.warning(LogEvents.ACTIVITY_PROPERTY_TRUV_VALIDATION_FAILED,
                    activity_id=activity_id,
                    property=prop,
                    errors=validation_errors,
                    message="TRUV validation failed, but property is kept",
                )

        stats = {
            "invalid_count": invalid_count,
            "valid_count": len(validated),
        }

        return validated, stats

    def _deduplicate_activity_properties(
        self,
        properties: list[dict[str, Any]],
        log: BoundLogger,
        activity_id: Any | None = None,
    ) -> tuple[list[dict[str, Any]], dict[str, int]]:
        """Deduplicate exact duplicates within activity_properties.

        Removes duplicates across all fields (type, relation, units, value, text_value) while
        preserving order (first occurrence wins).

        Parameters
        ----------
        properties:
            Validated properties to deduplicate.
        log:
            Logger instance.
        activity_id:
            Optional activity identifier for logging.

        Returns
        -------
        tuple[list[dict[str, Any]], dict[str, int]]:
            Deduplicated properties and deduplication statistics.
        """
        seen: set[tuple[Any, ...]] = set()
        deduplicated: list[dict[str, Any]] = []
        duplicates_removed = 0

        for prop in properties:
            # Build a deduplication key from all property fields.
            dedup_key = (
                prop.get("type"),
                prop.get("relation"),
                prop.get("units"),
                prop.get("value"),
                prop.get("text_value"),
            )

            if dedup_key not in seen:
                seen.add(dedup_key)
                deduplicated.append(prop)
            else:
                duplicates_removed += 1
                log.debug(LogEvents.ACTIVITY_PROPERTY_DUPLICATE_REMOVED,
                    activity_id=activity_id,
                    property=prop,
                    message="Exact duplicate removed",
                )

        stats = {
            "duplicates_removed": duplicates_removed,
            "deduplicated_count": len(deduplicated),
        }

        return deduplicated, stats

    def _add_row_metadata(self, df: pd.DataFrame, log: BoundLogger) -> pd.DataFrame:
        """Add required row metadata fields (row_subtype, row_index)."""

        df = df.copy()
        if df.empty:
            return df

        # Add row_subtype: "activity" for all rows
        if "row_subtype" not in df.columns:
            df["row_subtype"] = "activity"
            log.debug(LogEvents.ROW_SUBTYPE_ADDED, value="activity")
        elif df["row_subtype"].isna().all():
            df["row_subtype"] = "activity"
            log.debug(LogEvents.ROW_SUBTYPE_FILLED, value="activity")

        # Add row_index: sequential index starting from 0
        if "row_index" not in df.columns:
            df["row_index"] = range(len(df))
            log.debug(LogEvents.ROW_INDEX_ADDED, count=len(df))
        elif df["row_index"].isna().all():
            df["row_index"] = range(len(df))
            log.debug(LogEvents.ROW_INDEX_FILLED, count=len(df))

        return df

    def _normalize_data_types(
        self, df: pd.DataFrame, schema: Any, log: BoundLogger
    ) -> pd.DataFrame:
        """Convert data types according to the Pandera schema."""

        df = df.copy()

        # Non-nullable integer fields
        non_nullable_int_fields = {
            "activity_id": "int64",
        }

        # Nullable integer fields
        nullable_int_fields = {
            "row_index": "Int64",
            "target_tax_id": "int64",
            "assay_tax_id": "int64",
            "record_id": "int64",
            "src_id": "int64",
        }

        # Float fields
        float_fields = {
            "standard_value": "float64",
            "standard_upper_value": "float64",
            "pchembl_value": "float64",
            "upper_value": "float64",
            "lower_value": "float64",
        }

        bool_fields = [
            "potential_duplicate",
            "curated",
            "removed",
        ]

        binary_flag_fields = [
            "standard_flag",
        ]

        # Handle non-nullable integers
        for field in non_nullable_int_fields:
            if field not in df.columns:
                continue
            try:
                numeric_series_int: pd.Series[Any] = pd.to_numeric(  # pyright: ignore[reportUnknownMemberType]
                    df[field], errors="coerce"
                )
                df[field] = numeric_series_int.astype("Int64")
            except (ValueError, TypeError) as exc:
                log.warning(LogEvents.TYPE_CONVERSION_FAILED, field=field, error=str(exc))

        # Handle nullable integers - preserve NA values
        for field in nullable_int_fields:
            if field not in df.columns:
                continue
            try:
                if field == "row_index":
                    # row_index should be non-nullable, but use Int64 for consistency
                    numeric_series_row: pd.Series[Any] = pd.to_numeric(df[field], errors="coerce")  # pyright: ignore[reportUnknownMemberType]
                    df[field] = numeric_series_row.astype("Int64")
                    # Fill any NA values with sequential index
                    if df[field].isna().any():
                        df[field] = range(len(df))
                else:
                    # Convert to numeric, preserving NA values
                    nullable_numeric_series: pd.Series[Any] = pd.to_numeric(  # pyright: ignore[reportUnknownMemberType]
                        df[field], errors="coerce"
                    )
                    # Use Int64 (nullable integer) to preserve NA values
                    df[field] = nullable_numeric_series.astype("Int64")
                    # For nullable fields, ensure values >= 1 if not NA
                    mask_valid = df[field].notna()
                    if mask_valid.any():
                        invalid_mask = mask_valid & (df[field] < 1)
                        if invalid_mask.any():
                            log.warning(LogEvents.INVALID_POSITIVE_INTEGER,
                                field=field,
                                count=int(invalid_mask.sum()),
                            )
                            df.loc[invalid_mask, field] = pd.NA
            except (ValueError, TypeError) as exc:
                log.warning(LogEvents.TYPE_CONVERSION_FAILED, field=field, error=str(exc))

        # Handle float fields
        for field in float_fields:
            if field not in df.columns:
                continue
            try:
                numeric_series_float: pd.Series[Any] = pd.to_numeric(  # pyright: ignore[reportUnknownMemberType]
                    df[field], errors="coerce"
                )
                df[field] = numeric_series_float.astype("float64")
            except (ValueError, TypeError) as exc:
                log.warning(LogEvents.TYPE_CONVERSION_FAILED, field=field, error=str(exc))

        for field in bool_fields:
            if field not in df.columns:
                continue
            try:
                # For curated/removed: if already boolean dtype, leave unchanged.
                if field in ("curated", "removed") and df[field].dtype == "boolean":
                    continue
                # Convert to boolean while preserving NA values.
                if field in ("curated", "removed"):
                    # Convert directly to boolean dtype, preserving NA.
                    df[field] = df[field].astype("boolean")  # pyright: ignore[reportUnknownMemberType]
                else:
                    # For other boolean fields use standard logic.
                    bool_numeric_series: pd.Series[Any] = pd.to_numeric(  # pyright: ignore[reportUnknownMemberType]
                        df[field], errors="coerce"
                    )
                    df[field] = (bool_numeric_series != 0).astype("boolean")  # pyright: ignore[reportUnknownMemberType]
            except (ValueError, TypeError) as exc:
                log.warning(LogEvents.BOOL_CONVERSION_FAILED, field=field, error=str(exc))

        for field in binary_flag_fields:
            if field not in df.columns:
                continue
            try:
                numeric_series_flag: pd.Series[Any] = pd.to_numeric(df[field], errors="coerce")  # pyright: ignore[reportUnknownMemberType]
                df[field] = numeric_series_flag.astype("Int64")
                mask_valid = df[field].notna()
                if mask_valid.any():
                    valid_values = df.loc[mask_valid, field]
                    invalid_valid_mask = ~valid_values.isin([0, 1])  # pyright: ignore[reportUnknownMemberType]
                    if invalid_valid_mask.any():
                        invalid_index = valid_values.index[invalid_valid_mask]
                        log.warning(LogEvents.INVALID_STANDARD_FLAG,
                            field=field,
                            count=int(invalid_valid_mask.sum()),
                        )
                        df.loc[invalid_index, field] = pd.NA
            except (ValueError, TypeError) as exc:
                log.warning(LogEvents.TYPE_CONVERSION_FAILED, field=field, error=str(exc))

        # Handle object fields (value, activity_properties, etc.)
        # These fields should remain as object type to allow mixed types
        object_fields = ["value", "activity_properties"]
        for field in object_fields:
            if field in df.columns:
                # Ensure it's object type, but don't convert if already object
                if df[field].dtype != "object":
                    # Convert to object to preserve mixed types
                    df[field] = df[field].astype("object")

        return df

    def _validate_foreign_keys(self, df: pd.DataFrame, log: BoundLogger) -> pd.DataFrame:
        """Validate foreign key integrity and format of ChEMBL IDs."""

        chembl_id_pattern = re.compile(r"^CHEMBL\d+$")
        chembl_fields = [
            "assay_chembl_id",
            "testitem_chembl_id",
            "molecule_chembl_id",
            "target_chembl_id",
            "document_chembl_id",
            "parent_molecule_chembl_id",
        ]

        warnings: list[str] = []

        for field in chembl_fields:
            if field not in df.columns:
                continue
            mask = df[field].notna()
            if mask.any():
                invalid_mask = mask & ~df[field].astype(str).str.match(
                    chembl_id_pattern.pattern, na=False
                )
                if invalid_mask.any():
                    warning_msg: str = f"{field}: {int(invalid_mask.sum())} invalid format(s)"
                    warnings.append(warning_msg)

        if warnings:
            log.warning(LogEvents.FOREIGN_KEY_VALIDATION, warnings=warnings)

        return df

    def _check_activity_id_uniqueness(self, df: pd.DataFrame, log: BoundLogger) -> None:
        """Check uniqueness of activity_id before validation."""

        if "activity_id" not in df.columns:
            log.warning(LogEvents.ACTIVITY_ID_UNIQUENESS_CHECK_SKIPPED, reason="column_not_found")
            return

        duplicates = df[df["activity_id"].duplicated(keep=False)]
        if not duplicates.empty:
            duplicate_count = len(duplicates)
            duplicate_ids = duplicates["activity_id"].unique().tolist()
            log.error(LogEvents.ACTIVITY_ID_DUPLICATES_FOUND,
                duplicate_count=duplicate_count,
                duplicate_ids=duplicate_ids[:10],  # Log first 10 duplicates
                total_duplicate_ids=len(duplicate_ids),
            )
            msg = (
                f"Found {duplicate_count} duplicate activity_id value(s): "
                f"{duplicate_ids[:5]}{'...' if len(duplicate_ids) > 5 else ''}"
            )
            raise ValueError(msg)

        log.debug(LogEvents.ACTIVITY_ID_UNIQUENESS_VERIFIED, unique_count=df["activity_id"].nunique())

    def _validate_data_validity_comment_soft_enum(
        self, df: pd.DataFrame, log: BoundLogger
    ) -> None:
        """Soft-enum validation for data_validity_comment."""
        if df.empty or "data_validity_comment" not in df.columns:
            return

        whitelist = self._get_data_validity_comment_whitelist()
        if not whitelist:
            return

        series_candidate = df["data_validity_comment"].dropna()
        if len(series_candidate) == 0:
            return

        non_null_comments_series = series_candidate.astype("string")
        whitelist_set: set[str] = set(whitelist)

        def _is_unknown(value: str) -> bool:
            return value not in whitelist_set

        unknown_mask = non_null_comments_series.map(_is_unknown).astype(bool)
        unknown_count = int(unknown_mask.sum())

        if unknown_count > 0:
            unknown_values = {
                str(key): int(value)
                for key, value in non_null_comments_series[unknown_mask]
                .value_counts()
                .head(10)
                .items()
            }
            log.warning(LogEvents.SOFT_ENUM_UNKNOWN_DATA_VALIDITY_COMMENT.legacy(),
                unknown_count=unknown_count,
                total_count=len(non_null_comments_series),
                samples=unknown_values,
                whitelist=whitelist,
                message="Unknown data_validity_comment values detected (soft enum: not blocking)",
            )

    def _check_foreign_key_integrity(self, df: pd.DataFrame, log: BoundLogger) -> None:
        """Check foreign key integrity for ChEMBL IDs (format validation for non-null values)."""

        reference_fields = [
            "assay_chembl_id",
            "testitem_chembl_id",
            "molecule_chembl_id",
            "target_chembl_id",
            "document_chembl_id",
            "parent_molecule_chembl_id",
        ]
        chembl_id_pattern = re.compile(r"^CHEMBL\d+$")
        errors: list[str] = []

        for field in reference_fields:
            if field not in df.columns:
                log.debug(LogEvents.FOREIGN_KEY_INTEGRITY_CHECK_SKIPPED, field=field, reason="column_not_found"
                )
                continue

            mask = df[field].notna()
            if not mask.any():
                log.debug(LogEvents.FOREIGN_KEY_INTEGRITY_CHECK_SKIPPED, field=field, reason="all_null")
                continue

            invalid_mask = mask & ~df[field].astype(str).str.match(
                chembl_id_pattern.pattern, na=False
            )
            if invalid_mask.any():
                invalid_count = int(invalid_mask.sum())
                invalid_samples = df.loc[invalid_mask, field].unique().tolist()[:5]
                errors.append(
                    f"{field}: {invalid_count} invalid format(s), samples: {invalid_samples}"
                )
                log.warning(LogEvents.FOREIGN_KEY_INTEGRITY_INVALID,
                    field=field,
                    invalid_count=invalid_count,
                    samples=invalid_samples,
                )

        if errors:
            log.error(LogEvents.FOREIGN_KEY_INTEGRITY_CHECK_FAILED, errors=errors)
            msg = f"Foreign key integrity check failed: {'; '.join(errors)}"
            raise ValueError(msg)

        log.debug(LogEvents.FOREIGN_KEY_INTEGRITY_VERIFIED)

    def _log_detailed_validation_errors(
        self,
        failure_cases: pd.DataFrame,
        payload: pd.DataFrame,
        log: BoundLogger,
    ) -> None:
        """Log individual validation errors with row index and activity_id."""

        if failure_cases.empty or payload.empty:
            return

        # Get activity_id column if available
        activity_id_col = "activity_id" if "activity_id" in payload.columns else None
        index_col = "index" if "index" in failure_cases.columns else None

        if index_col is None:
            return

        # Limit to first 20 errors to avoid log spam
        max_errors = 20
        errors_to_log = failure_cases.head(max_errors)

        for _, error_row in errors_to_log.iterrows():
            row_index = error_row.get(index_col)
            if row_index is None:
                continue

            error_details: dict[str, Any] = {
                "row_index": int(row_index)
                if isinstance(row_index, (int, float))
                else str(row_index),
            }

            # Add activity_id if available
            if activity_id_col:
                try:
                    # Cast row_index to int for proper indexing
                    idx = int(row_index) if isinstance(row_index, (int, float)) else row_index
                    activity_id_value: Any = payload.at[cast(int, idx), activity_id_col]
                    activity_id = activity_id_value
                except (KeyError, IndexError):
                    activity_id = None
                if activity_id is not None and pd.notna(activity_id):
                    error_details["activity_id"] = (
                        int(activity_id)
                        if isinstance(activity_id, (int, float))
                        else str(activity_id)
                    )

            # Add column name if available
            if "column" in error_row and pd.notna(error_row["column"]):
                error_details["column"] = str(error_row["column"])

            # Add schema context if available
            if "schema_context" in error_row and pd.notna(error_row["schema_context"]):
                error_details["schema_context"] = str(error_row["schema_context"])

            # Add error message if available
            if "failure_case" in error_row and pd.notna(error_row["failure_case"]):
                error_details["failure_case"] = str(error_row["failure_case"])

            log.error(LogEvents.VALIDATION_ERROR_DETAIL, **error_details)

        if len(failure_cases) > max_errors:
            log.warning(LogEvents.VALIDATION_ERRORS_TRUNCATED,
                total_errors=len(failure_cases),
                logged_errors=max_errors,
            )

    def build_quality_report(self, df: pd.DataFrame) -> pd.DataFrame | dict[str, object] | None:
        """Return QC report with activity-specific metrics including distributions."""

        # Build base quality report with activity_id as business key for duplicate checking
        business_key = ["activity_id"] if "activity_id" in df.columns else None

        base_report = build_default_quality_report(df, business_key_fields=business_key)
        # build_default_quality_report always returns pd.DataFrame by contract

        rows: list[dict[str, Any]] = []
        if not base_report.empty:
            records_raw = base_report.to_dict("records")  # pyright: ignore[reportUnknownMemberType]
            records: list[dict[str, Any]] = cast(list[dict[str, Any]], records_raw)
            for record in records:
                rows.append({str(k): v for k, v in record.items()})

        # Add foreign key integrity metrics
        chembl_id_pattern = re.compile(r"^CHEMBL\d+$")
        foreign_key_fields = [
            "assay_chembl_id",
            "molecule_chembl_id",
            "target_chembl_id",
            "document_chembl_id",
        ]
        for field in foreign_key_fields:
            if field in df.columns:
                mask = df[field].notna()
                if mask.any():
                    string_series = df[field].astype(str)
                    valid_mask = mask & string_series.str.match(chembl_id_pattern.pattern, na=False)
                    invalid_count = int((mask & ~valid_mask).astype(int).sum())
                    valid_count = int(valid_mask.astype(int).sum())
                    total_count = int(mask.astype(int).sum())
                    integrity_ratio = float(valid_count / total_count) if total_count > 0 else 0.0
                    rows.append(
                        {
                            "section": "foreign_key",
                            "metric": "integrity_ratio",
                            "column": field,
                            "value": float(integrity_ratio),
                            "valid_count": int(valid_count),
                            "invalid_count": int(invalid_count),
                            "total_count": int(total_count),
                        }
                    )

        # Add measurement type distribution
        if "standard_type" in df.columns:
            type_counts_series: pd.Series[Any] = df["standard_type"].value_counts()
            type_dist_raw = type_counts_series.to_dict()  # pyright: ignore[reportUnknownMemberType]
            type_dist: dict[Any, int] = cast(dict[Any, int], type_dist_raw)
            for type_value, count in type_dist.items():
                rows.append(
                    {
                        "section": "distribution",
                        "metric": "standard_type_count",
                        "column": "standard_type",
                        "value": str(type_value) if type_value is not None else "null",
                        "count": int(count),
                    }
                )

        # Add unit distribution
        if "standard_units" in df.columns:
            unit_counts_series: pd.Series[Any] = df["standard_units"].value_counts()
            unit_dist_raw = unit_counts_series.to_dict()  # pyright: ignore[reportUnknownMemberType]
            unit_dist: dict[Any, int] = cast(dict[Any, int], unit_dist_raw)
            for unit_value, count in unit_dist.items():
                rows.append(
                    {
                        "section": "distribution",
                        "metric": "standard_units_count",
                        "column": "standard_units",
                        "value": str(unit_value) if unit_value is not None else "null",
                        "count": int(count),
                    }
                )

        return pd.DataFrame(rows)

    def write(
        self,
        df: pd.DataFrame,
        output_path: Path,
        *,
        extended: bool = False,
        include_correlation: bool | None = None,
        include_qc_metrics: bool | None = None,
    ) -> RunResult:
        """Override write() to bind actor and ensure deterministic sorting.

        Parameters
        ----------
        df:
            The DataFrame to write.
        output_path:
            The base output path for all artifacts.
        extended:
            Whether to include extended QC artifacts.

        Returns
        -------
        RunResult:
            All artifacts generated by the write operation.
        """
        log = UnifiedLogger.get(__name__).bind(component=f"{self.pipeline_code}.write")

        # Bind actor to logs for all write operations
        UnifiedLogger.bind(actor=self.actor)

        # Ensure sort configuration is set for activity pipeline
        # Use canonical identifier columns from schema: assay_chembl_id, testitem_chembl_id, activity_id
        sort_keys = ["assay_chembl_id", "testitem_chembl_id", "activity_id"]

        # Check if all sort keys exist in the DataFrame
        # If DataFrame is empty or missing columns, fall back to original sort config
        if df.empty or not all(key in df.columns for key in sort_keys):
            # Use original sort config if DataFrame is empty or missing required columns
            return super().write(
                df,
                output_path,
                extended=extended,
                include_correlation=include_correlation,
                include_qc_metrics=include_qc_metrics,
            )

        # Temporarily override sort config if not already set
        original_sort_by = self.config.determinism.sort.by
        if not original_sort_by or original_sort_by != sort_keys:
            # Create a modified config with the correct sort keys
            from copy import deepcopy

            from bioetl.config.models.determinism import DeterminismSortingConfig

            modified_config = deepcopy(self.config)
            modified_config.determinism.sort = DeterminismSortingConfig(
                by=sort_keys,
                ascending=[True, True, True],
                na_position="last",
            )

            log.debug(LogEvents.WRITE_SORT_CONFIG_SET,
                sort_keys=sort_keys,
                original_sort_keys=list(original_sort_by) if original_sort_by else [],
            )

            # Temporarily replace config
            original_config = self.config
            self.config = modified_config

            try:
                result = super().write(
                    df,
                    output_path,
                    extended=extended,
                    include_correlation=include_correlation,
                    include_qc_metrics=include_qc_metrics,
                )
            finally:
                # Restore original config
                self.config = original_config

            return result

        # If sort config already matches, proceed normally
        return super().write(
            df,
            output_path,
            extended=extended,
            include_correlation=include_correlation,
            include_qc_metrics=include_qc_metrics,
        )
