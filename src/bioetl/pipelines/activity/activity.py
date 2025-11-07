"""Activity pipeline implementation for ChEMBL."""

from __future__ import annotations

import hashlib
import json
import re
import time
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, cast
from urllib.parse import urlparse

import pandas as pd
import pandera.errors
from requests.exceptions import RequestException

from bioetl.clients import ChemblClient
from bioetl.config import ActivitySourceConfig, PipelineConfig
from bioetl.config.models.source import SourceConfig
from bioetl.core import APIClientFactory, UnifiedLogger
from bioetl.core.api_client import CircuitBreakerOpenError, UnifiedAPIClient
from bioetl.core.normalizers import (
    IdentifierRule,
    StringRule,
    normalize_identifier_columns,
    normalize_string_columns,
)
from bioetl.pipelines.common.validation import format_failure_cases, summarize_schema_errors
from bioetl.qc.report import build_quality_report as build_default_quality_report
from bioetl.schemas.activity import (
    ACTIVITY_PROPERTY_KEYS,
    COLUMN_ORDER,
    RELATIONS,
    STANDARD_TYPES,
    ActivitySchema,
)
from bioetl.schemas.vocab import required_vocab_ids

from ..base import PipelineBase, RunResult
from .activity_enrichment import (
    enrich_with_assay,
    enrich_with_compound_record,
    enrich_with_data_validity,
)
from .join_molecule import join_activity_with_molecule

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
    # data_validity_description извлекается отдельным запросом в extract через fetch_data_validity_lookup()
    "curated_by",  # Для вычисления curated: (curated_by IS NOT NULL) -> bool
)


class ChemblActivityPipeline(PipelineBase):
    """ETL pipeline extracting activity records from the ChEMBL API."""

    actor = "activity_chembl"

    def __init__(self, config: PipelineConfig, run_id: str) -> None:
        super().__init__(config, run_id)
        self._client_factory = APIClientFactory(config)
        self._chembl_release: str | None = None
        self._last_batch_extract_stats: dict[str, Any] | None = None

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

        # Check for input file and extract IDs if present
        if self.config.cli.input_file:
            id_column_name = self._get_id_column_name()
            ids = self._read_input_ids(
                id_column_name=id_column_name,
                limit=self.config.cli.limit,
                sample=self.config.cli.sample,
            )
            if ids:
                log.info("chembl_activity.extract_mode", mode="batch", ids_count=len(ids))
                return self.extract_by_ids(ids)

        # Legacy support: check kwargs for activity_ids (deprecated)
        payload_activity_ids = kwargs.get("activity_ids")
        if payload_activity_ids is not None:
            log.warning(
                "chembl_activity.deprecated_kwargs",
                message="Using activity_ids in kwargs is deprecated. Use --input-file instead.",
            )
            if isinstance(payload_activity_ids, Sequence):
                sequence_ids: Sequence[str | int] = cast(Sequence[str | int], payload_activity_ids)
                ids_list: list[str] = [str(id_val) for id_val in sequence_ids]
            else:
                ids_list = [str(payload_activity_ids)]
            return self.extract_by_ids(ids_list)

        log.info("chembl_activity.extract_mode", mode="full")
        return self.extract_all()

    def extract_all(self) -> pd.DataFrame:
        """Extract all activity records from ChEMBL using pagination."""

        log = UnifiedLogger.get(__name__).bind(component=f"{self.pipeline_code}.extract")
        stage_start = time.perf_counter()

        source_raw = self._resolve_source_config("chembl")
        source_config = ActivitySourceConfig.from_source_config(source_raw)
        base_url = self._resolve_base_url(source_config)
        client = self._client_factory.for_source("chembl", base_url=base_url)
        self.register_client("chembl_activity_client", client)

        self._chembl_release = self._fetch_chembl_release(client, log)

        batch_size = source_config.batch_size
        limit = self.config.cli.limit
        page_size = min(batch_size, 25)
        if limit is not None:
            page_size = min(page_size, limit)
        page_size = max(page_size, 1)

        parameters: Mapping[str, Any] = (  # type: ignore[assignment, misc]
            cast(Mapping[str, Any], source_config.parameters)  # type: ignore[arg-type]
            if isinstance(source_config.parameters, Mapping)  # type: ignore[arg-type]
            else {}
        )
        select_fields_raw: Any = parameters.get("select_fields")  # type: ignore[assignment, misc]
        if (
            select_fields_raw is not None
            and isinstance(select_fields_raw, Sequence)  # type: ignore[arg-type]
            and not isinstance(select_fields_raw, (str, bytes))  # type: ignore[arg-type]
        ):
            select_fields: list[str] = [str(field) for field in select_fields_raw]  # type: ignore[arg-type, misc]
        else:
            select_fields = list(API_ACTIVITY_FIELDS)
        records: list[dict[str, Any]] = []
        next_endpoint: str | None = "/activity.json"
        params: Mapping[str, Any] | None = {
            "limit": page_size,
            "only": ",".join(select_fields),
        }
        parameters_dict = dict(sorted(parameters.items()))
        filters_payload: dict[str, Any] = {
            "mode": "all",
            "limit": int(limit) if limit is not None else None,
            "page_size": page_size,
            "select_fields": list(select_fields),
        }
        if parameters_dict:
            filters_payload["parameters"] = parameters_dict
        compact_filters = {key: value for key, value in filters_payload.items() if value is not None}
        self.record_extract_metadata(
            chembl_release=self._chembl_release,
            filters=compact_filters,
            requested_at_utc=datetime.now(timezone.utc),
        )
        pages = 0

        while next_endpoint:
            page_start = time.perf_counter()
            response = client.get(next_endpoint, params=params)
            payload = self._coerce_mapping(response.json())
            page_items = self._extract_page_items(payload)

            # Process nested fields for each item
            processed_items: list[dict[str, Any]] = []
            for item in page_items:
                processed_item = self._extract_nested_fields(dict(item))
                processed_item = self._extract_activity_properties_fields(processed_item)
                processed_items.append(processed_item)

            if limit is not None:
                remaining = max(limit - len(records), 0)
                if remaining == 0:
                    break
                processed_items = processed_items[:remaining]

            records.extend(processed_items)
            pages += 1
            page_duration_ms = (time.perf_counter() - page_start) * 1000.0
            log.debug(
                "chembl_activity.page_fetched",
                endpoint=next_endpoint,
                batch_size=len(processed_items),
                total_records=len(records),
                duration_ms=page_duration_ms,
            )

            next_link = self._next_link(payload, base_url=base_url)
            if not next_link or (limit is not None and len(records) >= limit):
                break
            # Log the next_link before using it for debugging
            log.info(
                "chembl_activity.next_link_resolved",
                next_link=next_link,
                base_url=base_url,
            )
            next_endpoint = next_link
            params = None

        dataframe: pd.DataFrame = pd.DataFrame.from_records(records)  # pyright: ignore[reportUnknownMemberType]; type: ignore
        if dataframe.empty:
            dataframe = pd.DataFrame({"activity_id": pd.Series(dtype="Int64")})
        elif "activity_id" in dataframe.columns:
            dataframe = dataframe.sort_values("activity_id").reset_index(drop=True)

        # Гарантия присутствия полей комментариев
        dataframe = self._ensure_comment_fields(dataframe, log)

        # Извлечение data_validity_description из DATA_VALIDITY_LOOKUP
        chembl_client = ChemblClient(client)
        dataframe = self._extract_data_validity_descriptions(dataframe, chembl_client, log)

        # Логирование метрик заполненности
        self._log_validity_comments_metrics(dataframe, log)

        duration_ms = (time.perf_counter() - stage_start) * 1000.0
        log.info(
            "chembl_activity.extract_summary",
            rows=int(dataframe.shape[0]),
            duration_ms=duration_ms,
            chembl_release=self._chembl_release,
            pages=pages,
        )
        return dataframe

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

        source_raw = self._resolve_source_config("chembl")
        source_config = ActivitySourceConfig.from_source_config(source_raw)
        base_url = self._resolve_base_url(source_config)
        client = self._client_factory.for_source("chembl", base_url=base_url)
        self.register_client("chembl_activity_client", client)

        self._chembl_release = self._fetch_chembl_release(client, log)

        batch_size = source_config.batch_size
        limit = self.config.cli.limit

        # Convert list of IDs to DataFrame for compatibility with _extract_from_chembl
        input_frame = pd.DataFrame({"activity_id": list(ids)})

        id_filters = {
            "mode": "ids",
            "requested_ids": [str(id_value) for id_value in ids],
            "limit": int(limit) if limit is not None else None,
            "batch_size": batch_size,
        }
        compact_id_filters = {key: value for key, value in id_filters.items() if value is not None}
        self.record_extract_metadata(
            chembl_release=self._chembl_release,
            filters=compact_id_filters,
            requested_at_utc=datetime.now(timezone.utc),
        )

        batch_dataframe = self._extract_from_chembl(
            input_frame,
            client,
            batch_size=batch_size,
            limit=limit,
        )

        duration_ms = (time.perf_counter() - stage_start) * 1000.0
        batch_stats = self._last_batch_extract_stats or {}
        log.info(
            "chembl_activity.extract_by_ids_summary",
            rows=int(batch_dataframe.shape[0]),
            requested=len(ids),
            duration_ms=duration_ms,
            chembl_release=self._chembl_release,
            batches=batch_stats.get("batches"),
            api_calls=batch_stats.get("api_calls"),
            cache_hits=batch_stats.get("cache_hits"),
        )
        return batch_dataframe

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform raw activity data by normalizing measurements, identifiers, and data types."""

        log = UnifiedLogger.get(__name__).bind(component=f"{self.pipeline_code}.transform")

        # According to documentation, transform should accept df: pd.DataFrame
        # df is already a pd.DataFrame, so we can use it directly
        df = df.copy()

        df = self._harmonize_identifier_columns(df, log)
        df = self._ensure_schema_columns(df, COLUMN_ORDER, log)

        if df.empty:
            log.debug("transform_empty_dataframe")
            return df

        log.info("transform_started", rows=len(df))

        df = self._normalize_identifiers(df, log)
        df = self._normalize_measurements(df, log)
        df = self._normalize_string_fields(df, log)
        df = self._normalize_nested_structures(df, log)
        df = self._add_row_metadata(df, log)
        df = self._normalize_data_types(df, ActivitySchema, log)

        # Восстановить curated из curated_by если curated пусто
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

        log.info("transform_completed", rows=len(df))
        return df

    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate payload against ActivitySchema with detailed error handling."""

        log = UnifiedLogger.get(__name__).bind(component=f"{self.pipeline_code}.validate")

        if df.empty:
            log.debug("validate_empty_dataframe")
            return df

        if self.config.validation.strict:
            allowed_columns = set(COLUMN_ORDER)
            extra_columns = [column for column in df.columns if column not in allowed_columns]
            if extra_columns:
                log.debug(
                    "drop_extra_columns_before_validation",
                    extras=extra_columns,
                )
                df = df.drop(columns=extra_columns)

        log.info("validate_started", rows=len(df))

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

        # Soft enum валидация для data_validity_comment
        self._validate_data_validity_comment_soft_enum(df, log)

        # Call base validation with error handling
        # Temporarily disable coercion to preserve nullable Int64 types for target_tax_id
        # Schema has coerce=False, but base.validate uses config.validation.coerce
        original_coerce = self.config.validation.coerce
        try:
            self.config.validation.coerce = False
            validated = super().validate(df)
            log.info(
                "validate_completed",
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

            log.error(
                "validation_failed",
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
                log.error("validation_failure_cases", failure_cases=failure_cases_summary)
                # Log individual errors with row index and activity_id as per documentation
                self._log_detailed_validation_errors(failure_cases_df, df, log)

            msg = (
                f"Validation failed with {error_count} error(s) against schema "
                f"{self.config.validation.schema_out}: {error_summary}"
            )
            raise ValueError(msg) from exc
        except Exception as exc:
            log.error(
                "validation_error",
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

    def _resolve_source_config(self, name: str) -> SourceConfig:
        try:
            return self.config.sources[name]
        except KeyError as exc:  # pragma: no cover - configuration error path
            msg = f"Source '{name}' is not configured for pipeline '{self.pipeline_code}'"
            raise KeyError(msg) from exc

    @staticmethod
    def _resolve_base_url(source_config: ActivitySourceConfig) -> str:
        base_url = source_config.parameters.base_url or "https://www.ebi.ac.uk/chembl/api/data"
        if not base_url.strip():
            msg = "sources.chembl.parameters.base_url must be a non-empty string"
            raise ValueError(msg)
        return base_url.rstrip("/")

    def _should_enrich_compound_record(self) -> bool:
        """Проверить, включено ли обогащение compound_record в конфиге."""
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
        """Обогатить DataFrame полями из compound_record."""
        log = UnifiedLogger.get(__name__).bind(component=f"{self.pipeline_code}.enrich")

        # Получить конфигурацию обогащения
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
                            compound_record_section = cast(Mapping[str, Any], compound_record_section)
                            enrich_cfg = dict(compound_record_section)
        except (AttributeError, KeyError, TypeError) as exc:
            log.warning(
                "enrichment_config_error",
                error=str(exc),
                message="Using default enrichment config",
            )

        # Создать или переиспользовать клиент ChEMBL
        source_raw = self._resolve_source_config("chembl")
        source_config = ActivitySourceConfig.from_source_config(source_raw)
        base_url = self._resolve_base_url(source_config)
        api_client = self._client_factory.for_source("chembl", base_url=base_url)
        # Регистрируем клиент только если он еще не зарегистрирован
        if "chembl_enrichment_client" not in self._registered_clients:
            self.register_client("chembl_enrichment_client", api_client)
        chembl_client = ChemblClient(api_client)

        # Вызвать функцию обогащения
        return enrich_with_compound_record(df, chembl_client, enrich_cfg)

    def _should_enrich_assay(self) -> bool:
        """Проверить, включено ли обогащение assay в конфиге."""
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
        """Обогатить DataFrame полями из assay."""
        log = UnifiedLogger.get(__name__).bind(component=f"{self.pipeline_code}.enrich")

        # Получить конфигурацию обогащения
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
            log.warning(
                "enrichment_config_error",
                error=str(exc),
                message="Using default enrichment config",
            )

        # Создать или переиспользовать клиент ChEMBL
        source_raw = self._resolve_source_config("chembl")
        source_config = ActivitySourceConfig.from_source_config(source_raw)
        base_url = self._resolve_base_url(source_config)
        api_client = self._client_factory.for_source("chembl", base_url=base_url)
        # Регистрируем клиент только если он еще не зарегистрирован
        if "chembl_enrichment_client" not in self._registered_clients:
            self.register_client("chembl_enrichment_client", api_client)
        chembl_client = ChemblClient(api_client)

        # Вызвать функцию обогащения
        return enrich_with_assay(df, chembl_client, enrich_cfg)

    def _should_enrich_data_validity(self) -> bool:
        """Проверить, включено ли обогащение data_validity в конфиге."""
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
        """Обогатить DataFrame полями из data_validity_lookup."""
        log = UnifiedLogger.get(__name__).bind(component=f"{self.pipeline_code}.enrich")

        # Получить конфигурацию обогащения
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
            log.warning(
                "enrichment_config_error",
                error=str(exc),
                message="Using default enrichment config",
            )

        # Проверка: если data_validity_description уже заполнено (не все NA), логировать информационное сообщение
        if "data_validity_description" in df.columns:
            non_na_count = int(df["data_validity_description"].notna().sum())
            if non_na_count > 0:
                log.info(
                    "enrichment_data_validity_description_already_filled",
                    non_na_count=non_na_count,
                    total_count=len(df),
                    message="data_validity_description already populated from extract, enrichment will update/overwrite",
                )

        # Создать или переиспользовать клиент ChEMBL
        source_raw = self._resolve_source_config("chembl")
        source_config = ActivitySourceConfig.from_source_config(source_raw)
        base_url = self._resolve_base_url(source_config)
        api_client = self._client_factory.for_source("chembl", base_url=base_url)
        # Регистрируем клиент только если он еще не зарегистрирован
        if "chembl_enrichment_client" not in self._registered_clients:
            self.register_client("chembl_enrichment_client", api_client)
        chembl_client = ChemblClient(api_client)

        # Вызвать функцию обогащения (может обновить/перезаписать данные из extract)
        return enrich_with_data_validity(df, chembl_client, enrich_cfg)

    def _should_enrich_molecule(self) -> bool:
        """Проверить, включено ли обогащение molecule в конфиге."""
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
        """Обогатить DataFrame полями из molecule через join."""
        log = UnifiedLogger.get(__name__).bind(component=f"{self.pipeline_code}.enrich")

        # Получить конфигурацию обогащения
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
            log.warning(
                "enrichment_config_error",
                error=str(exc),
                message="Using default enrichment config",
            )

        # Создать или переиспользовать клиент ChEMBL
        source_raw = self._resolve_source_config("chembl")
        source_config = ActivitySourceConfig.from_source_config(source_raw)
        base_url = self._resolve_base_url(source_config)
        api_client = self._client_factory.for_source("chembl", base_url=base_url)
        # Регистрируем клиент только если он еще не зарегистрирован
        if "chembl_enrichment_client" not in self._registered_clients:
            self.register_client("chembl_enrichment_client", api_client)
        chembl_client = ChemblClient(api_client)

        # Вызвать функцию join для получения molecule_name
        df_join = join_activity_with_molecule(df, chembl_client, enrich_cfg)

        # Коалесцировать molecule_name из join в molecule_pref_name если оно пусто
        if "molecule_name" in df_join.columns:
            if "molecule_pref_name" not in df.columns:
                df["molecule_pref_name"] = pd.NA
            # Заполнить molecule_pref_name из molecule_name где оно пусто
            mask = df["molecule_pref_name"].isna() | (df["molecule_pref_name"].astype("string").str.strip() == "")
            if mask.any():
                # Merge по activity_id для получения molecule_name
                df_merged = df.merge(
                    df_join[["activity_id", "molecule_name"]],
                    on="activity_id",
                    how="left",
                    suffixes=("", "_join"),
                )
                # Заполнить molecule_pref_name из molecule_name где оно пусто
                df.loc[mask, "molecule_pref_name"] = df_merged.loc[mask, "molecule_name"]
                log.info(
                    "molecule_pref_name_enriched",
                    rows_enriched=int(mask.sum()),
                    total_rows=len(df),
                )

        return df

    def _fetch_chembl_release(
        self,
        client: UnifiedAPIClient,
        log: Any,
    ) -> str | None:
        response = client.get("/status.json")
        status_payload = self._coerce_mapping(response.json())
        release_value = self._extract_chembl_release(status_payload)
        log.info("chembl_activity.status", chembl_release=release_value)
        return release_value

    def _extract_from_chembl(
        self,
        dataset: object,
        client: UnifiedAPIClient,
        *,
        batch_size: int | None = None,
        limit: int | None = None,
    ) -> pd.DataFrame:
        """Extract activity records by batching ``activity_id`` values."""

        log = UnifiedLogger.get(__name__).bind(component=f"{self.pipeline_code}.extract")
        method_start = time.perf_counter()
        self._last_batch_extract_stats = None
        source_config = self._resolve_source_config("chembl")

        if isinstance(dataset, pd.Series):
            input_frame = dataset.to_frame(name="activity_id")
        elif isinstance(dataset, pd.DataFrame):
            input_frame = dataset
        elif isinstance(dataset, Mapping):
            input_frame = pd.DataFrame([cast(dict[str, Any], dataset)])
        elif isinstance(dataset, Sequence) and not isinstance(dataset, (str, bytes)):
            dataset_list: list[Any] = list(dataset)  # pyright: ignore[reportUnknownArgumentType]
            input_frame = pd.DataFrame({"activity_id": dataset_list})
        else:
            msg = (
                "ChemblActivityPipeline._extract_from_chembl expects a DataFrame, Series, "
                "mapping, or sequence of activity_id values"
            )
            raise TypeError(msg)

        if "activity_id" not in input_frame.columns:
            msg = "Input dataset must contain an 'activity_id' column"
            raise ValueError(msg)

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
            log.warning(
                "chembl_activity.invalid_activity_ids",
                invalid_count=len(invalid_ids),
            )

        if limit is not None:
            normalized_ids = normalized_ids[: max(int(limit), 0)]

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
            log.info("chembl_activity.batch_summary", **summary)
            return pd.DataFrame()

        source_raw = self._resolve_source_config("chembl")
        source_config_temp = ActivitySourceConfig.from_source_config(source_raw)
        effective_batch_size = batch_size or source_config_temp.batch_size
        effective_batch_size = max(min(int(effective_batch_size), 25), 1)

        records: list[dict[str, Any]] = []
        success_count = 0
        fallback_count = 0
        error_count = 0
        cache_hits = 0
        api_calls = 0
        total_batches = 0

        for index in range(0, len(normalized_ids), effective_batch_size):
            batch = normalized_ids[index : index + effective_batch_size]
            batch_keys: list[str] = [key for _, key in batch]
            batch_start = time.perf_counter()
            try:
                cached_records = self._check_cache(batch_keys, self._chembl_release)
                from_cache = cached_records is not None
                batch_records: dict[str, dict[str, Any]] = {}
                if cached_records is not None:
                    batch_records = cached_records
                    cache_hits += len(batch_keys)
                else:
                    parameters: Mapping[str, Any] = (  # type: ignore[assignment, misc]
                        cast(Mapping[str, Any], source_config.parameters)  # type: ignore[arg-type]
                        if isinstance(source_config.parameters, Mapping)  # type: ignore[arg-type]
                        else {}
                    )
                    select_fields_raw: Any = parameters.get("select_fields")  # type: ignore[assignment, misc]
                    if (
                        select_fields_raw is not None
                        and isinstance(select_fields_raw, Sequence)  # type: ignore[arg-type]
                        and not isinstance(select_fields_raw, (str, bytes))  # type: ignore[arg-type]
                    ):
                        select_fields: list[str] = [str(field) for field in select_fields_raw]  # type: ignore[arg-type, misc]
                    else:
                        select_fields = list(API_ACTIVITY_FIELDS)
                    params = {
                        "activity_id__in": ",".join(batch_keys),
                        "only": ",".join(select_fields),
                    }
                    response = client.get("/activity.json", params=params)
                    api_calls += 1
                    payload = self._coerce_mapping(response.json())
                    for item in self._extract_page_items(payload):
                        activity_value = item.get("activity_id")
                        if activity_value is None:
                            continue
                        batch_records[str(activity_value)] = item
                    self._store_cache(batch_keys, batch_records, self._chembl_release)

                success_in_batch = 0
                for numeric_id, key in batch:
                    record = batch_records.get(key)
                    if record and not record.get("error"):
                        materialised = dict(record)
                        materialised = self._extract_nested_fields(materialised)
                        materialised = self._extract_activity_properties_fields(materialised)
                        materialised.setdefault("activity_id", numeric_id)
                        records.append(materialised)
                        success_count += 1
                        success_in_batch += 1
                    else:
                        fallback_record = self._create_fallback_record(numeric_id)
                        records.append(fallback_record)
                        fallback_count += 1
                        error_count += 1
                total_batches += 1
                batch_duration_ms = (time.perf_counter() - batch_start) * 1000.0
                log.debug(
                    "chembl_activity.batch_processed",
                    batch_size=len(batch_keys),
                    from_cache=from_cache,
                    success_in_batch=success_in_batch,
                    fallback_in_batch=len(batch_keys) - success_in_batch,
                    duration_ms=batch_duration_ms,
                )
            except CircuitBreakerOpenError as exc:
                total_batches += 1
                log.warning(
                    "chembl_activity.batch_circuit_breaker",
                    batch_size=len(batch_keys),
                    error=str(exc),
                )
                for numeric_id, _ in batch:
                    records.append(self._create_fallback_record(numeric_id, exc))
                    fallback_count += 1
                    error_count += 1
            except RequestException as exc:
                total_batches += 1
                log.error(
                    "chembl_activity.batch_request_error",
                    batch_size=len(batch_keys),
                    error=str(exc),
                )
                for numeric_id, _ in batch:
                    records.append(self._create_fallback_record(numeric_id, exc))
                    fallback_count += 1
                    error_count += 1
            except Exception as exc:  # pragma: no cover - defensive path
                total_batches += 1
                log.error(
                    "chembl_activity.batch_unhandled_error",
                    batch_size=len(batch_keys),
                    error=str(exc),
                    exc_info=True,
                )
                for numeric_id, _ in batch:
                    records.append(self._create_fallback_record(numeric_id, exc))
                    fallback_count += 1
                    error_count += 1

        duration_ms = (time.perf_counter() - method_start) * 1000.0
        total_records = len(normalized_ids)
        success_rate = (
            float(success_count + fallback_count) / float(total_records)
            if total_records
            else 0.0
        )
        summary = {
            "total_activities": total_records,
            "success": success_count,
            "fallback": fallback_count,
            "errors": error_count,
            "api_calls": api_calls,
            "cache_hits": cache_hits,
            "batches": total_batches,
            "duration_ms": duration_ms,
            "success_rate": success_rate,
        }
        self._last_batch_extract_stats = summary
        log.info("chembl_activity.batch_summary", **summary)

        dataframe: pd.DataFrame = pd.DataFrame.from_records(records)  # pyright: ignore[reportUnknownMemberType]; type: ignore
        if dataframe.empty:
            dataframe = pd.DataFrame({"activity_id": pd.Series(dtype="Int64")})
        elif "activity_id" in dataframe.columns:
            dataframe = dataframe.sort_values("activity_id").reset_index(drop=True)

        # Гарантия присутствия полей комментариев
        log = UnifiedLogger.get(__name__).bind(component=f"{self.pipeline_code}.extract")
        dataframe = self._ensure_comment_fields(dataframe, log)

        # Извлечение data_validity_description из DATA_VALIDITY_LOOKUP
        source_raw = self._resolve_source_config("chembl")
        source_config = ActivitySourceConfig.from_source_config(source_raw)
        base_url = self._resolve_base_url(source_config)
        extract_client = self._client_factory.for_source("chembl", base_url=base_url)
        chembl_client = ChemblClient(extract_client)
        dataframe = self._extract_data_validity_descriptions(dataframe, chembl_client, log)

        # Извлечение assay_organism и assay_tax_id из ASSAYS
        dataframe = self._extract_assay_fields(dataframe, chembl_client, log)

        # Логирование метрик заполненности
        self._log_validity_comments_metrics(dataframe, log)

        return dataframe

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
            identifier: cast(dict[str, Any], payload[identifier])
            for identifier in normalized_ids
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
            log.debug("chembl_activity.cache_store_failed", error=str(exc))

    def _cache_file_path(self, batch_ids: Sequence[str], release: str | None) -> Path:
        directory = self._cache_directory(release)
        cache_key = self._cache_key(batch_ids, release)
        return directory / f"{cache_key}.json"

    def _cache_directory(self, release: str | None) -> Path:
        cache_root = Path(self.config.paths.cache_root)
        directory_name = (self.config.cache.directory or "http_cache").strip() or "http_cache"
        release_component = self._sanitize_cache_component(release or "unknown")
        pipeline_component = self._sanitize_cache_component(self.pipeline_code)
        version_component = self._sanitize_cache_component(self.config.pipeline.version or "unknown")
        return cache_root / directory_name / pipeline_component / release_component / version_component

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

    def _create_fallback_record(self, activity_id: int, error: Exception | None = None) -> dict[str, Any]:
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
            "activity_properties": self._serialize_activity_properties(
                fallback_properties
            ),
        }

    def _ensure_comment_fields(self, df: pd.DataFrame, log: Any) -> pd.DataFrame:
        """Гарантировать присутствие полей комментариев в DataFrame.

        Если поля отсутствуют, добавляет их с pd.NA (dtype="string").
        data_validity_description извлекается отдельным запросом в extract через _extract_data_validity_descriptions().

        Parameters
        ----------
        df:
            DataFrame для проверки и дополнения.
        log:
            Logger instance.

        Returns
        -------
        pd.DataFrame:
            DataFrame с гарантированным наличием полей комментариев.
        """
        if df.empty:
            return df

        required_comment_fields = ["activity_comment", "data_validity_comment", "data_validity_description"]
        missing_fields = [field for field in required_comment_fields if field not in df.columns]

        if missing_fields:
            for field in missing_fields:
                df[field] = pd.Series([pd.NA] * len(df), dtype="string")
            log.debug("comment_fields_ensured", fields=missing_fields)

        return df

    def _extract_data_validity_descriptions(
        self, df: pd.DataFrame, client: ChemblClient, log: Any
    ) -> pd.DataFrame:
        """Извлечь data_validity_description из DATA_VALIDITY_LOOKUP через отдельный запрос.

        Собирает уникальные непустые значения data_validity_comment из DataFrame,
        вызывает fetch_data_validity_lookup() и выполняет LEFT JOIN обратно к DataFrame.

        Parameters
        ----------
        df:
            DataFrame с данными activity, должен содержать data_validity_comment.
        client:
            ChemblClient для запросов к ChEMBL API.
        log:
            Logger instance.

        Returns
        -------
        pd.DataFrame:
            DataFrame с заполненной колонкой data_validity_description.
        """
        if df.empty:
            return df

        if "data_validity_comment" not in df.columns:
            log.debug("extract_data_validity_descriptions_skipped", reason="data_validity_comment_column_missing")
            return df

        # Собрать уникальные непустые значения data_validity_comment
        validity_comments: list[str] = []
        for _, row in df.iterrows():
            comment = row.get("data_validity_comment")

            # Пропускаем NaN/None значения
            if pd.isna(comment) or comment is None:
                continue

            # Преобразуем в строку
            comment_str = str(comment).strip()

            if comment_str:
                validity_comments.append(comment_str)

        if not validity_comments:
            log.debug("extract_data_validity_descriptions_skipped", reason="no_valid_comments")
            # Гарантируем наличие колонки с pd.NA
            if "data_validity_description" not in df.columns:
                df["data_validity_description"] = pd.Series([pd.NA] * len(df), dtype="string")
            return df

        # Вызвать fetch_data_validity_lookup для получения descriptions
        unique_comments = list(set(validity_comments))
        log.info("extract_data_validity_descriptions_fetching", comments_count=len(unique_comments))

        try:
            records_dict = client.fetch_data_validity_lookup(
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
            # Гарантируем наличие колонки с pd.NA
            if "data_validity_description" not in df.columns:
                df["data_validity_description"] = pd.Series([pd.NA] * len(df), dtype="string")
            return df

        # Создать DataFrame для join
        enrichment_data: list[dict[str, Any]] = []
        for comment in unique_comments:
            record = records_dict.get(comment)
            if record:
                enrichment_data.append({
                    "data_validity_comment": comment,
                    "data_validity_description": record.get("description"),
                })
            else:
                # Если запись не найдена, оставляем description как NULL
                enrichment_data.append({
                    "data_validity_comment": comment,
                    "data_validity_description": None,
                })

        if not enrichment_data:
            log.debug("extract_data_validity_descriptions_no_records")
            # Гарантируем наличие колонки с pd.NA
            if "data_validity_description" not in df.columns:
                df["data_validity_description"] = pd.Series([pd.NA] * len(df), dtype="string")
            return df

        df_enrich = pd.DataFrame(enrichment_data)

        # Сохранить исходный порядок строк через индекс
        original_index = df.index.copy()

        # LEFT JOIN обратно к df на data_validity_comment
        df_result = df.merge(
            df_enrich,
            on=["data_validity_comment"],
            how="left",
            suffixes=("", "_enrich"),
        )

        # Убедиться, что колонка присутствует (заполнить NA для отсутствующих)
        if "data_validity_description" not in df_result.columns:
            df_result["data_validity_description"] = pd.Series([pd.NA] * len(df_result), dtype="string")
        else:
            # Если колонка уже была, нормализовать типы
            df_result["data_validity_description"] = df_result["data_validity_description"].astype("string")

        # Восстановить исходный порядок
        df_result = df_result.reindex(original_index)

        log.info(
            "extract_data_validity_descriptions_complete",
            comments_requested=len(unique_comments),
            records_fetched=len(enrichment_data),
            rows_enriched=len(df_result),
        )

        return df_result

    def _extract_assay_fields(
        self, df: pd.DataFrame, client: ChemblClient, log: Any
    ) -> pd.DataFrame:
        """Извлечь assay_organism и assay_tax_id из ASSAYS через отдельный запрос.

        Собирает уникальные непустые значения assay_chembl_id из DataFrame,
        вызывает fetch_assays_by_ids() и выполняет LEFT JOIN обратно к DataFrame.

        Parameters
        ----------
        df:
            DataFrame с данными activity, должен содержать assay_chembl_id.
        client:
            ChemblClient для запросов к ChEMBL API.
        log:
            Logger instance.

        Returns
        -------
        pd.DataFrame:
            DataFrame с заполненными колонками assay_organism и assay_tax_id.
        """
        if df.empty:
            return df

        if "assay_chembl_id" not in df.columns:
            log.debug("extract_assay_fields_skipped", reason="assay_chembl_id_column_missing")
            # Гарантируем наличие колонок с pd.NA
            for col, dtype in (("assay_organism", "string"), ("assay_tax_id", "Int64")):
                if col not in df.columns:
                    df[col] = pd.Series([pd.NA] * len(df), dtype=dtype)
            return df

        # Собрать уникальные непустые значения assay_chembl_id
        assay_ids: list[str] = []
        for _, row in df.iterrows():
            assay_id = row.get("assay_chembl_id")

            # Пропускаем NaN/None значения
            if pd.isna(assay_id) or assay_id is None:
                continue

            # Преобразуем в строку и нормализуем
            assay_id_str = str(assay_id).strip().upper()

            if assay_id_str:
                assay_ids.append(assay_id_str)

        if not assay_ids:
            log.debug("extract_assay_fields_skipped", reason="no_valid_assay_ids")
            # Гарантируем наличие колонок с pd.NA
            for col, dtype in (("assay_organism", "string"), ("assay_tax_id", "Int64")):
                if col not in df.columns:
                    df[col] = pd.Series([pd.NA] * len(df), dtype=dtype)
            return df

        # Вызвать fetch_assays_by_ids для получения assay данных
        unique_assay_ids = list(set(assay_ids))
        log.info("extract_assay_fields_fetching", assay_ids_count=len(unique_assay_ids))

        try:
            records_dict = client.fetch_assays_by_ids(
                ids=unique_assay_ids,
                fields=["assay_chembl_id", "assay_organism", "assay_tax_id"],
                page_limit=1000,
            )
        except Exception as exc:
            log.warning(
                "extract_assay_fields_fetch_error",
                error=str(exc),
                exc_info=True,
            )
            # Гарантируем наличие колонок с pd.NA
            for col, dtype in (("assay_organism", "string"), ("assay_tax_id", "Int64")):
                if col not in df.columns:
                    df[col] = pd.Series([pd.NA] * len(df), dtype=dtype)
            return df

        # Создать DataFrame для join
        enrichment_data: list[dict[str, Any]] = []
        for assay_id in unique_assay_ids:
            record = records_dict.get(assay_id) if records_dict else None
            if record:
                enrichment_data.append({
                    "assay_chembl_id": assay_id,
                    "assay_organism": record.get("assay_organism"),
                    "assay_tax_id": record.get("assay_tax_id"),
                })
            else:
                # Если запись не найдена, оставляем значения как NULL
                enrichment_data.append({
                    "assay_chembl_id": assay_id,
                    "assay_organism": None,
                    "assay_tax_id": None,
                })

        if not enrichment_data:
            log.debug("extract_assay_fields_no_records")
            # Гарантируем наличие колонок с pd.NA
            for col, dtype in (("assay_organism", "string"), ("assay_tax_id", "Int64")):
                if col not in df.columns:
                    df[col] = pd.Series([pd.NA] * len(df), dtype=dtype)
            return df

        df_enrich = pd.DataFrame(enrichment_data)

        # Нормализовать assay_chembl_id в df_enrich для join (upper, strip)
        df_enrich["assay_chembl_id"] = (
            df_enrich["assay_chembl_id"].astype("string").str.strip().str.upper()
        )

        # Нормализовать assay_chembl_id в df для join (upper, strip)
        original_index = df.index.copy()
        df_normalized = df.copy()
        df_normalized["assay_chembl_id_normalized"] = (
            df_normalized["assay_chembl_id"].astype("string").str.strip().str.upper()
        )

        # LEFT JOIN обратно к df на assay_chembl_id
        df_result = df_normalized.merge(
            df_enrich,
            left_on="assay_chembl_id_normalized",
            right_on="assay_chembl_id",
            how="left",
            suffixes=("", "_enrich"),
        )

        # Удалить временную нормализованную колонку
        df_result = df_result.drop(columns=["assay_chembl_id_normalized"])

        # Коалесценс после merge: *_enrich → базовые колонки
        for col in ["assay_organism", "assay_tax_id"]:
            if f"{col}_enrich" in df_result.columns:
                # Коалесценс: использовать значение из обогащения, если базовое значение NA
                if col not in df_result.columns:
                    df_result[col] = df_result[f"{col}_enrich"]
                else:
                    # Заменить NA значения значениями из _enrich
                    df_result[col] = df_result[col].fillna(df_result[f"{col}_enrich"])  # type: ignore[assignment]
                # Удалить колонку _enrich
                df_result = df_result.drop(columns=[f"{col}_enrich"])

        # Убедиться, что все новые колонки присутствуют (заполнить NA для отсутствующих)
        for col, dtype in (("assay_organism", "string"), ("assay_tax_id", "Int64")):
            if col not in df_result.columns:
                df_result[col] = pd.Series([pd.NA] * len(df_result), dtype=dtype)

        # Приведение типов
        df_result["assay_organism"] = df_result["assay_organism"].astype("string")

        # assay_tax_id может приходить строкой — приводим к Int64 с NA
        tax_id_numeric: pd.Series[Any] = pd.to_numeric(df_result["assay_tax_id"], errors="coerce")  # type: ignore[arg-type]
        df_result["assay_tax_id"] = tax_id_numeric.astype("Int64")

        # Проверка диапазона для assay_tax_id (>= 1 или NA)
        mask_valid = df_result["assay_tax_id"].notna()
        if mask_valid.any():
            invalid_mask = mask_valid & (df_result["assay_tax_id"] < 1)
            if invalid_mask.any():
                log.warning(
                    "invalid_assay_tax_id_range",
                    count=int(invalid_mask.sum()),
                )
                df_result.loc[invalid_mask, "assay_tax_id"] = pd.NA

        # Восстановить исходный порядок
        df_result = df_result.reindex(original_index)

        log.info(
            "extract_assay_fields_complete",
            assay_ids_requested=len(unique_assay_ids),
            records_fetched=len(enrichment_data),
            rows_enriched=len(df_result),
        )

        return df_result

    def _log_validity_comments_metrics(self, df: pd.DataFrame, log: Any) -> None:
        """Логировать метрики заполненности полей комментариев.

        Вычисляет:
        - Долю NA для каждого из трех полей
        - Топ-10 наиболее частых значений data_validity_comment
        - Количество неизвестных значений data_validity_comment (не в whitelist)

        Parameters
        ----------
        df:
            DataFrame с данными activity.
        log:
            Logger instance.
        """
        if df.empty:
            return

        # Вычисление долей NA
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

        # Топ-10 наиболее частых значений data_validity_comment
        if "data_validity_comment" in df.columns:
            non_null_comments = df["data_validity_comment"].dropna()
            if len(non_null_comments) > 0:
                value_counts = non_null_comments.value_counts().head(10)
                top_10 = value_counts.to_dict()  # type: ignore[reportUnknownArgumentType]
                metrics["top_10_data_validity_comments"] = top_10

        # Количество неизвестных значений data_validity_comment
        whitelist = self._get_data_validity_comment_whitelist()
        if whitelist and "data_validity_comment" in df.columns:
            non_null_comments = df["data_validity_comment"].dropna()
            if len(non_null_comments) > 0:
                whitelist_set = set(whitelist)
                unknown_mask = ~non_null_comments.isin(whitelist_set)  # type: ignore[reportUnknownArgumentType]
                unknown_count = int(unknown_mask.sum())
                if unknown_count > 0:
                    unknown_values = non_null_comments[unknown_mask].value_counts().head(10).to_dict()  # type: ignore[reportUnknownArgumentType]
                    metrics["unknown_data_validity_comments_count"] = unknown_count
                    metrics["unknown_data_validity_comments_samples"] = unknown_values
                    log.warning(
                        "unknown_data_validity_comments_detected",
                        unknown_count=unknown_count,
                        samples=unknown_values,
                        whitelist=whitelist,
                    )

        if metrics:
            log.info("validity_comments_metrics", **metrics)

    def _get_data_validity_comment_whitelist(self) -> list[str] | None:
        """Получить whitelist допустимых значений для data_validity_comment из конфига.

        Returns
        -------
        list[str] | None:
            Список допустимых значений или None, если не настроен.
        """
        try:
            values = sorted(required_vocab_ids("data_validity_comment"))
        except RuntimeError as exc:
            UnifiedLogger.get(__name__).warning(
                "data_validity_comment_dictionary_unavailable",
                error=str(exc),
            )
            return None
        return values

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
        """Extract TRUV fields, standard_* fields, and comments from activity_properties array as fallback.

        Поля извлекаются из activity_properties только если они отсутствуют
        в основном ответе API (приоритет у прямых полей из ACTIVITIES).
        Извлекаются оригинальные TRUV-поля: value, text_value, relation, units.
        Также извлекаются стандартизованные поля: standard_upper_value, standard_text_value.
        Извлекаются диапазоны: upper_value, lower_value.
        Извлекаются комментарии: activity_comment, data_validity_comment.

        Также нормализует и сохраняет activity_properties в записи для последующей сериализации.
        Выполняет валидацию TRUV-формата и дедупликацию точных дубликатов.
        """
        log = UnifiedLogger.get(__name__).bind(component=f"{self.pipeline_code}.extract_activity_properties")
        activity_id = record.get("activity_id")

        # Проверка наличия activity_properties (совместимость с ChEMBL < v24)
        if "activity_properties" not in record:
            log.warning(
                "activity_properties_missing",
                activity_id=activity_id,
                message="activity_properties not found in API response (possible ChEMBL < v24)",
            )
            record["activity_properties"] = None
            return record

        properties = record["activity_properties"]
        if properties is None:
            log.debug(
                "activity_properties_null",
                activity_id=activity_id,
                message="activity_properties is None (possible ChEMBL < v24)",
            )
            return record

        # Parse JSON string if needed
        if isinstance(properties, str):
            try:
                properties = json.loads(properties)
            except (TypeError, ValueError, json.JSONDecodeError) as exc:
                log.debug(
                    "activity_properties_parse_failed",
                    error=str(exc),
                    activity_id=record.get("activity_id"),
                )
                return record

        # Handle list of properties
        if not isinstance(properties, Sequence) or isinstance(properties, (str, bytes)):
            return record

        properties_seq: Sequence[Any] = cast(Sequence[Any], properties)

        # Helper for fallback assignment
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
        items: list[Mapping[str, Any]] = [
            cast(Mapping[str, Any], p)
            for p in properties_seq  # type: ignore[assignment]
            if isinstance(p, Mapping)
            and ("type" in p)
            and ("value" in p or "text_value" in p)
        ]

        # Helper to check if property represents measured result (result_flag == 1 or True)
        def _is_measured(p: Mapping[str, Any]) -> bool:
            rf = p.get("result_flag")
            return (rf is True) or (isinstance(rf, int) and rf == 1)

        # Sort: measured results (result_flag == 1) first, then others
        items.sort(key=lambda p: (not _is_measured(p)))

        # Process items: extract TRUV fields, standard_* fields, ranges, and comments as fallback
        for prop in items:
            val = prop.get("value")
            txt = prop.get("text_value")
            rel = prop.get("relation")
            unt = prop.get("units")
            prop_type = str(prop.get("type", "")).lower()

            # Проверяем, будем ли мы заполнять value или text_value
            will_set_value = val is not None and record.get("value") is None
            will_set_text_value = txt is not None and record.get("text_value") is None

            # Fallback для original/TRUV полей
            _set_fallback("value", val)
            _set_fallback("text_value", txt)

            # Согласованное подтягивание сопряжённых полей из того же элемента
            # Если заполняем value или text_value, подтягиваем relation и units (если они есть)
            if will_set_value or will_set_text_value:
                _set_fallback("relation", rel)
                _set_fallback("units", unt)

            # Fallback для units из свойства (если еще не заполнено)
            if unt is not None and record.get("units") is None:
                _set_fallback("units", unt)

            # Fallback для upper_value (по типам "upper", "upper_value", "upper limit")
            if record.get("upper_value") is None and (
                "upper" in prop_type or prop_type in ("upper_value", "upper limit")
            ):
                if val is not None:
                    _set_fallback("upper_value", val)

            # Fallback для lower_value (по типам "lower", "lower_value", "lower limit")
            if record.get("lower_value") is None and (
                "lower" in prop_type or prop_type in ("lower_value", "lower limit")
            ):
                if val is not None:
                    _set_fallback("lower_value", val)

            # Fallback для standard_upper_value (по типам "standard_upper", "standard upper", "standard upper value")
            if record.get("standard_upper_value") is None and (
                "standard_upper" in prop_type
                or prop_type in ("standard upper", "standard upper value")
            ):
                if val is not None:
                    _set_fallback("standard_upper_value", val)

            # Fallback для standard_text_value (по типам с подстрокой "standard" + "text")
            if record.get("standard_text_value") is None and "standard" in prop_type and "text" in prop_type:
                if txt is not None:
                    _set_fallback("standard_text_value", txt)
                elif val is not None:
                    _set_fallback("standard_text_value", val)

        # Fallback для data_validity_comment: извлечь из activity_properties если поле пустое
        current_comment = record.get("data_validity_comment")
        if _is_empty(current_comment):
            # Найти элементы с type содержащим "data_validity" или "validity" и наличием хотя бы одного из: text_value или value
            data_validity_items: list[Mapping[str, Any]] = [
                prop
                for prop in items
                if ("data_validity" in str(prop.get("type", "")).lower() or "validity" in str(prop.get("type", "")).lower())
                and (prop.get("text_value") is not None or prop.get("value") is not None)
            ]

            if data_validity_items:
                # Приоритет: элементы с result_flag == 1 (измеренные результаты)
                measured_items = [p for p in data_validity_items if _is_measured(p)]
                if measured_items:
                    # Использовать первый элемент с result_flag == 1
                    # Приоритет: text_value если есть и не пустой, иначе value
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
                        log.debug(
                            "data_validity_comment_fallback_applied",
                            activity_id=record.get("activity_id"),
                            source="activity_properties",
                            priority="measured",
                            comment_value=comment_value,
                        )
                else:
                    # Использовать первый найденный элемент
                    # Приоритет: text_value если есть и не пустой, иначе value
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
                        log.debug(
                            "data_validity_comment_fallback_applied",
                            activity_id=record.get("activity_id"),
                            source="activity_properties",
                            priority="first",
                            comment_value=comment_value,
                        )
            else:
                # Логирование случая, когда activity_properties есть, но data_validity_comment не найден
                log.debug(
                    "data_validity_comment_fallback_no_items",
                    activity_id=record.get("activity_id"),
                    activity_properties_count=len(items),
                    has_activity_properties=True,
                )
        else:
            # Логирование случая, когда data_validity_comment уже заполнен из прямого поля API
            log.debug(
                "data_validity_comment_from_api",
                activity_id=record.get("activity_id"),
                comment_value=current_comment,
            )

        # Нормализация и сохранение activity_properties для последующей сериализации
        # Сохраняем все свойства без фильтрации (только валидация TRUV-формата)
        normalized_properties = self._normalize_activity_properties_items(properties_seq, log)
        if normalized_properties is not None:
            # Валидация TRUV-формата и логирование некорректных свойств
            validated_properties, validation_stats = self._validate_activity_properties_truv(
                normalized_properties, log, activity_id
            )
            # Дедупликация точных дубликатов
            deduplicated_properties, dedup_stats = self._deduplicate_activity_properties(
                validated_properties, log, activity_id
            )
            # Сохраняем нормализованные и дедуплицированные свойства
            record["activity_properties"] = deduplicated_properties
            # Логирование статистики
            log.debug(
                "activity_properties_processed",
                activity_id=activity_id,
                original_count=len(properties_seq),
                normalized_count=len(normalized_properties),
                validated_count=len(validated_properties),
                deduplicated_count=len(deduplicated_properties),
                invalid_count=validation_stats.get("invalid_count", 0),
                duplicates_removed=dedup_stats.get("duplicates_removed", 0),
            )
        else:
            # Если нормализация не удалась, сохраняем исходные свойства
            record["activity_properties"] = properties
            log.debug(
                "activity_properties_normalization_failed",
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
    def _extract_page_items(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
        candidates: list[dict[str, Any]] = []
        for key in ("activities", "data", "items", "results"):
            value: Any = payload.get(key)
            if isinstance(value, Sequence):
                candidates = [
                    cast(dict[str, Any], item)  # pyright: ignore[reportUnknownVariableType]
                    for item in value  # pyright: ignore[reportUnknownVariableType]
                    if isinstance(item, Mapping)
                ]
                if candidates:
                    return candidates
        for key, value in payload.items():
            if key == "page_meta":
                continue
            if isinstance(value, Sequence):
                candidates = [
                    cast(dict[str, Any], item)  # pyright: ignore[reportUnknownVariableType]
                    for item in value  # pyright: ignore[reportUnknownVariableType]
                    if isinstance(item, Mapping)
                ]
                if candidates:
                    return candidates
        return []

    @staticmethod
    def _next_link(payload: Mapping[str, Any], base_url: str) -> str | None:
        page_meta: Any = payload.get("page_meta")
        if isinstance(page_meta, Mapping):
            next_link_raw: Any = page_meta.get("next")  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
            next_link: str | None = cast(str | None, next_link_raw) if next_link_raw is not None else None
            if isinstance(next_link, str) and next_link:
                # urlparse returns ParseResult with str path when input is str
                base_path_parse_result = urlparse(base_url)
                base_path: str = str(base_path_parse_result.path).rstrip("/")  # type: ignore[assignment]

                # If next_link is a full URL, extract only the relative path
                if next_link.startswith("http://") or next_link.startswith("https://"):
                    parsed = urlparse(next_link)
                    base_parsed = urlparse(base_url)

                    # Get paths - urlparse returns str path when input is str
                    path: str = str(parsed.path)  # type: ignore[assignment]
                    base_path_from_url: str = str(base_parsed.path)  # type: ignore[assignment]

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

    def _harmonize_identifier_columns(self, df: pd.DataFrame, log: Any) -> pd.DataFrame:
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

        required_columns = ["activity_id", "assay_chembl_id", "testitem_chembl_id", "molecule_chembl_id"]
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
            log.debug("identifier_harmonization", actions=actions)

        return df

    def _schema_column_specs(self) -> Mapping[str, Mapping[str, Any]]:
        specs = dict(super()._schema_column_specs())
        boolean_columns = ("potential_duplicate", "curated", "removed")
        for column in boolean_columns:
            specs[column] = {"dtype": "boolean", "default": pd.NA}
        return specs

    def _normalize_identifiers(self, df: pd.DataFrame, log: Any) -> pd.DataFrame:
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
            log.debug(
                "identifiers_normalized",
                normalized_count=stats.normalized,
                invalid_count=stats.invalid,
                columns=list(stats.per_column.keys()),
            )

        return normalized_df

    def _finalize_identifier_columns(self, df: pd.DataFrame, log: Any) -> pd.DataFrame:
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
                log.warning(
                    "identifier_mismatch",
                    count=mismatch_count,
                    samples=samples,
                )
                df.loc[mismatch_mask, "testitem_chembl_id"] = df.loc[mismatch_mask, "molecule_chembl_id"]

        required_columns = ["activity_id", "assay_chembl_id", "testitem_chembl_id", "molecule_chembl_id"]
        missing_columns = [column for column in required_columns if column not in df.columns]
        if missing_columns:
            for column in missing_columns:
                if column == "testitem_chembl_id" and "molecule_chembl_id" in df.columns:
                    # testitem_chembl_id should be equal to molecule_chembl_id if missing
                    df[column] = df["molecule_chembl_id"].copy()
                else:
                    df[column] = pd.Series([None] * len(df), dtype="object")
            log.warning("identifier_columns_missing", columns=missing_columns)

        return df

    def _finalize_output_columns(self, df: pd.DataFrame, log: Any) -> pd.DataFrame:
        """Align final column order with schema and drop unexpected fields."""

        df = df.copy()
        expected = list(COLUMN_ORDER)

        extras = [column for column in df.columns if column not in expected]
        if extras:
            df = df.drop(columns=extras)
            log.debug("output_columns_dropped", columns=extras)

        missing = [column for column in expected if column not in df.columns]
        if missing:
            for column in missing:
                if column == "testitem_chembl_id" and "molecule_chembl_id" in df.columns:
                    # testitem_chembl_id should be equal to molecule_chembl_id if missing
                    df[column] = df["molecule_chembl_id"].copy()
                else:
                    df[column] = pd.Series([pd.NA] * len(df), dtype="object")
            log.warning("output_columns_missing", columns=missing)

        if not expected:
            return df

        return df[expected]

    def _filter_invalid_required_fields(self, df: pd.DataFrame, log: Any) -> pd.DataFrame:
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

        required_fields = ["assay_chembl_id",  "molecule_chembl_id"]
        missing_fields = [field for field in required_fields if field not in df.columns]
        if missing_fields:
            log.warning(
                "filter_skipped_missing_columns",
                missing_columns=missing_fields,
                message="Cannot filter: required columns are missing",
            )
            return df

        # Create mask for rows with all required fields populated
        valid_mask = (
            df["assay_chembl_id"].notna()  & df["molecule_chembl_id"].notna()
        )

        invalid_count = int((~valid_mask).sum())
        if invalid_count > 0:
            # Log sample of invalid rows for debugging
            invalid_rows = df[~valid_mask]
            sample_size = min(5, len(invalid_rows))
            sample_activity_ids = (
                invalid_rows["activity_id"].head(sample_size).tolist() if "activity_id" in invalid_rows.columns else []
            )
            log.warning(
                "filtered_invalid_rows",
                filtered_count=invalid_count,
                remaining_count=int(valid_mask.sum()),
                sample_activity_ids=sample_activity_ids,
                message="Rows with NULL in required identifier fields were filtered out",
            )
            df = df[valid_mask].reset_index(drop=True)

        return df

    def _normalize_measurements(self, df: pd.DataFrame, log: Any) -> pd.DataFrame:
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
                    log.warning("negative_standard_value", count=int(negative_mask.sum()))
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
                    log.warning("invalid_standard_relation", count=int(invalid_mask.sum()))
                    df.loc[invalid_mask, "standard_relation"] = None
                normalized_count += int(mask.sum())

        if "standard_type" in df.columns:
            mask = df["standard_type"].notna()
            if mask.any():
                df.loc[mask, "standard_type"] = (
                    df.loc[mask, "standard_type"].astype(str).str.strip()
                )
                standard_types_set: set[str] = STANDARD_TYPES
                invalid_mask = mask & ~df["standard_type"].isin(standard_types_set)  # pyright: ignore[reportUnknownMemberType]
                if invalid_mask.any():
                    log.warning("invalid_standard_type", count=int(invalid_mask.sum()))
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
                    log.warning("invalid_relation", count=int(invalid_mask.sum()))
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
                    log.warning("negative_standard_upper_value", count=int(negative_mask.sum()))
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
                    log.warning("negative_upper_value", count=int(negative_mask.sum()))
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
                    log.warning("negative_lower_value", count=int(negative_mask.sum()))
                    df.loc[negative_mask, "lower_value"] = None
                normalized_count += int(mask.sum())

        if normalized_count > 0:
            log.debug("measurements_normalized", normalized_count=normalized_count)

        return df

    def _normalize_string_fields(self, df: pd.DataFrame, log: Any) -> pd.DataFrame:
        """Normalize string fields: trim, empty string to null, title-case for organism."""

        working_df = df.copy()

        # Проверка инварианта: data_validity_description заполнено при data_validity_comment = NA
        if "data_validity_description" in working_df.columns and "data_validity_comment" in working_df.columns:
            invalid_mask = working_df["data_validity_description"].notna() & working_df["data_validity_comment"].isna()
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
            log.debug(
                "string_fields_normalized",
                columns=list(stats.per_column.keys()),
                rows_processed=stats.processed,
            )

        return normalized_df

    def _normalize_nested_structures(self, df: pd.DataFrame, log: Any) -> pd.DataFrame:
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
                            log.warning(
                                "nested_serialization_failed",
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

        # Диагностическое логирование для ligand_efficiency
        if "standard_value" in df.columns and "ligand_efficiency" in df.columns:
            mask = df["standard_value"].notna() & df["ligand_efficiency"].isna()
            if mask.any():
                log.warning(
                    "ligand_efficiency_missing_with_standard_value",
                    count=int(mask.sum()),
                    message="ligand_efficiency is empty while standard_value exists",
                )

        return df

    def _serialize_activity_properties(self, value: Any, log: Any | None = None) -> str | None:
        """Return normalized JSON for activity_properties or None if not serializable."""

        normalized_items = self._normalize_activity_properties_items(value, log)
        if normalized_items is None:
            return None
        try:
            return json.dumps(normalized_items, ensure_ascii=False, sort_keys=True)
        except (TypeError, ValueError) as exc:
            if log is not None:
                log.warning("activity_properties_serialization_failed", error=str(exc))
            return None

    def _normalize_activity_properties_items(
        self, value: Any, log: Any | None = None
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
                log.warning(
                    "activity_properties_unhandled_type",
                    value_type=type(raw_value).__name__,
                )
            return None

        normalized: list[dict[str, Any]] = []
        for item in items:
            if item is None:
                continue
            if isinstance(item, Mapping):
                # Приводим Mapping к dict для явной типизации
                item_mapping = cast(Mapping[str, Any], item)
                normalized_item: dict[str, Any | None] = {key: item_mapping.get(key) for key in ACTIVITY_PROPERTY_KEYS}
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
                    log.warning(
                        "activity_properties_item_unhandled",
                        item_type=type(item).__name__,
                    )

        return normalized

    def _validate_activity_properties_truv(
        self,
        properties: list[dict[str, Any]],
        log: Any,
        activity_id: Any | None = None,
    ) -> tuple[list[dict[str, Any]], dict[str, int]]:
        """Валидация TRUV-формата для activity_properties.

        Валидирует инварианты:
        - value IS NOT NULL ⇒ text_value IS NULL (и наоборот)
        - relation IN ('=', '<', '≤', '>', '≥', '~') OR NULL

        Parameters
        ----------
        properties:
            Список нормализованных свойств для валидации.
        log:
            Logger instance.
        activity_id:
            ID активности для логирования (опционально).

        Returns
        -------
        tuple[list[dict[str, Any]], dict[str, int]]:
            Кортеж из валидированных свойств и статистики валидации.
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

            # Валидация инварианта: value IS NOT NULL ⇒ text_value IS NULL (и наоборот)
            if value is not None and text_value is not None:
                is_valid = False
                validation_errors.append("both value and text_value are not None")
            elif value is None and text_value is None:
                # Оба могут быть None, это допустимо
                pass

            # Валидация relation: должен быть в допустимых значениях или NULL
            if relation is not None:
                if not isinstance(relation, str):
                    is_valid = False
                    validation_errors.append(f"relation is not a string: {type(relation).__name__}")
                elif relation not in RELATIONS:
                    is_valid = False
                    validation_errors.append(f"relation '{relation}' not in allowed values: {RELATIONS}")

            # Сохраняем все свойства без фильтрации (только логируем некорректные)
            validated.append(prop)
            if not is_valid:
                invalid_count += 1
                invalid_items.append(prop)
                log.warning(
                    "activity_property_truv_validation_failed",
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
        log: Any,
        activity_id: Any | None = None,
    ) -> tuple[list[dict[str, Any]], dict[str, int]]:
        """Дедупликация точных дубликатов в activity_properties.

        Удаляет точные дубликаты по всем полям: type, relation, units, value, text_value.
        Сохраняет порядок свойств (первое вхождение при дедупликации).

        Parameters
        ----------
        properties:
            Список валидированных свойств для дедупликации.
        log:
            Logger instance.
        activity_id:
            ID активности для логирования (опционально).

        Returns
        -------
        tuple[list[dict[str, Any]], dict[str, int]]:
            Кортеж из дедуплицированных свойств и статистики дедупликации.
        """
        seen: set[tuple[Any, ...]] = set()
        deduplicated: list[dict[str, Any]] = []
        duplicates_removed = 0

        for prop in properties:
            # Создаем ключ для дедупликации из всех полей свойства
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
                log.debug(
                    "activity_property_duplicate_removed",
                    activity_id=activity_id,
                    property=prop,
                    message="Exact duplicate removed",
                )

        stats = {
            "duplicates_removed": duplicates_removed,
            "deduplicated_count": len(deduplicated),
        }

        return deduplicated, stats

    def _add_row_metadata(self, df: pd.DataFrame, log: Any) -> pd.DataFrame:
        """Add required row metadata fields (row_subtype, row_index)."""

        df = df.copy()
        if df.empty:
            return df

        # Add row_subtype: "activity" for all rows
        if "row_subtype" not in df.columns:
            df["row_subtype"] = "activity"
            log.debug("row_subtype_added", value="activity")
        elif df["row_subtype"].isna().all():
            df["row_subtype"] = "activity"
            log.debug("row_subtype_filled", value="activity")

        # Add row_index: sequential index starting from 0
        if "row_index" not in df.columns:
            df["row_index"] = range(len(df))
            log.debug("row_index_added", count=len(df))
        elif df["row_index"].isna().all():
            df["row_index"] = range(len(df))
            log.debug("row_index_filled", count=len(df))

        return df

    def _normalize_data_types(self, df: pd.DataFrame, schema: Any, log: Any) -> pd.DataFrame:
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
                log.warning("type_conversion_failed", field=field, error=str(exc))

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
                            log.warning(
                                "invalid_positive_integer",
                                field=field,
                                count=int(invalid_mask.sum()),
                            )
                            df.loc[invalid_mask, field] = pd.NA
            except (ValueError, TypeError) as exc:
                log.warning("type_conversion_failed", field=field, error=str(exc))

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
                log.warning("type_conversion_failed", field=field, error=str(exc))

        for field in bool_fields:
            if field not in df.columns:
                continue
            try:
                # Для curated и removed: если уже boolean, оставляем как есть
                if field in ("curated", "removed") and df[field].dtype == "boolean":
                    continue
                # Конвертируем в boolean, сохраняя NA значения
                if field in ("curated", "removed"):
                    # Конвертируем напрямую в boolean dtype, сохраняя NA
                    df[field] = df[field].astype("boolean")  # pyright: ignore[reportUnknownMemberType]
                else:
                    # Для остальных bool полей используем стандартную логику
                    bool_numeric_series: pd.Series[Any] = pd.to_numeric(  # pyright: ignore[reportUnknownMemberType]
                        df[field], errors="coerce"
                    )
                    df[field] = (bool_numeric_series != 0).astype("boolean")  # pyright: ignore[reportUnknownMemberType]
            except (ValueError, TypeError) as exc:
                log.warning("bool_conversion_failed", field=field, error=str(exc))

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
                        log.warning(
                            "invalid_standard_flag",
                            field=field,
                            count=int(invalid_valid_mask.sum()),
                        )
                        df.loc[invalid_index, field] = pd.NA
            except (ValueError, TypeError) as exc:
                log.warning("type_conversion_failed", field=field, error=str(exc))

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

    def _validate_foreign_keys(self, df: pd.DataFrame, log: Any) -> pd.DataFrame:
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
            log.warning("foreign_key_validation", warnings=warnings)

        return df

    def _check_activity_id_uniqueness(self, df: pd.DataFrame, log: Any) -> None:
        """Check uniqueness of activity_id before validation."""

        if "activity_id" not in df.columns:
            log.warning("activity_id_uniqueness_check_skipped", reason="column_not_found")
            return

        duplicates = df[df["activity_id"].duplicated(keep=False)]
        if not duplicates.empty:
            duplicate_count = len(duplicates)
            duplicate_ids = duplicates["activity_id"].unique().tolist()
            log.error(
                "activity_id_duplicates_found",
                duplicate_count=duplicate_count,
                duplicate_ids=duplicate_ids[:10],  # Log first 10 duplicates
                total_duplicate_ids=len(duplicate_ids),
            )
            msg = (
                f"Found {duplicate_count} duplicate activity_id value(s): "
                f"{duplicate_ids[:5]}{'...' if len(duplicate_ids) > 5 else ''}"
            )
            raise ValueError(msg)

        log.debug("activity_id_uniqueness_verified", unique_count=df["activity_id"].nunique())

    def _validate_data_validity_comment_soft_enum(self, df: pd.DataFrame, log: Any) -> None:
        """Soft enum валидация для data_validity_comment.

        Проверяет значения против whitelist из конфига. Неизвестные значения
        логируются как warning, но не блокируют валидацию (soft enum).

        Parameters
        ----------
        df:
            DataFrame для валидации.
        log:
            Logger instance.
        """
        if df.empty or "data_validity_comment" not in df.columns:
            return

        whitelist = self._get_data_validity_comment_whitelist()
        if not whitelist:
            return

        non_null_comments = df["data_validity_comment"].dropna()
        if len(non_null_comments) == 0:
            return

        whitelist_set = set(whitelist)
        unknown_mask = ~non_null_comments.isin(whitelist_set)  # type: ignore[reportUnknownArgumentType]
        unknown_count = int(unknown_mask.sum())

        if unknown_count > 0:
            unknown_values = non_null_comments[unknown_mask].value_counts().head(10).to_dict()  # type: ignore[reportUnknownArgumentType]
            log.warning(
                "soft_enum_unknown_data_validity_comment",
                unknown_count=unknown_count,
                total_count=len(non_null_comments),
                samples=unknown_values,
                whitelist=whitelist,
                message="Unknown data_validity_comment values detected (soft enum: not blocking)",
            )

    def _check_foreign_key_integrity(self, df: pd.DataFrame, log: Any) -> None:
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
                log.debug(
                    "foreign_key_integrity_check_skipped", field=field, reason="column_not_found"
                )
                continue

            mask = df[field].notna()
            if not mask.any():
                log.debug("foreign_key_integrity_check_skipped", field=field, reason="all_null")
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
                log.warning(
                    "foreign_key_integrity_invalid",
                    field=field,
                    invalid_count=invalid_count,
                    samples=invalid_samples,
                )

        if errors:
            log.error("foreign_key_integrity_check_failed", errors=errors)
            msg = f"Foreign key integrity check failed: {'; '.join(errors)}"
            raise ValueError(msg)

        log.debug("foreign_key_integrity_verified")

    def _log_detailed_validation_errors(
        self,
        failure_cases: pd.DataFrame,
        payload: pd.DataFrame,
        log: Any,
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

            log.error("validation_error_detail", **error_details)

        if len(failure_cases) > max_errors:
            log.warning(
                "validation_errors_truncated",
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
            return super().write(df, output_path, extended=extended)

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

            log.debug(
                "write_sort_config_set",
                sort_keys=sort_keys,
                original_sort_keys=list(original_sort_by) if original_sort_by else [],
            )

            # Temporarily replace config
            original_config = self.config
            self.config = modified_config

            try:
                result = super().write(df, output_path, extended=extended)
            finally:
                # Restore original config
                self.config = original_config

            return result

        # If sort config already matches, proceed normally
        return super().write(df, output_path, extended=extended)
