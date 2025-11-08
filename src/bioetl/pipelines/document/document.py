"""Document pipeline implementation for ChEMBL."""

from __future__ import annotations

import re
import time
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from numbers import Integral, Real
from typing import Any, cast

import pandas as pd

from bioetl.clients.chembl import ChemblClient
from bioetl.config import DocumentSourceConfig, PipelineConfig
from bioetl.core import UnifiedLogger
from bioetl.core.normalizers import StringRule, normalize_string_columns
from bioetl.schemas.document import COLUMN_ORDER

from ..chembl_base import ChemblPipelineBase
from .document_enrich import enrich_with_document_terms

API_DOCUMENT_FIELDS: tuple[str, ...] = (
    "document_chembl_id",
    "doc_type",
    "journal",
    "journal_full_title",
    "doi",
    "src_id",
    "title",
    "abstract",
    "year",
    "volume",
    "issue",
    "first_page",
    "last_page",
    "pubmed_id",
    "authors",
)

# Обязательные поля, которые всегда должны быть в запросе к API
MUST_HAVE_FIELDS = {"document_chembl_id", "doi", "issue"}


class ChemblDocumentPipeline(ChemblPipelineBase):
    """ETL pipeline extracting document records from the ChEMBL API."""

    actor = "document_chembl"

    def __init__(self, config: PipelineConfig, run_id: str) -> None:
        super().__init__(config, run_id)
        self._last_batch_extract_stats: dict[str, Any] | None = None

    def extract(self, *args: object, **kwargs: object) -> pd.DataFrame:
        """Fetch document payloads from ChEMBL using the unified HTTP client.

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
                log.info("chembl_document.extract_mode", mode="batch", ids_count=len(ids))
                return self.extract_by_ids(ids)

        log.info("chembl_document.extract_mode", mode="full")
        return self.extract_all()

    def extract_all(self) -> pd.DataFrame:
        """Extract all document records from ChEMBL using pagination."""

        log = UnifiedLogger.get(__name__).bind(component=f"{self.pipeline_code}.extract")
        stage_start = time.perf_counter()

        source_raw = self._resolve_source_config("chembl")
        source_config = DocumentSourceConfig.from_source_config(source_raw)
        base_url = self._resolve_base_url(cast(Mapping[str, Any], dict(source_config.parameters)))
        http_client, _ = self.prepare_chembl_client(
            "chembl", base_url=base_url, client_name="chembl_document_client"
        )

        chembl_client = ChemblClient(
            http_client,
            load_meta_store=self.load_meta_store,
            job_id=self.run_id,
            operator=self.pipeline_code,
        )
        self.perform_source_handshake(
            chembl_client,
            source_config=source_config,
            log=log,
            event="chembl_document.handshake",
        )

        limit = self.config.cli.limit
        page_size = self._resolve_page_size(source_config.page_size, limit)

        select_fields = self._resolve_select_fields(
            source_raw, default_fields=list(API_DOCUMENT_FIELDS)
        )
        # Защита: добавить обязательные поля, если их нет
        select_fields = list(dict.fromkeys(list(select_fields) + list(MUST_HAVE_FIELDS)))
        records: list[dict[str, Any]] = []
        next_endpoint: str | None = "/document.json"
        params: Mapping[str, Any] | None = {
            "limit": page_size,
            "only": ",".join(select_fields),
        }
        parameter_filters = source_config.parameters.model_dump()
        filters_payload: dict[str, Any] = {
            "mode": "all",
            "limit": int(limit) if limit is not None else None,
            "page_size": page_size,
            "select_fields": list(select_fields),
            "max_url_length": source_config.max_url_length,
        }
        if parameter_filters:
            filters_payload["parameters"] = parameter_filters
        compact_filters = {
            key: value for key, value in filters_payload.items() if value is not None
        }
        self.record_extract_metadata(
            chembl_release=self.chembl_release,
            filters=compact_filters,
            requested_at_utc=datetime.now(timezone.utc),
        )
        pages = 0

        while next_endpoint:
            page_start = time.perf_counter()
            response = http_client.get(next_endpoint, params=params)
            payload = self._coerce_mapping(response.json())
            page_items = self._extract_page_items(
                payload, items_keys=("documents", "data", "items", "results")
            )

            # Process nested fields for each item
            processed_items: list[dict[str, Any]] = []
            for item in page_items:
                processed_item = self._extract_nested_fields(dict(item))
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
                "chembl_document.page_fetched",
                endpoint=next_endpoint,
                batch_size=len(processed_items),
                total_records=len(records),
                duration_ms=page_duration_ms,
            )

            next_link = self._next_link(payload, base_url)
            if not next_link or (limit is not None and len(records) >= limit):
                break
            log.info(
                "chembl_document.next_link_resolved",
                next_link=next_link,
                base_url=base_url,
            )
            next_endpoint = next_link
            params = None

        dataframe: pd.DataFrame = pd.DataFrame.from_records(records)  # pyright: ignore[reportUnknownMemberType]; type: ignore
        if dataframe.empty:
            dataframe = pd.DataFrame({"document_chembl_id": pd.Series(dtype="string")})
        elif "document_chembl_id" in dataframe.columns:
            dataframe = dataframe.sort_values("document_chembl_id").reset_index(drop=True)

        duration_ms = (time.perf_counter() - stage_start) * 1000.0
        log.info(
            "chembl_document.extract_summary",
            rows=int(dataframe.shape[0]),
            duration_ms=duration_ms,
            chembl_release=self.chembl_release,
            pages=pages,
        )
        return dataframe

    def extract_by_ids(self, ids: Sequence[str]) -> pd.DataFrame:
        """Extract document records by a specific list of IDs using batch extraction.

        Parameters
        ----------
        ids:
            Sequence of document_chembl_id values to extract (as strings).

        Returns
        -------
        pd.DataFrame:
            DataFrame containing extracted document records.
        """
        log = UnifiedLogger.get(__name__).bind(component=f"{self.pipeline_code}.extract")
        stage_start = time.perf_counter()

        source_raw = self._resolve_source_config("chembl")
        source_config = DocumentSourceConfig.from_source_config(source_raw)
        base_url = self._resolve_base_url(cast(Mapping[str, Any], dict(source_config.parameters)))
        http_client, _ = self.prepare_chembl_client(
            "chembl", base_url=base_url, client_name="chembl_document_client"
        )

        chembl_client = ChemblClient(
            http_client,
            load_meta_store=self.load_meta_store,
            job_id=self.run_id,
            operator=self.pipeline_code,
        )
        self.perform_source_handshake(
            chembl_client,
            source_config=source_config,
            log=log,
            event="chembl_document.handshake",
        )

        source_raw = self._resolve_source_config("chembl")
        source_config = DocumentSourceConfig.from_source_config(source_raw)
        select_fields = self._resolve_select_fields(
            source_raw, default_fields=list(API_DOCUMENT_FIELDS)
        )
        # Защита: добавить обязательные поля, если их нет
        select_fields = list(dict.fromkeys(list(select_fields) + list(MUST_HAVE_FIELDS)))

        id_filters = {
            "mode": "ids",
            "requested_ids": [str(value) for value in ids],
            "limit": int(self.config.cli.limit) if self.config.cli.limit is not None else None,
            "page_size": source_config.page_size,
            "max_url_length": source_config.max_url_length,
            "select_fields": list(select_fields),
        }
        compact_id_filters = {key: value for key, value in id_filters.items() if value is not None}
        self.record_extract_metadata(
            chembl_release=self.chembl_release,
            filters=compact_id_filters,
            requested_at_utc=datetime.now(timezone.utc),
        )

        batch_dataframe: pd.DataFrame = self.extract_ids_paginated(
            ids,
            endpoint="/document.json",
            id_column="document_chembl_id",
            id_param_name="document_chembl_id__in",
            client=http_client,
            page_size=source_config.page_size,
            max_url_length=source_config.max_url_length,
            limit=self.config.cli.limit,
            select_fields=select_fields,
            items_keys=("documents", "data", "items", "results"),
            process_item=self._extract_nested_fields,
        )

        duration_ms = (time.perf_counter() - stage_start) * 1000.0
        batch_stats = self._last_batch_extract_stats or {}
        log.info(
            "chembl_document.extract_by_ids_summary",
            rows=int(batch_dataframe.shape[0]),
            requested=len(ids),
            duration_ms=duration_ms,
            chembl_release=self.chembl_release,
            batches=batch_stats.get("batches"),
            api_calls=batch_stats.get("api_calls"),
            cache_hits=batch_stats.get("cache_hits"),
        )
        return batch_dataframe

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform raw document data by normalizing fields and identifiers."""

        log = UnifiedLogger.get(__name__).bind(component=f"{self.pipeline_code}.transform")

        df = df.copy()

        if df.empty:
            log.debug("transform_empty_dataframe")
            return df

        log.info("transform_started", rows=len(df))

        # Normalize identifiers
        df = self._normalize_identifiers(df, log)

        # Normalize string fields
        df = self._normalize_string_fields(df, log)

        # Normalize numeric fields
        df = self._normalize_numeric_fields(df, log)

        # Enrichment: document_term fields
        if self._should_enrich_document_terms():
            df = self._enrich_document_terms(df)

        # Add system fields
        df = self._add_system_fields(df, log)

        # Ensure schema columns
        df = self._ensure_schema_columns(df, COLUMN_ORDER, log)

        # Deduplicate by document_chembl_id before validation
        # Schema requires unique document_chembl_id, so we need to remove duplicates
        # Use deterministic deduplication: sort by all columns, then keep first occurrence
        if "document_chembl_id" in df.columns and df["document_chembl_id"].duplicated().any():
            initial_count = len(df)
            # Sort by all columns for deterministic deduplication
            df = df.sort_values(by=list(df.columns)).drop_duplicates(
                subset=["document_chembl_id"], keep="first"
            )
            deduped_count = len(df)
            if deduped_count < initial_count:
                log.warning(
                    "document_deduplication_applied",
                    initial_count=initial_count,
                    deduped_count=deduped_count,
                    removed_count=initial_count - deduped_count,
                )

        # Order columns according to schema
        df = self._order_schema_columns(df, COLUMN_ORDER)

        log.info("transform_completed", rows=len(df))
        return df

    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate payload against DocumentSchema with detailed error handling."""

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

        # Pre-validation checks
        self._check_document_id_uniqueness(df, log)

        # Call base validation
        validated = super().validate(df)
        log.info(
            "validate_completed",
            rows=len(validated),
            schema=self.config.validation.schema_out,
            strict=self.config.validation.strict,
            coerce=self.config.validation.coerce,
        )
        return validated

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _normalize_identifiers(self, df: pd.DataFrame, log: Any) -> pd.DataFrame:
        """Normalize identifier fields (DOI, PMID)."""
        df = df.copy()

        # Normalize DOI
        if "doi" in df.columns:
            df["doi_clean"] = df["doi"].apply(self._normalize_doi)  # pyright: ignore[reportUnknownMemberType]

        # Normalize PMID
        if "pubmed_id" in df.columns:
            df["pubmed_id"] = pd.to_numeric(df["pubmed_id"], errors="coerce").astype("Int64")  # pyright: ignore[reportUnknownMemberType]

        return df

    @staticmethod
    def _normalize_doi(doi: str | None) -> str:
        """Normalize DOI by removing prefixes and validating format."""
        if not doi:
            return ""
        if not isinstance(doi, str):  # pyright: ignore[reportUnnecessaryIsInstance]
            return ""
        doi = doi.strip().lower()
        # Remove prefixes
        for prefix in ["doi:", "https://doi.org/", "http://dx.doi.org/", "http://doi.org/"]:
            if doi.startswith(prefix):
                doi = doi[len(prefix) :]
        doi = doi.strip()
        # Validate regex
        doi_pattern = re.compile(r"^10\.\d{4,9}/\S+$")
        if doi_pattern.match(doi):
            return doi
        return ""

    def _normalize_string_fields(self, df: pd.DataFrame, log: Any) -> pd.DataFrame:
        """Normalize string fields (title, abstract, journal, authors)."""
        working_df = df.copy()

        rules = {
            "title": StringRule(max_length=1000),
            "abstract": StringRule(max_length=5000),
        }

        normalized_df, stats = normalize_string_columns(working_df, rules, copy=False)

        if stats.has_changes:
            log.debug(
                "string_fields_normalized",
                columns=list(stats.per_column.keys()),
                rows_processed=stats.processed,
            )

        # Normalize journal
        if "journal" in normalized_df.columns:
            journal_series: pd.Series[Any] = normalized_df["journal"]
            normalized_df["journal"] = journal_series.map(
                lambda value: self._normalize_journal(value)
            )

        # Normalize authors
        if "authors" in normalized_df.columns:
            def _to_author_tuple(item: object) -> tuple[str, int] | None:
                if not isinstance(item, tuple):
                    return None
                tuple_item = cast(tuple[object, ...], item)
                if len(tuple_item) != 2:
                    return None
                name_raw, count_raw = tuple_item
                if not isinstance(name_raw, str):
                    return None
                name_value: str = name_raw
                if isinstance(count_raw, Integral):
                    count_value = int(count_raw)
                elif isinstance(count_raw, Real):
                    float_value = float(count_raw)
                    if not float_value.is_integer():
                        return None
                    count_value = int(float_value)
                else:
                    return None
                if count_value < 0:
                    return None
                return (name_value, count_value)

            def _author_name_from_tuple(data: tuple[str, int] | None) -> str:
                return data[0] if data is not None else ""

            def _author_count_from_tuple(data: tuple[str, int] | None) -> int:
                return data[1] if data is not None else 0

            authors_series: pd.Series[Any] = normalized_df["authors"]
            normalized_result = authors_series.map(self._normalize_authors)
            normalized_tuples = normalized_result.map(_to_author_tuple)
            normalized_df["authors"] = normalized_tuples.map(_author_name_from_tuple)
            normalized_df["authors_count"] = normalized_tuples.map(_author_count_from_tuple)

        return normalized_df

    @staticmethod
    def _normalize_journal(value: Any, max_len: int = 255) -> str:
        """Trim and collapse whitespace for journal name."""
        if pd.isna(value):
            return ""
        text = str(value)
        text = re.sub(r"\s+", " ", text).strip()
        return text[:max_len] if len(text) > max_len else text

    @staticmethod
    def _normalize_authors(authors: Any, separator: str = ", ") -> tuple[str, int]:
        """Normalize author separators and count."""
        if pd.isna(authors):
            return ("", 0)
        text = str(authors).strip()
        text = re.sub(r";", ",", text)  # ; → ,
        text = re.sub(r"\s+", " ", text)  # collapse whitespace
        if not text:
            return ("", 0)
        parts = text.split(",")
        parts = [p.strip() for p in parts if p.strip()]
        return (separator.join(parts), len(parts))

    def _normalize_numeric_fields(self, df: pd.DataFrame, log: Any) -> pd.DataFrame:
        """Normalize numeric fields (year)."""
        df = df.copy()

        # Normalize year
        if "year" in df.columns:
            def _coerce_year(value: object) -> int | None:
                if value is None or value is pd.NA:
                    return None
                if isinstance(value, Integral):
                    year_int = int(value)
                elif isinstance(value, Real):
                    float_value = float(value)
                    if not float_value.is_integer():
                        return None
                    year_int = int(float_value)
                elif isinstance(value, str):
                    stripped = value.strip()
                    if not stripped:
                        return None
                    if not stripped.isdigit():
                        return None
                    year_int = int(stripped)
                else:
                    return None

                if 1500 <= year_int <= 2100:
                    return year_int
                return None

            normalized_year = df["year"].map(_coerce_year)
            df["year"] = normalized_year.astype("Int64")

        return df

    def _add_system_fields(self, df: pd.DataFrame, log: Any) -> pd.DataFrame:
        """Add document-specific system fields (source)."""
        df = df.copy()

        # Add source field
        df["source"] = "ChEMBL"

        return df

    def _schema_column_specs(self) -> Mapping[str, Mapping[str, Any]]:
        specs = dict(super()._schema_column_specs())
        specs["source"] = {"default": "ChEMBL"}
        specs["authors_count"] = {"default": 0, "dtype": "Int64"}

        hashing_config = self.config.determinism.hashing
        business_key_column = hashing_config.business_key_column
        row_hash_column = hashing_config.row_hash_column

        if business_key_column:
            specs[business_key_column] = {"default": ""}
        if row_hash_column:
            specs[row_hash_column] = {"default": ""}

        return specs

    def _check_document_id_uniqueness(self, df: pd.DataFrame, log: Any) -> None:
        """Check that document_chembl_id is unique."""
        if df.empty:
            return
        if "document_chembl_id" not in df.columns:
            return
        duplicates = df["document_chembl_id"].duplicated()
        if duplicates.any():
            duplicate_ids = (
                df[df["document_chembl_id"].duplicated()]["document_chembl_id"].unique().tolist()
            )
            log.warning(
                "document_id_duplicates",
                duplicate_count=duplicates.sum(),
                duplicate_ids=duplicate_ids[:10],  # Limit to first 10
            )

    def _should_enrich_document_terms(self) -> bool:
        """Проверить, включено ли обогащение document_term в конфиге."""
        if not self.config.chembl:
            return False
        try:
            chembl_section = self.config.chembl
            document_section: Any = chembl_section.get("document")
            if not isinstance(document_section, Mapping):
                return False
            document_section = cast(Mapping[str, Any], document_section)
            enrich_section: Any = document_section.get("enrich")
            if not isinstance(enrich_section, Mapping):
                return False
            enrich_section = cast(Mapping[str, Any], enrich_section)
            document_term_section: Any = enrich_section.get("document_term")
            if not isinstance(document_term_section, Mapping):
                return False
            document_term_section = cast(Mapping[str, Any], document_term_section)
            enabled: Any = document_term_section.get("enabled")
            return bool(enabled) if enabled is not None else False
        except (AttributeError, KeyError, TypeError):
            return False

    def _enrich_document_terms(self, df: pd.DataFrame) -> pd.DataFrame:
        """Обогатить DataFrame полями из document_term."""
        log = UnifiedLogger.get(__name__).bind(component=f"{self.pipeline_code}.enrich")

        # Получить конфигурацию обогащения
        enrich_cfg: dict[str, Any] = {}
        try:
            if self.config.chembl:
                chembl_section = self.config.chembl
                document_section: Any = chembl_section.get("document")
                if isinstance(document_section, Mapping):
                    document_section = cast(Mapping[str, Any], document_section)
                    enrich_section: Any = document_section.get("enrich")
                    if isinstance(enrich_section, Mapping):
                        enrich_section = cast(Mapping[str, Any], enrich_section)
                        document_term_section: Any = enrich_section.get("document_term")
                        if isinstance(document_term_section, Mapping):
                            document_term_section = cast(Mapping[str, Any], document_term_section)
                            enrich_cfg = dict(document_term_section)
        except (AttributeError, KeyError, TypeError) as exc:
            log.warning(
                "enrichment_config_error",
                error=str(exc),
                message="Using default enrichment config",
            )

        # Создать или переиспользовать клиент ChEMBL
        source_raw = self._resolve_source_config("chembl")
        source_config = DocumentSourceConfig.from_source_config(source_raw)
        api_client, _ = self.prepare_chembl_client(
            "chembl",
            base_url=self._resolve_base_url(
                cast(Mapping[str, Any], dict(source_config.parameters))
            ),
            client_name="chembl_enrichment_client",
        )
        chembl_client = ChemblClient(
            api_client,
            load_meta_store=self.load_meta_store,
            job_id=self.run_id,
            operator=self.pipeline_code,
        )

        # Вызвать функцию обогащения
        return enrich_with_document_terms(df, chembl_client, enrich_cfg)

    def _extract_nested_fields(self, record: dict[str, Any]) -> dict[str, Any]:
        """Extract fields from nested objects in document records."""
        # For documents, there are typically no nested objects to extract
        # But we keep this method for consistency with other pipelines
        return record

    @staticmethod
    def _coerce_mapping(payload: Any) -> dict[str, Any]:
        """Coerce payload to dictionary mapping."""
        if isinstance(payload, Mapping):
            return cast(dict[str, Any], payload)
        return {}
