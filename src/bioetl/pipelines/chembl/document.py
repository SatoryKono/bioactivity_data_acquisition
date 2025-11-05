"""Document pipeline implementation for ChEMBL."""

from __future__ import annotations

import hashlib
import json
import re
import time
from collections.abc import Mapping, Sequence
from typing import Any, cast
from urllib.parse import urlparse

import pandas as pd

from bioetl.clients.chembl import ChemblClient
from bioetl.config import PipelineConfig
from bioetl.config.models import SourceConfig
from bioetl.core import APIClientFactory, UnifiedLogger
from bioetl.core.api_client import UnifiedAPIClient
from bioetl.schemas.document_chembl import COLUMN_ORDER

from ..base import PipelineBase
from .document_enrich import enrich_with_document_terms

API_DOCUMENT_FIELDS: tuple[str, ...] = (
    "document_chembl_id",
    "doc_type",
    "journal",
    "journal_full_title",
    "doi",
    "doi_chembl",
    "src_id",
    "title",
    "abstract",
    "year",
    "journal_abbrev",
    "volume",
    "issue",
    "first_page",
    "last_page",
    "pubmed_id",
    "authors",
)


class ChemblDocumentPipeline(PipelineBase):
    """ETL pipeline extracting document records from the ChEMBL API."""

    actor = "document_chembl"

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

        source_config = self._resolve_source_config("chembl")
        base_url = self._resolve_base_url(source_config.parameters)
        client = self._client_factory.for_source("chembl", base_url=base_url)
        self.register_client("chembl_document_client", client)

        self._chembl_release = self._fetch_chembl_release(client, log)

        batch_size = self._resolve_batch_size(source_config)
        limit = self.config.cli.limit
        page_size = min(batch_size, 25)
        if limit is not None:
            page_size = min(page_size, limit)
        page_size = max(page_size, 1)

        select_fields = self._resolve_select_fields(source_config)
        records: list[dict[str, Any]] = []
        next_endpoint: str | None = "/document.json"
        params: Mapping[str, Any] | None = {
            "limit": page_size,
            "only": ",".join(select_fields),
        }
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

            next_link = self._next_link(payload, base_url=base_url)
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
            chembl_release=self._chembl_release,
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

        source_config = self._resolve_source_config("chembl")
        base_url = self._resolve_base_url(source_config.parameters)
        client = self._client_factory.for_source("chembl", base_url=base_url)
        self.register_client("chembl_document_client", client)

        self._chembl_release = self._fetch_chembl_release(client, log)

        batch_size = self._resolve_batch_size(source_config)
        limit = self.config.cli.limit

        # Convert list of IDs to DataFrame for compatibility with _extract_from_chembl
        input_frame = pd.DataFrame({"document_chembl_id": list(ids)})

        batch_dataframe = self._extract_from_chembl(
            input_frame,
            client,
            batch_size=batch_size,
            limit=limit,
        )

        duration_ms = (time.perf_counter() - stage_start) * 1000.0
        batch_stats = self._last_batch_extract_stats or {}
        log.info(
            "chembl_document.extract_by_ids_summary",
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
        df = self._ensure_schema_columns(df, log)

        # Order columns according to schema
        df = self._order_schema_columns(df)

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

    def _resolve_source_config(self, name: str) -> SourceConfig:
        try:
            return self.config.sources[name]
        except KeyError as exc:
            msg = f"Source '{name}' is not configured for pipeline '{self.pipeline_code}'"
            raise KeyError(msg) from exc

    @staticmethod
    def _resolve_base_url(parameters: Mapping[str, Any]) -> str:
        base_url = parameters.get("base_url")
        if not isinstance(base_url, str) or not base_url.strip():
            msg = "sources.chembl.parameters.base_url must be a non-empty string"
            raise ValueError(msg)
        return base_url

    @staticmethod
    def _resolve_batch_size(source_config: SourceConfig) -> int:
        batch_size: int | None = getattr(source_config, "batch_size", None)
        if batch_size is None:
            parameters = getattr(source_config, "parameters", {})
            if isinstance(parameters, Mapping):
                candidate: Any = parameters.get("batch_size")  # pyright: ignore[reportUnknownVariableType,reportUnknownMemberType]
                if isinstance(candidate, int) and candidate > 0:
                    batch_size = candidate
        if batch_size is None or batch_size <= 0:
            batch_size = 25
        return batch_size

    def _resolve_select_fields(self, source_config: SourceConfig) -> list[str]:
        """Resolve select_fields from config or use default API_DOCUMENT_FIELDS."""
        parameters_raw = getattr(source_config, "parameters", {})
        if isinstance(parameters_raw, Mapping):
            parameters = cast(Mapping[str, Any], parameters_raw)
            select_fields_raw = parameters.get("select_fields")
            if (
                select_fields_raw is not None
                and isinstance(select_fields_raw, Sequence)
                and not isinstance(select_fields_raw, (str, bytes))
            ):
                select_fields = cast(Sequence[Any], select_fields_raw)
                return [str(field) for field in select_fields]
        return list(API_DOCUMENT_FIELDS)

    def _fetch_chembl_release(
        self,
        client: UnifiedAPIClient,
        log: Any,
    ) -> str | None:
        response = client.get("/status.json")
        status_payload = self._coerce_mapping(response.json())
        release_value = self._extract_chembl_release(status_payload)
        log.info("chembl_document.status", chembl_release=release_value)
        return release_value

    def _extract_from_chembl(
        self,
        dataset: object,
        client: UnifiedAPIClient,
        *,
        batch_size: int | None = None,
        limit: int | None = None,
    ) -> pd.DataFrame:
        """Extract document records by batching document_chembl_id values."""

        log = UnifiedLogger.get(__name__).bind(component=f"{self.pipeline_code}.extract")
        method_start = time.perf_counter()
        self._last_batch_extract_stats = None
        source_config = self._resolve_source_config("chembl")
        select_fields = self._resolve_select_fields(source_config)

        if batch_size is None:
            batch_size = self._resolve_batch_size(source_config)

        # Ensure batch_size does not exceed ChEMBL API limit
        batch_size = min(batch_size, 25)

        if not isinstance(dataset, pd.DataFrame):
            msg = f"Expected pd.DataFrame, got {type(dataset)}"
            raise TypeError(msg)

        if dataset.empty:
            log.debug("extract_from_chembl.empty_dataset")
            return pd.DataFrame()

        id_column = "document_chembl_id"
        if id_column not in dataset.columns:
            msg = f"Dataset missing required column '{id_column}'"
            raise ValueError(msg)

        # Extract unique IDs, filter out NaN
        ids = dataset[id_column].dropna().astype(str).unique().tolist()
        ids.sort()  # Deterministic ordering

        if not ids:
            log.debug("extract_from_chembl.no_valid_ids")
            return pd.DataFrame()

        # Process in batches
        all_records: list[dict[str, Any]] = []
        batches = 0
        api_calls = 0
        cache_hits = 0

        for i in range(0, len(ids), batch_size):
            batch_ids = ids[i : i + batch_size]
            batches += 1

            params: dict[str, Any] = {
                "document_chembl_id__in": ",".join(batch_ids),
                "limit": batch_size,
                "only": ",".join(select_fields),
            }

            try:
                response = client.get("/document.json", params=params)
                api_calls += 1
                payload = self._coerce_mapping(response.json())
                page_items = self._extract_page_items(payload)

                for item in page_items:
                    processed_item = self._extract_nested_fields(dict(item))
                    all_records.append(processed_item)

                if limit is not None and len(all_records) >= limit:
                    all_records = all_records[:limit]
                    break

            except Exception as exc:
                log.warning(
                    "extract_from_chembl.batch_error",
                    batch_ids=batch_ids,
                    error=str(exc),
                    exc_info=True,
                )

        dataframe = pd.DataFrame.from_records(all_records)  # pyright: ignore[reportUnknownMemberType]
        if not dataframe.empty and "document_chembl_id" in dataframe.columns:
            dataframe = dataframe.sort_values("document_chembl_id").reset_index(drop=True)

        duration_ms = (time.perf_counter() - method_start) * 1000.0
        self._last_batch_extract_stats = {
            "batches": batches,
            "api_calls": api_calls,
            "cache_hits": cache_hits,
        }

        log.info(
            "extract_from_chembl.summary",
            rows=int(dataframe.shape[0]),
            requested=len(ids),
            batches=batches,
            api_calls=api_calls,
            duration_ms=duration_ms,
        )

        return dataframe

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
        df = df.copy()

        # Normalize title
        if "title" in df.columns:
            df["title"] = df["title"].apply(lambda x: str(x).strip()[:1000] if pd.notna(x) else None)  # pyright: ignore[reportUnknownMemberType,reportUnknownLambdaType,reportUnknownArgumentType]

        # Normalize abstract
        if "abstract" in df.columns:
            df["abstract"] = df["abstract"].apply(lambda x: str(x).strip()[:5000] if pd.notna(x) else None)  # pyright: ignore[reportUnknownMemberType,reportUnknownLambdaType,reportUnknownArgumentType]

        # Normalize journal
        if "journal" in df.columns:
            df["journal"] = df["journal"].apply(self._normalize_journal)  # pyright: ignore[reportUnknownMemberType]

        # Normalize authors
        if "authors" in df.columns:
            authors_result = df["authors"].apply(self._normalize_authors)  # pyright: ignore[reportUnknownMemberType]
            df["authors"] = authors_result.apply(lambda x: x[0] if isinstance(x, tuple) else "")  # pyright: ignore[reportUnknownMemberType,reportUnknownLambdaType]
            df["authors_count"] = authors_result.apply(lambda x: x[1] if isinstance(x, tuple) else 0)  # pyright: ignore[reportUnknownMemberType,reportUnknownLambdaType]

        return df

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
            df["year"] = pd.to_numeric(df["year"], errors="coerce")  # pyright: ignore[reportUnknownMemberType]
            df["year"] = df["year"].apply(  # pyright: ignore[reportUnknownMemberType,reportUnknownLambdaType,reportUnknownArgumentType]
                lambda x: int(x) if pd.notna(x) and 1500 <= x <= 2100 else None  # pyright: ignore[reportUnknownLambdaType,reportUnknownArgumentType]
            ).astype("Int64")

        return df

    def _add_system_fields(self, df: pd.DataFrame, log: Any) -> pd.DataFrame:
        """Add system fields (source, hash_business_key, hash_row)."""
        df = df.copy()

        # Add source field
        df["source"] = "ChEMBL"

        # Add hash_business_key
        if "document_chembl_id" in df.columns:
            df["hash_business_key"] = df["document_chembl_id"].apply(self._compute_hash_business_key)  # pyright: ignore[reportUnknownMemberType]

        # Add hash_row
        df["hash_row"] = df.apply(self._compute_hash_row, axis=1)

        return df

    @staticmethod
    def _compute_hash_business_key(document_chembl_id: str) -> str:
        """Compute SHA256 hash of business key."""
        return hashlib.sha256(str(document_chembl_id).encode()).hexdigest()

    def _compute_hash_row(self, row: pd.Series) -> str:
        """Compute SHA256 hash of canonical row representation."""
        # Create canonical representation
        canonical_dict: dict[str, Any] = {}
        for col in COLUMN_ORDER:
            if col in row.index:
                value = row[col]
                if pd.isna(value):
                    canonical_dict[col] = None
                elif isinstance(value, (int, float)):
                    canonical_dict[col] = value
                elif isinstance(value, str):
                    canonical_dict[col] = value
                else:
                    canonical_dict[col] = str(value)
            else:
                canonical_dict[col] = None
        # Convert to JSON string
        canonical_json = json.dumps(canonical_dict, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()

    def _ensure_schema_columns(self, df: pd.DataFrame, log: Any) -> pd.DataFrame:
        """Ensure all schema columns exist with correct types."""
        df = df.copy()

        # Add missing columns
        for col in COLUMN_ORDER:
            if col not in df.columns:
                if col == "source":
                    df[col] = "ChEMBL"
                elif col in ("hash_business_key", "hash_row"):
                    df[col] = ""
                elif col == "authors_count":
                    df[col] = 0
                else:
                    df[col] = None

        return df

    def _order_schema_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Order columns according to COLUMN_ORDER."""
        # Ensure all columns from schema exist
        for col in COLUMN_ORDER:
            if col not in df.columns:
                df[col] = None
        # Reorder columns
        existing_columns = [col for col in COLUMN_ORDER if col in df.columns]
        extra_columns = [col for col in df.columns if col not in COLUMN_ORDER]
        return df[existing_columns + extra_columns]

    def _check_document_id_uniqueness(self, df: pd.DataFrame, log: Any) -> None:
        """Check that document_chembl_id is unique."""
        if df.empty:
            return
        if "document_chembl_id" not in df.columns:
            return
        duplicates = df["document_chembl_id"].duplicated()
        if duplicates.any():
            duplicate_ids = df[df["document_chembl_id"].duplicated()]["document_chembl_id"].unique().tolist()
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
        source_config = self._resolve_source_config("chembl")
        base_url = self._resolve_base_url(source_config.parameters)
        api_client = self._client_factory.for_source("chembl", base_url=base_url)
        # Регистрируем клиент только если он еще не зарегистрирован
        if "chembl_enrichment_client" not in self._registered_clients:
            self.register_client("chembl_enrichment_client", api_client)
        chembl_client = ChemblClient(api_client)

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

    @staticmethod
    def _extract_chembl_release(payload: Mapping[str, Any]) -> str | None:
        """Extract ChEMBL release version from status payload."""
        for key in ("chembl_release", "chembl_db_version", "release", "version"):
            value = payload.get(key)
            if isinstance(value, str) and value.strip():
                return value
            if value is not None:
                return str(value)
        return None

    @staticmethod
    def _extract_page_items(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
        """Extract items from paginated response payload."""
        candidates: list[dict[str, Any]] = []
        # Check common keys for document items
        for key in ("documents", "data", "items", "results"):
            value: Any = payload.get(key)
            if isinstance(value, Sequence):
                candidates = [
                    cast(dict[str, Any], item)  # pyright: ignore[reportUnknownVariableType]
                    for item in value  # pyright: ignore[reportUnknownVariableType]
                    if isinstance(item, Mapping)
                ]
                if candidates:
                    return candidates
        # Fallback: iterate through all keys
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
        """Extract next page link from paginated response."""
        page_meta: Any = payload.get("page_meta")  # pyright: ignore[reportUnknownMemberType]
        if isinstance(page_meta, Mapping):
            next_link_raw: Any = page_meta.get("next")  # pyright: ignore[reportUnknownVariableType,reportUnknownMemberType]
            next_link: str | None = cast(str | None, next_link_raw) if next_link_raw is not None else None
            if isinstance(next_link, str) and next_link:
                # Handle full URLs
                if next_link.startswith("http://") or next_link.startswith("https://"):
                    parsed = urlparse(next_link)
                    base_parsed = urlparse(base_url)
                    path: str = str(parsed.path)
                    base_path_from_url: str = str(base_parsed.path)
                    path_normalized: str = path.rstrip("/")
                    base_path_normalized: str = base_path_from_url.rstrip("/")
                    if base_path_normalized and path_normalized.startswith(base_path_normalized):
                        relative_path = path_normalized[len(base_path_normalized) :]
                        if not relative_path:
                            return None
                        if not relative_path.startswith("/"):
                            relative_path = f"/{relative_path}"
                    else:
                        if "/api/data/" in path:
                            parts = path.split("/api/data/", 1)
                            if len(parts) > 1:
                                relative_path = "/" + parts[1]
                            else:
                                relative_path = path
                        else:
                            relative_path = path
                        if not relative_path.startswith("/"):
                            relative_path = f"/{relative_path}"
                    if parsed.query:
                        relative_path = f"{relative_path}?{parsed.query}"
                    return relative_path
                # Handle relative URLs
                base_path_parse_result = urlparse(base_url)
                base_path: str = str(base_path_parse_result.path).rstrip("/")
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

