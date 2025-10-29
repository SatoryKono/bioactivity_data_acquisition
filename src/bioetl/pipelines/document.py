"""Document Pipeline - ChEMBL document extraction with external enrichment."""

import os
import re
from collections.abc import Iterable, Sequence
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import pandas as pd
import requests
from pandera.errors import SchemaErrors

from bioetl.adapters import (
    CrossrefAdapter,
    OpenAlexAdapter,
    PubMedAdapter,
    SemanticScholarAdapter,
)
from bioetl.adapters.base import AdapterConfig
from bioetl.config import PipelineConfig
from bioetl.core.api_client import APIConfig, CircuitBreakerOpenError, UnifiedAPIClient
from bioetl.core.logger import UnifiedLogger
from bioetl.pipelines.base import PipelineBase
from bioetl.pipelines.document_enrichment import merge_with_precedence
from bioetl.schemas.document import (
    DocumentNormalizedSchema,
    DocumentRawSchema,
    DocumentSchema,
)
from bioetl.schemas.document_input import DocumentInputSchema
from bioetl.schemas.registry import schema_registry

logger = UnifiedLogger.get(__name__)

# Register schema
schema_registry.register("document", "1.0.0", DocumentNormalizedSchema)  # type: ignore[arg-type]


class DocumentPipeline(PipelineBase):
    """Pipeline for extracting ChEMBL document data.

    Modes:
    - chembl: ChEMBL only
    - all: ChEMBL + PubMed/Crossref/OpenAlex/Semantic Scholar
    """

    def __init__(self, config: PipelineConfig, run_id: str):
        super().__init__(config, run_id)

        # Initialize ChEMBL API client
        chembl_source = config.sources.get("chembl")

        default_base_url = "https://www.ebi.ac.uk/chembl/api/data"
        default_batch_size = 10
        default_max_url_length = 1800

        if isinstance(chembl_source, dict):
            base_url = chembl_source.get("base_url", default_base_url) or default_base_url
            batch_size_value = chembl_source.get("batch_size", default_batch_size)
            max_url_length_value = chembl_source.get("max_url_length", default_max_url_length)
        elif chembl_source is not None:
            base_url = getattr(chembl_source, "base_url", default_base_url) or default_base_url
            batch_size_value = getattr(chembl_source, "batch_size", default_batch_size)
            max_url_length_value = getattr(chembl_source, "max_url_length", default_max_url_length)
        else:
            base_url = default_base_url
            batch_size_value = default_batch_size
            max_url_length_value = default_max_url_length

        try:
            batch_size = int(batch_size_value)
        except (TypeError, ValueError):
            batch_size = default_batch_size

        try:
            max_url_length = int(max_url_length_value)
        except (TypeError, ValueError):
            max_url_length = default_max_url_length

        chembl_config = APIConfig(
            name="chembl",
            base_url=base_url,
            cache_enabled=config.cache.enabled,
            cache_ttl=config.cache.ttl,
        )
        self.api_client = UnifiedAPIClient(chembl_config)
        self.max_batch_size = 25
        self.batch_size = min(self.max_batch_size, max(1, batch_size))
        self.max_url_length = max(1, max_url_length)
        self._document_cache: dict[str, dict[str, Any]] = {}

        # Initialize external adapters if enabled
        self.external_adapters: dict[str, Any] = {}
        self._init_external_adapters()

        # Cache ChEMBL release version
        self._chembl_release = self._get_chembl_release()

    def extract(self, input_file: Path | None = None) -> pd.DataFrame:
        """Extract document data from input file with optional enrichment."""
        if input_file is None:
            input_file = Path("data/input/document.csv")

        logger.info("reading_input", path=input_file)

        if not input_file.exists():
            logger.warning("input_file_not_found", path=input_file)
            return pd.DataFrame(columns=["document_chembl_id"])

        df = pd.read_csv(input_file, dtype="string")

        valid_ids, rejected_rows = self._prepare_input_ids(df)
        if rejected_rows:
            self._persist_rejected_inputs(rejected_rows)

        if not valid_ids:
            logger.warning("no_valid_ids", total=len(df))
            return pd.DataFrame(columns=["document_chembl_id"])

        logger.info("validated_ids", total=len(valid_ids))

        limit_value = self.get_runtime_limit()
        if limit_value is not None and len(valid_ids) > limit_value:
            logger.info(
                "applying_input_limit",
                limit=limit_value,
                original_ids=len(valid_ids),
            )
            valid_ids = valid_ids[:limit_value]

        records = self._fetch_documents(valid_ids)
        if not records:
            logger.warning("no_records_fetched", ids=len(valid_ids))
            return pd.DataFrame(columns=["document_chembl_id"])

        result_df = pd.DataFrame(records)
        result_df = result_df.convert_dtypes()

        validated = self._validate_raw_dataframe(result_df)
        logger.info("extraction_completed", rows=len(validated))
        return validated

    def _init_external_adapters(self) -> None:
        """Initialize external API adapters."""
        sources = self.config.sources

        # PubMed adapter
        if "pubmed" in sources and hasattr(sources["pubmed"], "enabled") and sources["pubmed"].enabled:
            pubmed = sources["pubmed"]
            pubmed_config = APIConfig(
                name="pubmed",
                base_url=getattr(pubmed, "base_url", "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"),
                rate_limit_max_calls=getattr(pubmed, "rate_limit_max_calls", 3),
                rate_limit_period=getattr(pubmed, "rate_limit_period", 1.0),
                cache_enabled=self.config.cache.enabled,
                cache_ttl=self.config.cache.ttl,
            )
            adapter_config = AdapterConfig(
                enabled=True,
                batch_size=getattr(pubmed, "batch_size", 200),
                workers=getattr(pubmed, "workers", 1),
            )
            adapter_config.tool = getattr(pubmed, "tool", "bioactivity_etl")
            email = getattr(pubmed, "email", os.getenv("PUBMED_EMAIL", ""))
            # Resolve environment variables in config
            if email.startswith("${") and email.endswith("}"):
                env_var = email[2:-1]
                email = os.getenv(env_var, "")
            adapter_config.email = email
            api_key = getattr(pubmed, "api_key", os.getenv("PUBMED_API_KEY", ""))
            if api_key.startswith("${") and api_key.endswith("}"):
                env_var = api_key[2:-1]
                api_key = os.getenv(env_var, "")
            adapter_config.api_key = api_key
            self.external_adapters["pubmed"] = PubMedAdapter(pubmed_config, adapter_config)

        # Crossref adapter
        if "crossref" in sources and hasattr(sources["crossref"], "enabled") and sources["crossref"].enabled:
            crossref = sources["crossref"]
            crossref_config = APIConfig(
                name="crossref",
                base_url=getattr(crossref, "base_url", "https://api.crossref.org"),
                rate_limit_max_calls=getattr(crossref, "rate_limit_max_calls", 2),
                rate_limit_period=getattr(crossref, "rate_limit_period", 1.0),
                cache_enabled=self.config.cache.enabled,
                cache_ttl=self.config.cache.ttl,
            )
            adapter_config = AdapterConfig(
                enabled=True,
                batch_size=getattr(crossref, "batch_size", 100),
                workers=getattr(crossref, "workers", 2),
            )
            mailto = getattr(crossref, "mailto", os.getenv("CROSSREF_MAILTO", ""))
            if mailto.startswith("${") and mailto.endswith("}"):
                env_var = mailto[2:-1]
                mailto = os.getenv(env_var, "")
            adapter_config.mailto = mailto
            self.external_adapters["crossref"] = CrossrefAdapter(crossref_config, adapter_config)

        # OpenAlex adapter
        if "openalex" in sources and hasattr(sources["openalex"], "enabled") and sources["openalex"].enabled:
            openalex = sources["openalex"]
            openalex_config = APIConfig(
                name="openalex",
                base_url=getattr(openalex, "base_url", "https://api.openalex.org"),
                rate_limit_max_calls=getattr(openalex, "rate_limit_max_calls", 10),
                rate_limit_period=getattr(openalex, "rate_limit_period", 1.0),
                cache_enabled=self.config.cache.enabled,
                cache_ttl=self.config.cache.ttl,
            )
            adapter_config = AdapterConfig(
                enabled=True,
                batch_size=getattr(openalex, "batch_size", 100),
                workers=getattr(openalex, "workers", 4),
            )
            self.external_adapters["openalex"] = OpenAlexAdapter(openalex_config, adapter_config)

        # Semantic Scholar adapter
        if "semantic_scholar" in sources and hasattr(sources["semantic_scholar"], "enabled") and sources["semantic_scholar"].enabled:
            s2 = sources["semantic_scholar"]
            s2_config = APIConfig(
                name="semantic_scholar",
                base_url=getattr(s2, "base_url", "https://api.semanticscholar.org/graph/v1"),
                rate_limit_max_calls=getattr(s2, "rate_limit_max_calls", 1),
                rate_limit_period=getattr(s2, "rate_limit_period", 1.25),
                cache_enabled=self.config.cache.enabled,
                cache_ttl=self.config.cache.ttl,
            )
            adapter_config = AdapterConfig(
                enabled=True,
                batch_size=getattr(s2, "batch_size", 50),
                workers=getattr(s2, "workers", 1),
            )
            api_key = getattr(s2, "api_key", os.getenv("SEMANTIC_SCHOLAR_API_KEY", ""))
            if api_key.startswith("${") and api_key.endswith("}"):
                env_var = api_key[2:-1]
                api_key = os.getenv(env_var, "")
            adapter_config.api_key = api_key
            self.external_adapters["semantic_scholar"] = SemanticScholarAdapter(s2_config, adapter_config)

        logger.info("adapters_initialized", count=len(self.external_adapters))

    def _enrich_with_external_sources(self, chembl_df: pd.DataFrame) -> pd.DataFrame:
        """Enrich ChEMBL data with external sources."""
        # Extract PMIDs and DOIs from ChEMBL data
        pmids = []
        dois = []

        # Check for multiple possible PMID column names
        if "chembl_pmid" in chembl_df.columns:
            pmids = chembl_df["chembl_pmid"].dropna().astype(str).tolist()
        elif "pmid" in chembl_df.columns:
            pmids = chembl_df["pmid"].dropna().astype(str).tolist()
        elif "pubmed_id" in chembl_df.columns:
            pmids = chembl_df["pubmed_id"].dropna().astype(str).tolist()

        doi_columns = [col for col in ("doi", "chembl_doi") if col in chembl_df.columns]
        if doi_columns:
            seen_dois: set[str] = set()
            ordered_dois: list[str] = []
            for column in doi_columns:
                for value in chembl_df[column].dropna().tolist():
                    doi_str = str(value)
                    if not doi_str:
                        continue
                    if doi_str in seen_dois:
                        continue
                    seen_dois.add(doi_str)
                    ordered_dois.append(doi_str)
            dois = ordered_dois

        logger.info(
            "enrichment_data",
            pmids_count=len(pmids),
            dois_count=len(dois),
            doi_columns=doi_columns,
            sample_pmids=pmids[:3] if pmids else [],
        )

        # Fetch from external sources in parallel
        pubmed_df = None
        crossref_df = None
        openalex_df = None
        semantic_scholar_df = None

        # Use ThreadPoolExecutor for parallel fetching
        workers = min(4, len(self.external_adapters))
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {}

            if "pubmed" in self.external_adapters and pmids:
                futures["pubmed"] = executor.submit(self.external_adapters["pubmed"].process, pmids)

            if "crossref" in self.external_adapters and dois:
                futures["crossref"] = executor.submit(self.external_adapters["crossref"].process, dois)

            if "openalex" in self.external_adapters and dois:
                futures["openalex"] = executor.submit(self.external_adapters["openalex"].process, dois)

            if "semantic_scholar" in self.external_adapters:
                if pmids:
                    # Primary: fetch by PMIDs
                    logger.info("semantic_scholar_by_pmids", count=len(pmids))
                    futures["semantic_scholar"] = executor.submit(self.external_adapters["semantic_scholar"].process, pmids)
                elif "_original_title" in chembl_df.columns:
                    # Fallback: fetch by titles if no PMIDs
                    titles = chembl_df["_original_title"].dropna().tolist()
                    logger.info("semantic_scholar_by_titles_fallback", count=len(titles))
                    # Call process_titles wrapper method
                    futures["semantic_scholar"] = executor.submit(
                        self.external_adapters["semantic_scholar"].process_titles, titles
                    )
                else:
                    logger.warning("semantic_scholar_skipped", reason="no_pmids_or_titles")

            # Collect results
            for source, future in futures.items():
                try:
                    result = future.result(timeout=300)  # 5 min timeout
                    if source == "pubmed":
                        pubmed_df = result
                    elif source == "crossref":
                        crossref_df = result
                    elif source == "openalex":
                        openalex_df = result
                    elif source == "semantic_scholar":
                        semantic_scholar_df = result
                    logger.info("adapter_completed", source=source, rows=len(result) if not result.empty else 0)
                    # DEBUG: Log returned columns
                    if not result.empty:
                        logger.info("adapter_columns", source=source, columns=list(result.columns))
                except Exception as e:
                    logger.error("adapter_failed", source=source, error=str(e))

        # DEBUG: Log before merge
        logger.info("before_merge", chembl_cols=len(chembl_df.columns), chembl_rows=len(chembl_df))
        if pubmed_df is not None and not pubmed_df.empty:
            logger.info("pubmed_df_size", rows=len(pubmed_df), cols=len(pubmed_df.columns))
        if crossref_df is not None and not crossref_df.empty:
            logger.info("crossref_df_size", rows=len(crossref_df), cols=len(crossref_df.columns))
        if openalex_df is not None and not openalex_df.empty:
            logger.info("openalex_df_size", rows=len(openalex_df), cols=len(openalex_df.columns))
        if semantic_scholar_df is not None and not semantic_scholar_df.empty:
            logger.info("semantic_scholar_df_size", rows=len(semantic_scholar_df), cols=len(semantic_scholar_df.columns))

        # Merge with precedence
        enriched_df = merge_with_precedence(
            chembl_df, pubmed_df, crossref_df, openalex_df, semantic_scholar_df
        )

        logger.info("after_merge", enriched_cols=len(enriched_df.columns), enriched_rows=len(enriched_df))

        return enriched_df

    # ------------------------------------------------------------------
    # Extraction helpers
    # ------------------------------------------------------------------

    def _prepare_input_ids(self, df: pd.DataFrame) -> tuple[list[str], list[dict[str, str]]]:
        """Normalize and validate input identifiers using Pandera schema."""

        if "document_chembl_id" not in df.columns:
            raise ValueError("Input file must contain 'document_chembl_id' column")

        regex = re.compile(r"^CHEMBL\d+$")
        valid_ids: list[str] = []
        rejected: list[dict[str, str]] = []
        seen: set[str] = set()

        for raw_value in df["document_chembl_id"].tolist():
            normalized, reason = self._normalize_identifier(raw_value, regex)
            if reason:
                rejected.append(
                    {
                        "document_chembl_id": "" if raw_value is None else str(raw_value),
                        "reason": reason,
                    }
                )
                continue

            if normalized in seen:
                logger.debug("duplicate_id_skipped", document_chembl_id=normalized)
                continue

            if normalized is not None:
                seen.add(normalized)
                valid_ids.append(normalized)

        if valid_ids:
            DocumentInputSchema.validate(pd.DataFrame({"document_chembl_id": valid_ids}))

        return valid_ids, rejected

    def _normalize_identifier(
        self, value: Any, pattern: re.Pattern[str]
    ) -> tuple[str | None, str | None]:
        """Normalize identifier to uppercase CHEMBL format with validation reason."""

        if value is None or pd.isna(value):
            return None, "missing"

        text = str(value).strip().upper()
        if not text or text in {"#N/A", "N/A", "NONE", "NULL"}:
            return None, "missing"

        if not pattern.fullmatch(text):
            return None, "invalid_format"

        return text, None

    def _persist_rejected_inputs(self, rows: list[dict[str, str]]) -> None:
        """Persist rejected inputs for auditability."""

        rejected_df = pd.DataFrame(rows)
        output_dir = Path("data/output/rejected_inputs")
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / f"document_rejected_{self.run_id}.csv"
        logger.warning("rejected_inputs_found", count=len(rejected_df), path=output_path)
        self.output_writer.atomic_writer.write(rejected_df, output_path)

    def _validate_raw_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate raw payloads against Pandera schema before transformation."""

        if df.empty:
            logger.info("raw_validation_skipped_empty", rows=0)
            return df

        working_df = df.copy()
        schema = DocumentRawSchema.to_schema()

        expected_order = list(schema.columns.keys())
        missing_columns = [column for column in expected_order if column not in working_df.columns]
        extra_columns = [column for column in working_df.columns if column not in expected_order]

        if missing_columns:
            for column in missing_columns:
                working_df[column] = pd.NA

        # Reindex the known columns explicitly to avoid Pandera's COLUMN_NOT_ORDERED
        # failures when upstream payloads shuffle column order. Using ``reindex`` with
        # the schema-driven order guarantees determinism while keeping optional fields
        # (like ``journal_abbrev``) present even when the input payload omits them.
        ordered_core = working_df.reindex(columns=expected_order)
        extras_df = working_df.loc[:, extra_columns] if extra_columns else None

        # Log the ordering context before recombining to simplify troubleshooting of
        # Pandera validation errors reported by end users.
        logger.debug(
            "raw_schema_ordering",
            expected=expected_order,
            current=list(df.columns),
            missing=missing_columns,
            extra=extra_columns,
        )

        try:
            core_df = ordered_core.copy()

            if "source" not in core_df.columns:
                core_df["source"] = "ChEMBL"
            else:
                core_df["source"] = core_df["source"].fillna("ChEMBL")

            core_df = core_df.convert_dtypes()
            validated_core = DocumentRawSchema.validate(core_df, lazy=True)

            if extras_df is not None and not extras_df.empty:
                extras_df = extras_df.convert_dtypes()
                validated = pd.concat([validated_core, extras_df], axis=1)
            else:
                validated = validated_core

            logger.info("raw_schema_validation_passed", rows=len(validated))
            return validated
        except SchemaErrors as exc:
            failure_cases = getattr(exc, "failure_cases", None)
            details = None
            if isinstance(failure_cases, pd.DataFrame):
                details = failure_cases.to_dict(orient="records")

            self.record_validation_issue(
                {
                    "metric": "raw_schema_validation",
                    "severity": "error",
                    "details": details,
                }
            )
            logger.error("raw_schema_validation_failed", error=str(exc))
            raise

    def _fetch_documents(self, ids: Sequence[str]) -> list[dict[str, Any]]:
        """Fetch document payloads from ChEMBL with resilient batching."""

        results: list[dict[str, Any]] = []
        for chunk in self._chunked(ids, max(1, self.batch_size)):
            cached_records, to_fetch = self._separate_cached(chunk)
            results.extend(cached_records)

            if not to_fetch:
                continue

            try:
                fetched = self._fetch_documents_recursive(to_fetch)
                for record in fetched:
                    document_id = record.get("document_chembl_id")
                    if document_id:
                        self._document_cache[self._document_cache_key(str(document_id))] = record
                results.extend(fetched)
            except Exception as exc:  # noqa: BLE001
                error_type = self._classify_error(exc)
                logger.error(
                    "document_fetch_failed",
                    chunk=list(to_fetch),
                    error=str(exc),
                    error_type=error_type,
                )
                for document_id in to_fetch:
                    fallback = self._create_fallback_row(document_id, error_type, str(exc))
                    self._document_cache[self._document_cache_key(document_id)] = fallback
                    results.append(fallback)

        return results

    def _fetch_documents_recursive(self, ids: Sequence[str]) -> list[dict[str, Any]]:
        """Recursively fetch documents handling URL length and timeouts."""

        if not ids:
            return []

        if len(ids) > self.max_batch_size:
            midpoint = max(1, len(ids) // 2)
            return self._fetch_documents_recursive(ids[:midpoint]) + self._fetch_documents_recursive(ids[midpoint:])

        params = {"document_chembl_id__in": ",".join(ids)}
        full_url = self._build_full_url("/document.json", params)

        if len(full_url) > self.max_url_length and len(ids) > 1:
            midpoint = max(1, len(ids) // 2)
            return self._fetch_documents_recursive(ids[:midpoint]) + self._fetch_documents_recursive(ids[midpoint:])

        try:
            response = self.api_client.request_json("/document.json", params=params)
        except requests.exceptions.ReadTimeout:
            if len(ids) <= 1:
                raise
            midpoint = max(1, len(ids) // 2)
            return self._fetch_documents_recursive(ids[:midpoint]) + self._fetch_documents_recursive(ids[midpoint:])

        documents = response.get("documents") or response.get("document") or []
        return [doc for doc in documents if isinstance(doc, dict)]

    def _build_full_url(self, endpoint: str, params: dict[str, Any]) -> str:
        base = self.api_client.config.base_url.rstrip("/")
        query = urlencode(params, doseq=False)
        return f"{base}{endpoint}?{query}" if query else f"{base}{endpoint}"

    def _chunked(self, items: Sequence[str], size: int) -> Iterable[Sequence[str]]:
        for idx in range(0, len(items), size):
            yield items[idx : idx + size]

    def _separate_cached(self, ids: Sequence[str]) -> tuple[list[dict[str, Any]], list[str]]:
        cached: list[dict[str, Any]] = []
        missing: list[str] = []
        for document_id in ids:
            key = self._document_cache_key(document_id)
            if key in self._document_cache:
                cached.append(self._document_cache[key])
            else:
                missing.append(document_id)
        return cached, missing

    def _document_cache_key(self, document_id: str) -> str:
        release = self._chembl_release or "unknown"
        return f"document:{release}:{document_id}"

    def _classify_error(self, exc: Exception) -> str:
        if isinstance(exc, requests.exceptions.ReadTimeout):
            return "E_TIMEOUT"
        if isinstance(exc, requests.exceptions.HTTPError):
            status = exc.response.status_code if exc.response is not None else None
            if status == 429:
                return "E_HTTP_429"
            if status and 500 <= status < 600:
                return "E_HTTP_5XX"
            if status and 400 <= status < 500:
                return "E_HTTP_4XX"
        if isinstance(exc, CircuitBreakerOpenError):
            return "E_CIRCUIT_BREAKER_OPEN"
        return "E_UNKNOWN"

    def _create_fallback_row(self, document_id: str, error_type: str, error_message: str) -> dict[str, Any]:
        return {
            "document_chembl_id": document_id,
            "error_type": error_type,
            "error_message": error_message,
            "attempted_at": datetime.now(timezone.utc).isoformat(),
        }

    def _get_chembl_release(self) -> str | None:
        """Get ChEMBL database release version from status endpoint.

        Returns:
            Version string (e.g., 'ChEMBL_36') or None
        """
        try:
            url = f"{self.api_client.config.base_url}/status.json"
            response = self.api_client.request_json(url)

            # Extract version info from status endpoint
            version = response.get("chembl_db_version")
            release_date = response.get("chembl_release_date")
            activities = response.get("activities")

            if version:
                logger.info(
                    "chembl_version_fetched",
                    version=version,
                    release_date=release_date,
                    activities=activities
                )
                return str(version)

            logger.warning("chembl_version_not_in_status_response")
            return None

        except Exception as e:
            logger.warning("failed_to_get_chembl_version", error=str(e))
            return None

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform document data with multi-source merge."""
        if df.empty:
            expected_columns = DocumentSchema.get_column_order()

            if expected_columns:
                empty_df = pd.DataFrame(columns=expected_columns)
                logger.info(
                    "transform_returning_empty_schema",
                    columns=len(expected_columns),
                )
                return empty_df

            return df

        # Map ChEMBL input fields according to schema
        # Map pubmed_id from CSV to document_pubmed_id
        if "pubmed_id" in df.columns:
            df["document_pubmed_id"] = df["pubmed_id"].apply(lambda x: int(x) if pd.notna(x) and str(x).isdigit() else None)

        # Map classification
        if "classification" in df.columns:
            df["document_classification"] = df["classification"]

        # Map is_experimental_doc to original_experimental_document (boolean)
        if "is_experimental_doc" in df.columns:
            df["original_experimental_document"] = df["is_experimental_doc"].apply(lambda x: bool(x) if pd.notna(x) else None)

        # Map document_contains_external_links to referenses_on_previous_experiments
        if "document_contains_external_links" in df.columns:
            df["referenses_on_previous_experiments"] = df["document_contains_external_links"].apply(lambda x: bool(x) if pd.notna(x) else None)

        # IMPORTANT: Map pubmed_id to chembl_pmid BEFORE normalization
        # Map pubmed_id -> chembl_pmid (convert to int)
        if "pubmed_id" in df.columns:
            df["chembl_pmid"] = df["pubmed_id"].apply(lambda x: int(x) if pd.notna(x) and str(x).isdigit() else None)

        # Normalize identifiers
        from bioetl.normalizers import registry

        # Normalize IDs before enrichment (now pubmed_id is safe to normalize)
        for col in ["document_chembl_id", "doi", "pmid", "pubmed_id"]:
            if col in df.columns:
                df[col] = df[col].apply(
                    lambda x: registry.normalize("identifier", x) if pd.notna(x) else None
                )

        # Save original title for semantic scholar enrichment BEFORE renaming
        if "title" in df.columns:
            df["_original_title"] = df["title"]

        # Rename ChEMBL fields to multi-source naming convention BEFORE enrichment
        field_mapping = {
            "title": "chembl_title",
            "journal": "chembl_journal",
            "year": "chembl_year",
            "authors": "chembl_authors",
            "abstract": "chembl_abstract",
            "volume": "chembl_volume",
            "issue": "chembl_issue",
        }

        for old_col, new_col in field_mapping.items():
            if old_col in df.columns:
                df[new_col] = df[old_col].apply(
                    lambda x: registry.normalize("string", x) if pd.notna(x) else None
                )
                if old_col != new_col:
                    df = df.drop(columns=[old_col])

        # Add chembl_doi from doi (but keep doi for enrichment)
        if "doi" in df.columns:
            df["chembl_doi"] = df["doi"]

        # Set chembl_doc_type - default to journal-article for ChEMBL documents
        df["chembl_doc_type"] = "journal-article"

        # Keep pubmed_id and doi with original names for enrichment (chembl_pmid already created above)

        # Check if enrichment is enabled and apply AFTER renaming
        enrichment_enabled = any(
            hasattr(self.config.sources.get(source_name), "enabled")
            and self.config.sources[source_name].enabled
            for source_name in ["pubmed", "crossref", "openalex", "semantic_scholar"]
            if source_name in self.config.sources
        )

        if enrichment_enabled and self.external_adapters:
            logger.info("enrichment_enabled", note="Fetching data from external sources")
            df = self._enrich_with_external_sources(df)
        else:
            logger.info(
                "enrichment_skipped",
                reason="no_external_adapters_enabled",
            )
            df = merge_with_precedence(df)

        # Drop temporary join keys if they exist
        if "pubmed_id" in df.columns:
            df = df.drop(columns=["pubmed_id"])
        if "_original_title" in df.columns:
            df = df.drop(columns=["_original_title"])

        # Add pipeline metadata
        df["pipeline_version"] = "1.0.0"
        df["source_system"] = "chembl"
        df["chembl_release"] = self._chembl_release
        df["extracted_at"] = pd.Timestamp.now(tz="UTC").isoformat()

        # Generate hash fields for data integrity
        from bioetl.core.hashing import generate_hash_business_key, generate_hash_row

        if "document_chembl_id" in df.columns:
            df["hash_business_key"] = df["document_chembl_id"].apply(generate_hash_business_key)
            df["hash_row"] = df.apply(lambda row: generate_hash_row(row.to_dict()), axis=1)

            # Generate deterministic index
            df = df.sort_values("document_chembl_id")  # Sort by primary key
            df["index"] = range(len(df))

            # Add all missing columns from schema
            expected_cols = DocumentSchema.get_column_order()

            if expected_cols:
                for col in expected_cols:
                    if col not in df.columns:
                        df[col] = pd.NA

                df = df[expected_cols]

        return df

    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate document data against schema with strict validation."""
        if df.empty:
            logger.info("validation_skipped_empty", rows=0)
            self.qc_metrics = {}
            return df

        working_df = df.copy().convert_dtypes()

        canonical_order = DocumentSchema.get_column_order()
        if canonical_order:
            missing_columns = [column for column in canonical_order if column not in working_df.columns]
            for column in missing_columns:
                working_df[column] = pd.NA

            extra_columns = [column for column in working_df.columns if column not in canonical_order]
            if extra_columns:
                working_df = working_df.drop(columns=extra_columns)

            working_df = working_df.loc[:, canonical_order]

        duplicate_count = (
            working_df["document_chembl_id"].duplicated().sum()
            if "document_chembl_id" in working_df.columns
            else 0
        )
        if duplicate_count > 0:
            logger.warning("duplicates_found", count=duplicate_count)
            working_df = working_df.drop_duplicates(subset=["document_chembl_id"], keep="first")
            self.record_validation_issue(
                {
                    "metric": "duplicates_removed",
                    "value": int(duplicate_count),
                    "severity": "warning",
                }
            )

        sources_to_validate = [
            ("pubmed", ["pubmed_doc_type"], ["pubmed_article_title", "pubmed_abstract"]),
            ("crossref", ["crossref_doc_type"], ["crossref_title", "crossref_authors"]),
            ("openalex", ["openalex_doc_type"], ["openalex_title", "openalex_authors"]),
            (
                "semantic_scholar",
                ["semantic_scholar_doc_type"],
                ["semantic_scholar_title", "semantic_scholar_abstract"],
            ),
        ]

        validation_errors: list[str] = []
        for source_name, required_fields, actual_field_names in sources_to_validate:
            has_source_data = any(
                column in working_df.columns and working_df[column].notna().any()
                for column in actual_field_names
            )

            if has_source_data:
                for required_field in required_fields:
                    if required_field not in working_df.columns or working_df[required_field].isna().all():
                        validation_errors.append(
                            f"{source_name}: {required_field} обязателен при наличии данных"
                        )

        if validation_errors:
            error_msg = "; ".join(validation_errors)
            self.record_validation_issue(
                {
                    "metric": "source_doc_type_missing",
                    "severity": "error",
                    "details": validation_errors,
                }
            )
            logger.error("validation_failed", errors=error_msg)
            raise ValueError(f"Валидация не прошла: {error_msg}")

        try:
            validated_df = DocumentNormalizedSchema.validate(working_df, lazy=True)
        except SchemaErrors as exc:
            failure_cases = getattr(exc, "failure_cases", None)
            details = None
            if isinstance(failure_cases, pd.DataFrame):
                details = failure_cases.to_dict(orient="records")

            self.record_validation_issue(
                {
                    "metric": "normalized_schema_validation",
                    "severity": "error",
                    "details": details,
                }
            )
            logger.error("schema_validation_failed", error=str(exc))
            raise

        metrics = self._compute_qc_metrics(validated_df)
        self.qc_metrics = metrics
        self._enforce_qc_thresholds(metrics)

        logger.info("validation_completed", rows=len(validated_df), duplicates_removed=duplicate_count)
        return validated_df

    def _compute_qc_metrics(self, df: pd.DataFrame) -> dict[str, float]:
        """Compute coverage, conflict, and fallback quality metrics."""

        total = len(df)
        if total == 0:
            return {
                "doi_coverage": 0.0,
                "pmid_coverage": 0.0,
                "title_coverage": 0.0,
                "journal_coverage": 0.0,
                "authors_coverage": 0.0,
                "conflicts_doi": 0.0,
                "conflicts_pmid": 0.0,
                "title_fallback_rate": 0.0,
            }

        def _coverage(column: str) -> float:
            if column not in df.columns:
                return 0.0
            return float(df[column].notna().sum() / total)

        metrics: dict[str, float] = {
            "doi_coverage": _coverage("doi_clean"),
            "pmid_coverage": _coverage("pmid") if "pmid" in df.columns else _coverage("pubmed_id"),
            "title_coverage": _coverage("title"),
            "journal_coverage": _coverage("journal"),
            "authors_coverage": _coverage("authors"),
        }

        if "conflict_doi" in df.columns:
            conflict_doi = df["conflict_doi"].fillna(False).astype("boolean")
            metrics["conflicts_doi"] = float(conflict_doi.sum() / total)
        else:
            metrics["conflicts_doi"] = 0.0

        if "conflict_pmid" in df.columns:
            conflict_pmid = df["conflict_pmid"].fillna(False).astype("boolean")
            metrics["conflicts_pmid"] = float(conflict_pmid.sum() / total)
        else:
            metrics["conflicts_pmid"] = 0.0

        if "title_source" in df.columns:
            fallback_mask = (
                df["title_source"]
                .fillna("")
                .astype(str)
                .str.contains("fallback", case=False)
            )
            metrics["title_fallback_rate"] = float(fallback_mask.sum() / total)
        else:
            metrics["title_fallback_rate"] = 0.0

        logger.debug("qc_metrics_computed", metrics=metrics)
        return metrics

    def _enforce_qc_thresholds(self, metrics: dict[str, float]) -> None:
        """Validate QC metrics against configured thresholds."""

        thresholds = self.config.qc.thresholds or {}
        if not thresholds:
            return

        failing: list[str] = []
        for metric_name, config in thresholds.items():
            if not isinstance(config, dict):
                continue

            value = metrics.get(metric_name)
            if value is None:
                continue

            min_threshold = config.get("min")
            max_threshold = config.get("max")
            severity = str(config.get("severity", "warning"))

            passed = True
            if min_threshold is not None and value < float(min_threshold):
                passed = False
            if max_threshold is not None and value > float(max_threshold):
                passed = False

            issue_payload = {
                "metric": metric_name,
                "value": value,
                "threshold_min": float(min_threshold) if min_threshold is not None else None,
                "threshold_max": float(max_threshold) if max_threshold is not None else None,
                "severity": severity if not passed else "info",
                "passed": passed,
            }
            self.record_validation_issue(issue_payload)

            if not passed and self._should_fail(severity):
                failing.append(metric_name)

        if failing:
            logger.error("qc_threshold_exceeded", failing=failing)
            raise ValueError("QC thresholds exceeded for metrics: " + ", ".join(sorted(failing)))

