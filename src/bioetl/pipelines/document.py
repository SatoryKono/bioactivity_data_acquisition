"""Document Pipeline - ChEMBL document extraction with external enrichment."""

import os
import re
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Sequence
from urllib.parse import urlencode

import pandas as pd
import requests

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
from bioetl.schemas import DocumentSchema, DocumentInputSchema
from bioetl.schemas.registry import schema_registry

logger = UnifiedLogger.get(__name__)

# Register schema
schema_registry.register("document", "1.0.0", DocumentSchema)


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
        if isinstance(chembl_source, dict):
            base_url = chembl_source.get("base_url", "https://www.ebi.ac.uk/chembl/api/data")
            batch_size = int(chembl_source.get("batch_size", 10) or 10)
            max_url_length = int(chembl_source.get("max_url_length", 1800) or 1800)
        else:
            base_url = "https://www.ebi.ac.uk/chembl/api/data"
            batch_size = 10
            max_url_length = 1800

        chembl_config = APIConfig(
            name="chembl",
            base_url=base_url,
            cache_enabled=config.cache.enabled,
            cache_ttl=config.cache.ttl,
        )
        self.api_client = UnifiedAPIClient(chembl_config)
        self.batch_size = batch_size
        self.max_url_length = max_url_length
        self.max_batch_size = 25
        self._document_cache: dict[str, dict[str, Any]] = {}

        # Initialize external adapters if enabled
        self.external_adapters: dict[str, Any] = {}
        self._init_external_adapters()

        # Cache ChEMBL release version
        self._chembl_release = self._get_chembl_release()

    def extract(self, input_file: Path | None = None) -> pd.DataFrame:
        """Extract document data from input file with optional enrichment."""
        if input_file is None:
            input_file = Path("data/input/documents.csv")

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

        records = self._fetch_documents(valid_ids)
        if not records:
            logger.warning("no_records_fetched", ids=len(valid_ids))
            return pd.DataFrame(columns=["document_chembl_id"])

        result_df = pd.DataFrame(records)
        logger.info("extraction_completed", rows=len(result_df))
        return result_df

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

        if "doi" in chembl_df.columns:
            dois = chembl_df["doi"].dropna().tolist()

        logger.info("enrichment_data", pmids_count=len(pmids), dois_count=len(dois), sample_pmids=pmids[:3] if pmids else [])

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
                fetched_ids: set[str] = set()
                for record in fetched:
                    document_id = record.get("document_chembl_id")
                    if not document_id:
                        continue
                    document_id = str(document_id)
                    fetched_ids.add(document_id)
                    self._document_cache[self._document_cache_key(document_id)] = record
                results.extend(fetched)

                missing_ids = [document_id for document_id in to_fetch if document_id not in fetched_ids]
                for document_id in missing_ids:
                    fallback = self._create_fallback_row(
                        document_id,
                        "E_NOT_FOUND",
                        "Document not found in ChEMBL",
                    )
                    self._document_cache[self._document_cache_key(document_id)] = fallback
                    results.append(fallback)
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

        if enrichment_enabled:
            logger.info("enrichment_enabled", note="Fetching data from external sources")
            df = self._enrich_with_external_sources(df)

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
            from bioetl.schemas import DocumentSchema
            expected_cols = DocumentSchema.column_order

            # Add missing columns with None values
            for col in expected_cols:
                if col not in df.columns:
                    df[col] = None

            # Reorder columns according to schema
            df = df[expected_cols]

        return df

    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate document data against schema with strict validation."""
        if df.empty:
            logger.info("validation_skipped_empty", rows=0)
            return df

        try:
            # Check for duplicates
            duplicate_count = df["document_chembl_id"].duplicated().sum() if "document_chembl_id" in df.columns else 0
            if duplicate_count > 0:
                logger.warning("duplicates_found", count=duplicate_count)
                df = df.drop_duplicates(subset=["document_chembl_id"], keep="first")

            # Validation: doc_type обязателен если есть данные от источника
            sources_to_validate = [
                ("pubmed", ["pubmed_doc_type"], ["pubmed_article_title", "pubmed_abstract"]),
                ("crossref", ["crossref_doc_type"], ["crossref_title", "crossref_authors"]),
                ("openalex", ["openalex_doc_type"], ["openalex_title", "openalex_authors"]),
                ("semantic_scholar", ["semantic_scholar_doc_type"], ["semantic_scholar_title", "semantic_scholar_abstract"]),
            ]

            validation_errors = []
            for source_name, required_fields, actual_field_names in sources_to_validate:
                # Check if any data exists from this source
                has_source_data = any(
                    col in df.columns and df[col].notna().any()
                    for col in actual_field_names
                    if col in df.columns
                )

                if has_source_data:
                    # Validate doc_type is present (DOI is optional)
                    for required_field in required_fields:
                        if required_field not in df.columns or df[required_field].isna().all():
                            validation_errors.append(f"{source_name}: {required_field} обязателен при наличии данных")

            if validation_errors:
                error_msg = "; ".join(validation_errors)
                logger.error("validation_failed", errors=error_msg)
                raise ValueError(f"Валидация не прошла: {error_msg}")

            logger.info("validation_completed", rows=len(df), duplicates_removed=duplicate_count)
            return df
        except Exception as e:
            logger.error("validation_failed", error=str(e))
            raise

