"""Document Pipeline - ChEMBL document extraction with external enrichment."""

import os
import re
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import pandas as pd
import pandera as pa

from bioetl.adapters import (
    CrossrefAdapter,
    OpenAlexAdapter,
    PubMedAdapter,
    SemanticScholarAdapter,
)
from bioetl.adapters.base import AdapterConfig
from bioetl.config import PipelineConfig
from bioetl.core.api_client import APIConfig, UnifiedAPIClient
from bioetl.core.logger import UnifiedLogger
from bioetl.core.qc import compute_document_qc_metrics
from bioetl.pipelines.base import PipelineBase
from bioetl.pipelines.document_enrichment import merge_with_precedence
from bioetl.pipelines.document_enrichment import detect_conflicts
from bioetl.schemas import (
    DOCUMENT_NORMALIZED_COLUMN_ORDER,
    DocumentInputSchema,
    DocumentNormalizedSchema,
    DocumentRawSchema,
    DocumentSchema,
)
from bioetl.schemas.registry import schema_registry
from bioetl.normalizers import registry as normalizer_registry
from bioetl.core.hashing import generate_hash_business_key, generate_hash_row

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
            batch_size = chembl_source.get("batch_size", 10)
        else:
            base_url = "https://www.ebi.ac.uk/chembl/api/data"
            batch_size = 10

        chembl_config = APIConfig(
            name="chembl",
            base_url=base_url,
            cache_enabled=config.cache.enabled,
            cache_ttl=config.cache.ttl,
        )
        self.api_client = UnifiedAPIClient(chembl_config)
        self.batch_size = batch_size

        # Initialize external adapters if enabled
        self.external_adapters: dict[str, Any] = {}
        self._init_external_adapters()

        # Cache ChEMBL release version
        self._chembl_release = self._get_chembl_release()

    def get_validation_schemas(self) -> dict[str, pa.DataFrameModel]:
        """Provide Pandera schemas for validation stages."""

        return {
            "input": DocumentInputSchema,
            "raw": DocumentRawSchema,
            "normalized": DocumentNormalizedSchema,
        }

    def extract(self, input_file: Path | None = None) -> pd.DataFrame:
        """Extract document data from input file with optional enrichment."""
        if input_file is None:
            input_file = Path("data/input/documents.csv")

        logger.info("reading_input", path=input_file)

        if not input_file.exists():
            logger.warning("input_file_not_found", path=input_file)
            return pd.DataFrame(
                columns=[
                    "document_chembl_id",
                    "chembl_title",
                    "chembl_abstract",
                    "chembl_doi",
                    "chembl_pmid",
                    "chembl_journal",
                    "chembl_year",
                    "chembl_authors",
                ]
            )

        df = pd.read_csv(input_file)

        if df.empty:
            logger.info("extraction_completed", rows=0)
            return df

        validated_ids = self.validate_input(df[["document_chembl_id"]].copy())
        df["document_chembl_id"] = validated_ids["document_chembl_id"]

        string_normalizer = normalizer_registry

        if "pubmed_id" in df.columns:
            df["chembl_pmid"] = (
                pd.to_numeric(df["pubmed_id"], errors="coerce").astype("Int64")
            )
        else:
            df["chembl_pmid"] = pd.Series([pd.NA] * len(df), dtype="Int64")

        field_mapping = {
            "title": "chembl_title",
            "abstract": "chembl_abstract",
            "journal": "chembl_journal",
            "authors": "chembl_authors",
        }

        for original, mapped in field_mapping.items():
            if original in df.columns:
                df[mapped] = df[original].apply(
                    lambda value: string_normalizer.normalize("string", value)
                    if pd.notna(value)
                    else None
                )
            else:
                df[mapped] = None

        if "doi" in df.columns:
            df["chembl_doi"] = df["doi"].apply(
                lambda value: string_normalizer.normalize("string", value)
                if pd.notna(value)
                else None
            )
        else:
            df["chembl_doi"] = None

        if "year" in df.columns:
            df["chembl_year"] = pd.to_numeric(df["year"], errors="coerce").astype(
                "Int64"
            )
        else:
            df["chembl_year"] = pd.Series([pd.NA] * len(df), dtype="Int64")

        columns_to_drop = [
            col
            for col in ["title", "abstract", "journal", "authors", "doi", "pubmed_id", "year"]
            if col in df.columns
        ]
        if columns_to_drop:
            df = df.drop(columns=columns_to_drop)

        # Ensure required columns exist even if input lacked values
        required_raw_cols = [
            "chembl_title",
            "chembl_abstract",
            "chembl_doi",
            "chembl_pmid",
            "chembl_journal",
            "chembl_year",
            "chembl_authors",
        ]

        for col in required_raw_cols:
            if col not in df.columns:
                if col in {"chembl_pmid", "chembl_year"}:
                    df[col] = pd.Series([pd.NA] * len(df), dtype="Int64")
                else:
                    df[col] = None

        enrichment_enabled = any(
            hasattr(self.config.sources.get(source_name), "enabled")
            and self.config.sources[source_name].enabled
            for source_name in ["pubmed", "crossref", "openalex", "semantic_scholar"]
            if source_name in self.config.sources
        )

        if enrichment_enabled:
            logger.info("enrichment_enabled", note="Fetching data from external sources")
            df = self._enrich_with_external_sources(df)

        if "pubmed_id" in df.columns:
            df = df.drop(columns=["pubmed_id"])

        logger.info("extraction_completed", rows=len(df))
        return df

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

    FIELD_PRECEDENCE = {
        "title": [
            "pubmed_article_title",
            "chembl_title",
            "crossref_title",
            "openalex_title",
            "semantic_scholar_title",
        ],
        "abstract": ["pubmed_abstract", "chembl_abstract"],
        "journal": ["pubmed_journal", "chembl_journal", "semantic_scholar_journal"],
        "doi": [
            "chembl_doi",
            "pubmed_doi",
            "crossref_doi",
            "openalex_doi",
            "semantic_scholar_doi",
        ],
        "pubmed_id": ["chembl_pmid", "pubmed_pmid", "semantic_scholar_pubmed_id"],
        "authors": [
            "pubmed_authors",
            "chembl_authors",
            "crossref_authors",
            "openalex_authors",
            "semantic_scholar_authors",
        ],
    }

    @staticmethod
    def _select_field(row: pd.Series, candidates: list[str]) -> tuple[Any, str | None]:
        """Select first non-empty value from candidates."""

        for column in candidates:
            if column in row.index:
                value = row[column]
                if pd.notna(value) and str(value).strip():
                    return value, column
        return None, None

    @staticmethod
    def _normalize_doi(value: Any) -> str | None:
        """Normalize DOI string."""

        if not isinstance(value, str):
            return None
        doi = value.strip().lower()
        if not doi:
            return None
        doi = re.sub(r"^https?://(dx\.)?doi.org/", "", doi)
        doi = doi.replace("doi:", "")
        doi = doi.strip()
        return doi or None

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize enriched document data and compute metadata."""

        if df.empty:
            return df

        df = df.copy()

        # Detect conflicts between source-provided identifiers
        df = df.apply(detect_conflicts, axis=1)

        for field, candidates in self.FIELD_PRECEDENCE.items():
            selections = df.apply(
                lambda row: self._select_field(row, candidates), axis=1
            )
            df[field] = selections.map(lambda item: item[0] if item else None)
            df[f"{field}_source"] = selections.map(
                lambda item: item[1] if item else None
            )

        primary_title_source = self.FIELD_PRECEDENCE["title"][0]
        if "title_source" in df.columns:
            df["qc_flag_title_fallback_used"] = df["title_source"].apply(
                lambda src: 0 if src == primary_title_source else 1
            )
            df.loc[df["title_source"].isna(), "qc_flag_title_fallback_used"] = 1
        else:
            df["qc_flag_title_fallback_used"] = 1

        if "chembl_year" in df.columns and "year" not in df.columns:
            df["year"] = df["chembl_year"]

        if "year" in df.columns:
            df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
        else:
            df["year"] = pd.Series([pd.NA] * len(df), dtype="Int64")

        if "pubmed_id" in df.columns:
            df["pubmed_id"] = pd.to_numeric(df["pubmed_id"], errors="coerce").astype(
                "Int64"
            )
        else:
            df["pubmed_id"] = pd.Series([pd.NA] * len(df), dtype="Int64")

        df["doi_clean"] = df["doi"].apply(self._normalize_doi)

        def _count_authors(value: Any) -> Any:
            if not isinstance(value, str):
                return pd.NA
            tokens = [token.strip() for token in re.split(r"[,;]", value) if token.strip()]
            return len(tokens) if tokens else pd.NA

        df["authors_count"] = df["authors"].apply(_count_authors).astype("Int64")

        if "conflict_doi" not in df.columns:
            df["conflict_doi"] = False
        if "conflict_pmid" not in df.columns:
            df["conflict_pmid"] = False

        df["conflict_doi"] = df["conflict_doi"].astype("boolean")
        df["conflict_pmid"] = df["conflict_pmid"].astype("boolean")

        df["qc_flag_title_fallback_used"] = (
            pd.to_numeric(df["qc_flag_title_fallback_used"], errors="coerce")
            .fillna(0)
            .astype("Int64")
        )

        source_columns = [col for col in df.columns if col.endswith("_source")]
        if source_columns:
            df = df.drop(columns=source_columns)

        df["pipeline_version"] = "1.0.0"
        df["source_system"] = "chembl"
        df["chembl_release"] = self._chembl_release
        df["extracted_at"] = pd.Timestamp.now(tz="UTC").isoformat()

        required_columns = set(DOCUMENT_NORMALIZED_COLUMN_ORDER)
        extra_columns = [col for col in df.columns if col not in required_columns]
        if extra_columns:
            df = df.drop(columns=extra_columns)

        # Ensure columns required by schema exist
        for column in DOCUMENT_NORMALIZED_COLUMN_ORDER:
            if column not in df.columns:
                df[column] = None

        df = df.sort_values("document_chembl_id").reset_index(drop=True)
        df["index"] = df.index
        df["hash_business_key"] = df["document_chembl_id"].apply(
            generate_hash_business_key
        )
        df["hash_row"] = df.apply(
            lambda row: generate_hash_row(
                {
                    col: (row[col] if not pd.isna(row[col]) else None)
                    for col in DOCUMENT_NORMALIZED_COLUMN_ORDER
                    if col != "hash_row"
                }
            ),
            axis=1,
        )

        df = df[DOCUMENT_NORMALIZED_COLUMN_ORDER]
        return df

    def compute_qc_metrics(self, df: pd.DataFrame) -> dict[str, float]:
        """Compute QC metrics for the normalized document dataset."""

        return compute_document_qc_metrics(df)

