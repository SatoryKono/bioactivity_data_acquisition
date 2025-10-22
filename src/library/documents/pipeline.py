"""Refactored document ETL pipeline using PipelineBase."""

from __future__ import annotations

import logging
import os
from typing import Any

import pandas as pd

from library.clients.chembl import ChEMBLClient
from library.clients.crossref import CrossrefClient
from library.clients.openalex import OpenAlexClient
from library.clients.pubmed import PubMedClient
from library.clients.semantic_scholar import SemanticScholarClient
from library.common.pipeline_base import ETLResult, PipelineBase
from library.documents.config import DocumentConfig
from library.documents.normalize import DocumentNormalizer
from library.documents.quality import DocumentQualityFilter
from library.documents.validate import DocumentValidator
from library.tools.citation_formatter import add_citation_column
from library.tools.journal_normalizer import normalize_journal_columns

logger = logging.getLogger(__name__)


class DocumentPipeline(PipelineBase[DocumentConfig]):
    """Document ETL pipeline using unified PipelineBase."""
    
    def __init__(self, config: DocumentConfig) -> None:
        """Initialize document pipeline with configuration."""
        super().__init__(config)
        self.validator = DocumentValidator(config.model_dump() if hasattr(config, 'model_dump') else {})
        self.normalizer = DocumentNormalizer(config.model_dump() if hasattr(config, 'model_dump') else {})
        self.quality_filter = DocumentQualityFilter(config.model_dump() if hasattr(config, 'model_dump') else {})
    
    def _setup_clients(self) -> None:
        """Initialize HTTP clients for document sources."""
        self.clients = {}
        
        # ChEMBL client
        if "chembl" in self.config.sources and self.config.sources["chembl"].enabled:
            self.clients["chembl"] = self._create_chembl_client()
        
        # Crossref client
        if "crossref" in self.config.sources and self.config.sources["crossref"].enabled:
            self.clients["crossref"] = self._create_crossref_client()
        
        # OpenAlex client
        if "openalex" in self.config.sources and self.config.sources["openalex"].enabled:
            self.clients["openalex"] = self._create_openalex_client()
        
        # PubMed client
        if "pubmed" in self.config.sources and self.config.sources["pubmed"].enabled:
            self.clients["pubmed"] = self._create_pubmed_client()
        
        # Semantic Scholar client
        if "semanticscholar" in self.config.sources and self.config.sources["semanticscholar"].enabled:
            self.clients["semanticscholar"] = self._create_semantic_scholar_client()
    
    def _create_chembl_client(self) -> ChEMBLClient:
        """Create ChEMBL client."""
        from library.config import APIClientConfig, RateLimitSettings, RetrySettings
        
        source_config = self.config.sources["chembl"]
        timeout = source_config.http.timeout_sec or self.config.http.global_.timeout_sec
        timeout = max(timeout, 60.0)  # At least 60 seconds for ChEMBL
        
        headers = self._get_headers("chembl")
        headers.update(self.config.http.global_.headers)
        headers.update(source_config.http.headers)
        
        # Process secret placeholders
        processed_headers = self._process_headers(headers)
        
        client_config = APIClientConfig(
            name="chembl",
            base_url=source_config.http.base_url,
            timeout_sec=timeout,
            retries=RetrySettings(
                total=source_config.http.retries["total"],
                backoff_multiplier=source_config.http.retries["backoff_multiplier"],
                backoff_max=source_config.http.retries["backoff_max"],
            ),
            rate_limit=RateLimitSettings(
                max_calls=source_config.rate_limit["max_calls"],
                period=source_config.rate_limit["period"],
            ),
            headers=processed_headers,
            verify_ssl=source_config.http.verify_ssl,
            follow_redirects=source_config.http.follow_redirects,
        )
        
        return ChEMBLClient(client_config)
    
    def _create_crossref_client(self) -> CrossrefClient:
        """Create Crossref client."""
        from library.config import APIClientConfig, RateLimitSettings, RetrySettings
        
        source_config = self.config.sources["crossref"]
        timeout = source_config.http.timeout_sec or self.config.http.global_.timeout_sec
        
        headers = self._get_headers("crossref")
        headers.update(self.config.http.global_.headers)
        headers.update(source_config.http.headers)
        
        processed_headers = self._process_headers(headers)
        
        client_config = APIClientConfig(
            name="crossref",
            base_url=source_config.http.base_url,
            timeout_sec=timeout,
            retries=RetrySettings(
                total=source_config.http.retries["total"],
                backoff_multiplier=source_config.http.retries["backoff_multiplier"],
                backoff_max=source_config.http.retries["backoff_max"],
            ),
            rate_limit=RateLimitSettings(
                max_calls=source_config.rate_limit["max_calls"],
                period=source_config.rate_limit["period"],
            ),
            headers=processed_headers,
            verify_ssl=source_config.http.verify_ssl,
            follow_redirects=source_config.http.follow_redirects,
        )
        
        return CrossrefClient(client_config)
    
    def _create_openalex_client(self) -> OpenAlexClient:
        """Create OpenAlex client."""
        from library.config import APIClientConfig, RateLimitSettings, RetrySettings
        
        source_config = self.config.sources["openalex"]
        timeout = source_config.http.timeout_sec or self.config.http.global_.timeout_sec
        
        headers = self._get_headers("openalex")
        headers.update(self.config.http.global_.headers)
        headers.update(source_config.http.headers)
        
        processed_headers = self._process_headers(headers)
        
        client_config = APIClientConfig(
            name="openalex",
            base_url=source_config.http.base_url,
            timeout_sec=timeout,
            retries=RetrySettings(
                total=source_config.http.retries["total"],
                backoff_multiplier=source_config.http.retries["backoff_multiplier"],
                backoff_max=source_config.http.retries["backoff_max"],
            ),
            rate_limit=RateLimitSettings(
                max_calls=source_config.rate_limit["max_calls"],
                period=source_config.rate_limit["period"],
            ),
            headers=processed_headers,
            verify_ssl=source_config.http.verify_ssl,
            follow_redirects=source_config.http.follow_redirects,
        )
        
        return OpenAlexClient(client_config)
    
    def _create_pubmed_client(self) -> PubMedClient:
        """Create PubMed client."""
        from library.config import APIClientConfig, RateLimitSettings, RetrySettings
        
        source_config = self.config.sources["pubmed"]
        timeout = source_config.http.timeout_sec or self.config.http.global_.timeout_sec
        
        headers = self._get_headers("pubmed")
        headers.update(self.config.http.global_.headers)
        headers.update(source_config.http.headers)
        
        processed_headers = self._process_headers(headers)
        
        client_config = APIClientConfig(
            name="pubmed",
            base_url=source_config.http.base_url,
            timeout_sec=timeout,
            retries=RetrySettings(
                total=source_config.http.retries["total"],
                backoff_multiplier=source_config.http.retries["backoff_multiplier"],
                backoff_max=source_config.http.retries["backoff_max"],
            ),
            rate_limit=RateLimitSettings(
                max_calls=source_config.rate_limit["max_calls"],
                period=source_config.rate_limit["period"],
            ),
            headers=processed_headers,
            verify_ssl=source_config.http.verify_ssl,
            follow_redirects=source_config.http.follow_redirects,
        )
        
        return PubMedClient(client_config)
    
    def _create_semantic_scholar_client(self) -> SemanticScholarClient:
        """Create Semantic Scholar client."""
        from library.config import APIClientConfig, RateLimitSettings, RetrySettings
        
        source_config = self.config.sources["semanticscholar"]
        timeout = source_config.http.timeout_sec or self.config.http.global_.timeout_sec
        
        headers = self._get_headers("semanticscholar")
        headers.update(self.config.http.global_.headers)
        headers.update(source_config.http.headers)
        
        processed_headers = self._process_headers(headers)
        
        client_config = APIClientConfig(
            name="semanticscholar",
            base_url=source_config.http.base_url,
            timeout_sec=timeout,
            retries=RetrySettings(
                total=source_config.http.retries["total"],
                backoff_multiplier=source_config.http.retries["backoff_multiplier"],
                backoff_max=source_config.http.retries["backoff_max"],
            ),
            rate_limit=RateLimitSettings(
                max_calls=source_config.rate_limit["max_calls"],
                period=source_config.rate_limit["period"],
            ),
            headers=processed_headers,
            verify_ssl=source_config.http.verify_ssl,
            follow_redirects=source_config.http.follow_redirects,
        )
        
        return SemanticScholarClient(client_config)
    
    def _get_headers(self, source: str) -> dict[str, str]:
        """Get default headers for a source."""
        headers = {
            "Accept": "application/json",
            "User-Agent": "bioactivity-data-acquisition/0.1.0",
        }
        
        if source == "crossref":
            headers["Accept"] = "application/vnd.crossref.unixsd+xml"
        elif source == "pubmed":
            headers["Accept"] = "application/xml"
        
        return headers
    
    def _process_headers(self, headers: dict[str, str]) -> dict[str, str]:
        """Process headers with secret placeholders."""
        processed = {}
        for key, value in headers.items():
            if isinstance(value, str) and value.startswith("${") and value.endswith("}"):
                # Handle environment variable substitution
                env_var = value[2:-1]
                processed[key] = os.getenv(env_var, value)
            else:
                processed[key] = value
        return processed
    
    def extract(self, input_data: pd.DataFrame) -> pd.DataFrame:
        """Extract document data from multiple sources."""
        logger.info(f"Extracting document data for {len(input_data)} documents")
        
        # Normalize input columns first
        normalized_data = self._normalize_columns(input_data)
        
        # Apply limit if specified
        if self.config.runtime.limit is not None:
            normalized_data = normalized_data.head(self.config.runtime.limit)
        
        # Check for duplicates
        duplicates = normalized_data["document_chembl_id"].duplicated()
        if duplicates.any():
            raise ValueError("Duplicate document_chembl_id values detected")
        
        # Extract data from each enabled source
        extracted_data = normalized_data.copy()
        
        for source_name, client in self.clients.items():
            try:
                logger.info(f"Extracting data from {source_name}")
                source_data = self._extract_from_source(client, source_name, normalized_data)
                extracted_data = self._merge_source_data(extracted_data, source_data, source_name)
            except Exception as e:
                logger.error(f"Failed to extract from {source_name}: {e}")
                if not self.config.runtime.allow_incomplete_sources:
                    raise
        
        logger.info(f"Extracted data for {len(extracted_data)} documents")
        return extracted_data
    
    def _normalize_columns(self, frame: pd.DataFrame) -> pd.DataFrame:
        """Normalize input columns."""
        normalized = frame.copy()
        
        # Remove deprecated columns
        if 'postcodes' in normalized.columns:
            normalized = normalized.drop(columns=['postcodes'])
        
        # Map column names to expected schema names
        column_mapping = {
            'pubmed_id': 'document_pubmed_id',
            'authors': 'pubmed_authors',
            'classification': 'document_classification',
            'document_contains_external_links': 'referenses_on_previous_experiments',
            'is_experimental_doc': 'original_experimental_document',
            'title': 'chembl_title',
            'journal': 'chembl_journal',
            'volume': 'chembl_volume',
            'issue': 'chembl_issue',
            'year': 'chembl_year'
        }
        
        # Rename columns
        for old_name, new_name in column_mapping.items():
            if old_name in normalized.columns:
                normalized[new_name] = normalized[old_name]
        
        # Check required columns
        required_columns = {"document_chembl_id", "doi", "title"}
        present = set(normalized.columns)
        missing = required_columns - present
        if missing:
            raise ValueError(f"Missing required columns: {missing}")
        
        # Normalize document_chembl_id
        if "document_chembl_id" in normalized.columns:
            normalized["document_chembl_id"] = normalized["document_chembl_id"].astype(str).str.strip()
            normalized = normalized[normalized["document_chembl_id"] != ""]
        
        return normalized.sort_values("document_chembl_id").reset_index(drop=True)
    
    def _extract_from_source(self, client: Any, source_name: str, data: pd.DataFrame) -> pd.DataFrame:
        """Extract data from a specific source."""
        # This would contain the actual extraction logic for each source
        # For now, return empty DataFrame as placeholder
        return pd.DataFrame()
    
    def _merge_source_data(self, base_data: pd.DataFrame, source_data: pd.DataFrame, source_name: str) -> pd.DataFrame:
        """Merge data from a source into base data."""
        # This would contain the actual merging logic
        # For now, return base data as placeholder
        return base_data
    
    def normalize(self, raw_data: pd.DataFrame) -> pd.DataFrame:
        """Normalize document data."""
        logger.info("Normalizing document data")
        
        # Apply document normalization
        normalized_data = self.normalizer.normalize_documents(raw_data)
        
        # Apply journal normalization
        normalized_data = normalize_journal_columns(normalized_data)
        
        # Add citation formatting
        normalized_data = add_citation_column(normalized_data)
        
        logger.info(f"Normalized {len(normalized_data)} documents")
        return normalized_data
    
    def validate(self, data: pd.DataFrame) -> pd.DataFrame:
        """Validate document data."""
        logger.info("Validating document data")
        
        # Validate normalized data
        validated_data = self.validator.validate_normalized(data)
        
        logger.info(f"Validated {len(validated_data)} documents")
        return validated_data
    
    def filter_quality(self, data: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Filter documents by quality."""
        logger.info("Filtering documents by quality")
        
        # Apply quality filters
        accepted_data, rejected_data = self.quality_filter.apply_moderate_quality_filter(data)
        
        logger.info(f"Quality filtering: {len(accepted_data)} accepted, {len(rejected_data)} rejected")
        return accepted_data, rejected_data
    
    def _build_metadata(self, data: pd.DataFrame) -> dict[str, Any]:
        """Build metadata for document pipeline."""
        # Create base metadata dictionary
        metadata = {
            "pipeline_name": "documents",
            "pipeline_version": "2.0.0",
            "entity_type": "documents",
            "sources_enabled": [name for name, source in self.config.sources.items() if source.enabled],
            "total_documents": len(data),
            "extraction_timestamp": pd.Timestamp.now().isoformat(),
            "config": self.config.model_dump() if hasattr(self.config, 'model_dump') else {},
        }
        
        return metadata
