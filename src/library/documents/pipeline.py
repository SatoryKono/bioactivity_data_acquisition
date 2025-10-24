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
from library.common.pipeline_base import PipelineBase
from library.documents.config import DocumentConfig
from library.documents.diagnostics import DocumentDiagnostics
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
        self.validator = DocumentValidator(config.model_dump() if hasattr(config, 'model_dump') else config if isinstance(config, dict) else {})
        self.normalizer = DocumentNormalizer(config.model_dump() if hasattr(config, 'model_dump') else config if isinstance(config, dict) else {})
        self.quality_filter = DocumentQualityFilter(config.model_dump() if hasattr(config, 'model_dump') else config if isinstance(config, dict) else {})
        self.diagnostics = DocumentDiagnostics()
    
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
        if "semantic_scholar" in self.config.sources and self.config.sources["semantic_scholar"].enabled:
            self.clients["semantic_scholar"] = self._create_semantic_scholar_client()
    
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
            base_url=source_config.http.base_url or 'https://www.ebi.ac.uk/chembl/api/data',
            timeout_sec=timeout,
            retries=RetrySettings(
                total=source_config.http.retries.get("total", 3),
                backoff_multiplier=source_config.http.retries.get("backoff_multiplier", 2.0),
                backoff_max=source_config.http.retries.get("backoff_max", 60.0),
            ),
            rate_limit=RateLimitSettings(
                max_calls=source_config.rate_limit.get("max_calls", 10),
                period=source_config.rate_limit.get("period", 1.0),
            ),
            headers=processed_headers,
            verify_ssl=getattr(source_config.http, 'verify_ssl', None) or True,
            follow_redirects=getattr(source_config.http, 'follow_redirects', None) or True,
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
                total=source_config.http.retries.get("total", 3),
                backoff_multiplier=source_config.http.retries.get("backoff_multiplier", 2.0),
                backoff_max=source_config.http.retries.get("backoff_max", 60.0),
            ),
            rate_limit=RateLimitSettings(
                max_calls=source_config.rate_limit.get("max_calls", 10),
                period=source_config.rate_limit.get("period", 1.0),
            ),
            headers=processed_headers,
            verify_ssl=getattr(source_config.http, 'verify_ssl', None) or True,
            follow_redirects=getattr(source_config.http, 'follow_redirects', None) or True,
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
                total=source_config.http.retries.get("total", 3),
                backoff_multiplier=source_config.http.retries.get("backoff_multiplier", 2.0),
                backoff_max=source_config.http.retries.get("backoff_max", 60.0),
            ),
            rate_limit=RateLimitSettings(
                max_calls=source_config.rate_limit.get("max_calls", 10),
                period=source_config.rate_limit.get("period", 1.0),
            ),
            headers=processed_headers,
            verify_ssl=getattr(source_config.http, 'verify_ssl', None) or True,
            follow_redirects=getattr(source_config.http, 'follow_redirects', None) or True,
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
                total=source_config.http.retries.get("total", 3),
                backoff_multiplier=source_config.http.retries.get("backoff_multiplier", 2.0),
                backoff_max=source_config.http.retries.get("backoff_max", 60.0),
            ),
            rate_limit=RateLimitSettings(
                max_calls=source_config.rate_limit.get("max_calls", 10),
                period=source_config.rate_limit.get("period", 1.0),
            ),
            headers=processed_headers,
            verify_ssl=getattr(source_config.http, 'verify_ssl', None) or True,
            follow_redirects=getattr(source_config.http, 'follow_redirects', None) or True,
        )
        
        return PubMedClient(client_config)
    
    def _create_semantic_scholar_client(self) -> SemanticScholarClient:
        """Create Semantic Scholar client."""
        from library.config import APIClientConfig, RateLimitSettings, RetrySettings
        
        source_config = self.config.sources["semantic_scholar"]
        timeout = source_config.http.timeout_sec or self.config.http.global_.timeout_sec
        
        headers = self._get_headers("semantic_scholar")
        headers.update(self.config.http.global_.headers)
        headers.update(source_config.http.headers)
        
        processed_headers = self._process_headers(headers)
        
        client_config = APIClientConfig(
            name="semantic_scholar",
            base_url=source_config.http.base_url,
            timeout_sec=timeout,
            retries=RetrySettings(
                total=source_config.http.retries.get("total", 3),
                backoff_multiplier=source_config.http.retries.get("backoff_multiplier", 2.0),
                backoff_max=source_config.http.retries.get("backoff_max", 60.0),
            ),
            rate_limit=RateLimitSettings(
                max_calls=source_config.rate_limit.get("max_calls", 10),
                period=source_config.rate_limit.get("period", 1.0),
            ),
            headers=processed_headers,
            verify_ssl=getattr(source_config.http, 'verify_ssl', None) or True,
            follow_redirects=getattr(source_config.http, 'follow_redirects', None) or True,
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
                
                # Логируем статистику извлечения
                if not source_data.empty:
                    logger.info(f"Successfully extracted {len(source_data)} records from {source_name}")
                else:
                    logger.warning(f"No data extracted from {source_name}")
                    
            except Exception as e:
                logger.error(f"Failed to extract from {source_name}: {e}", exc_info=True)
                
                # Создаем DataFrame с ошибками для graceful degradation
                error_data = self._create_error_dataframe(normalized_data, source_name, str(e))
                
                # Простое объединение без сложной логики для ошибок
                try:
                    extracted_data = self._merge_source_data(extracted_data, error_data, source_name)
                except Exception as merge_error:
                    logger.error(f"Failed to merge error data from {source_name}: {merge_error}")
                    # Добавляем колонки с ошибками напрямую
                    error_column = f"{source_name}_error"
                    extracted_data[error_column] = str(e)
                
                if not self.config.runtime.allow_incomplete_sources:
                    raise
        
        logger.info(f"Extracted data for {len(extracted_data)} documents")
        
        # Генерируем диагностический отчет после extract
        try:
            logger.info("Generating diagnostic report for extracted data")
            diagnostic_report = self.diagnostics.analyze_dataframe(extracted_data)
            summary_report = self.diagnostics.generate_summary_report(extracted_data)
            
            # Логируем краткий отчет
            logger.info("=== EXTRACTION DIAGNOSTICS ===")
            logger.info(summary_report)
            
            # Сохраняем полный отчет
            report_path = self.diagnostics.save_report(diagnostic_report)
            logger.info(f"Full diagnostic report saved to {report_path}")
            
        except Exception as e:
            logger.warning(f"Failed to generate diagnostic report: {e}")
        
        return extracted_data
    
    def _normalize_columns(self, frame: pd.DataFrame) -> pd.DataFrame:
        """Normalize input columns."""
        normalized = frame.copy()
        
        # Remove deprecated columns
        if 'postcodes' in normalized.columns:
            normalized = normalized.drop(columns=['postcodes'])
        
        # Map column names to expected schema names
        column_mapping = {
            'DOI': 'doi',  # Map DOI to doi
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
        
        # Rename columns and remove old ones
        columns_to_drop = []
        for old_name, new_name in column_mapping.items():
            if old_name in normalized.columns:
                normalized[new_name] = normalized[old_name]
                columns_to_drop.append(old_name)
        
        # Drop old column names to avoid confusion
        normalized = normalized.drop(columns=columns_to_drop, errors='ignore')
        
        # Add diagnostic logging
        logger.info(f"Normalized columns: {list(normalized.columns)}")
        if 'document_pubmed_id' in normalized.columns:
            pmid_count = normalized['document_pubmed_id'].notna().sum()
            logger.info(f"Records with PMID: {pmid_count}/{len(normalized)}")
        if 'doi' in normalized.columns:
            doi_count = normalized['doi'].notna().sum()
            logger.info(f"Records with DOI: {doi_count}/{len(normalized)}")
        
        # Check required columns
        required_columns = {"document_chembl_id", "doi"}
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
        from library.documents.extract import (
            extract_from_chembl,
            extract_from_crossref,
            extract_from_openalex,
            extract_from_pubmed,
            extract_from_semantic_scholar,
        )
        
        if source_name == "pubmed":
            # Извлекаем PMID из данных
            pmids = []
            if "document_pubmed_id" in data.columns:
                pmids = data["document_pubmed_id"].dropna().astype(str).unique().tolist()
            elif "pubmed_id" in data.columns:
                pmids = data["pubmed_id"].dropna().astype(str).unique().tolist()
            
            if pmids:
                batch_size = getattr(self.config.sources.get("pubmed", {}), "batch_size", 200)
                return extract_from_pubmed(client, pmids, batch_size)
            else:
                logger.warning("No PMIDs found for PubMed extraction")
                return pd.DataFrame()
        
        elif source_name == "crossref":
            # Извлекаем DOI из данных
            dois = []
            if "doi" in data.columns:
                dois = data["doi"].dropna().astype(str).unique().tolist()
            elif "DOI" in data.columns:
                dois = data["DOI"].dropna().astype(str).unique().tolist()
            
            if dois:
                batch_size = getattr(self.config.sources.get("crossref", {}), "batch_size", 100)
                return extract_from_crossref(client, dois, batch_size)
            else:
                logger.warning("No DOIs found for Crossref extraction")
                return pd.DataFrame()
        
        elif source_name == "openalex":
            # Извлекаем PMID для OpenAlex
            pmids = []
            if "document_pubmed_id" in data.columns:
                pmids = data["document_pubmed_id"].dropna().astype(str).unique().tolist()
            elif "pubmed_id" in data.columns:
                pmids = data["pubmed_id"].dropna().astype(str).unique().tolist()
            
            if pmids:
                batch_size = getattr(self.config.sources.get("openalex", {}), "batch_size", 50)
                return extract_from_openalex(client, pmids, batch_size)
            else:
                logger.warning("No PMIDs found for OpenAlex extraction")
                return pd.DataFrame()
        
        elif source_name == "semantic_scholar":
            # Извлекаем PMID для Semantic Scholar
            pmids = []
            if "document_pubmed_id" in data.columns:
                pmids = data["document_pubmed_id"].dropna().astype(str).unique().tolist()
            elif "pubmed_id" in data.columns:
                pmids = data["pubmed_id"].dropna().astype(str).unique().tolist()
            
            if pmids:
                batch_size = getattr(self.config.sources.get("semantic_scholar", {}), "batch_size", 100)
                return extract_from_semantic_scholar(client, pmids, batch_size)
            else:
                logger.warning("No PMIDs found for Semantic Scholar extraction")
                return pd.DataFrame()
        
        elif source_name == "chembl":
            # Извлекаем ChEMBL ID
            chembl_ids = []
            if "document_chembl_id" in data.columns:
                chembl_ids = data["document_chembl_id"].dropna().astype(str).unique().tolist()
            
            if chembl_ids:
                batch_size = getattr(self.config.sources.get("chembl", {}), "batch_size", 100)
                return extract_from_chembl(client, chembl_ids, batch_size)
            else:
                logger.warning("No ChEMBL IDs found for ChEMBL extraction")
                return pd.DataFrame()
        
        else:
            logger.warning(f"Unknown source: {source_name}")
            return pd.DataFrame()
    
    def _merge_source_data(self, base_data: pd.DataFrame, source_data: pd.DataFrame, source_name: str) -> pd.DataFrame:
        """Merge data from a source into base data."""
        from library.documents.merge import merge_source_data
        
        if source_data.empty:
            logger.warning(f"No data to merge from {source_name}")
            return base_data
        
        # Определить ключ объединения
        if source_name in ["pubmed", "openalex", "semantic_scholar"]:
            join_key = "document_pubmed_id"
        elif source_name == "crossref":
            join_key = "doi"
        elif source_name == "chembl":
            join_key = "document_chembl_id"
        else:
            logger.warning(f"Unknown source {source_name}")
            return base_data
        
        # Объединить данные
        merged = merge_source_data(
            base_df=base_data,
            source_df=source_data,
            source_name=source_name,
            join_key=join_key
        )
        
        logger.info(f"Merged {len(source_data)} records from {source_name}")
        return merged
    
    def _create_error_dataframe(self, base_data: pd.DataFrame, source_name: str, error_message: str) -> pd.DataFrame:
        """Создать DataFrame с ошибками для graceful degradation."""
        error_column = f"{source_name}_error"
        
        # Создаем DataFrame с ошибками для всех записей
        error_data = base_data[["document_chembl_id"]].copy()
        error_data[error_column] = error_message
        
        # Добавляем пустые поля для источника
        source_fields = {
            "pubmed": ["PubMed.PMID", "PubMed.Error"],
            "crossref": ["crossref.DOI", "crossref.Error"],
            "openalex": ["OpenAlex.PMID", "OpenAlex.Error"],
            "semantic_scholar": ["scholar.PMID", "scholar.Error"],
            "chembl": ["ChEMBL.document_chembl_id", "ChEMBL.Error"]
        }
        
        if source_name in source_fields:
            for field in source_fields[source_name]:
                if field.endswith("Error"):
                    error_data[field] = error_message
                else:
                    error_data[field] = ""
        
        return error_data
    
    def normalize(self, raw_data: pd.DataFrame) -> pd.DataFrame:
        """Normalize document data."""
        from library.documents.merge import add_document_sortorder, compute_publication_date, convert_data_types
        
        logger.info("Normalizing document data")
        
        # Apply document normalization
        normalized_data = self.normalizer.normalize_documents(raw_data)
        
        # Apply journal normalization
        normalized_data = normalize_journal_columns(normalized_data)
        
        # Add citation formatting
        normalized_data = add_citation_column(normalized_data)
        
        # Compute publication date from all sources
        normalized_data = compute_publication_date(normalized_data)
        
        # Add document sort order
        normalized_data = add_document_sortorder(normalized_data)
        
        # Convert data types to match schema
        normalized_data = convert_data_types(normalized_data)
        
        logger.info(f"Normalized {len(normalized_data)} documents")
        
        # Генерируем финальный диагностический отчет
        try:
            logger.info("Generating final diagnostic report")
            final_report = self.diagnostics.analyze_dataframe(normalized_data)
            final_summary = self.diagnostics.generate_summary_report(normalized_data)
            
            # Логируем финальный отчет
            logger.info("=== FINAL PIPELINE DIAGNOSTICS ===")
            logger.info(final_summary)
            
            # Сохраняем финальный отчет
            final_report_path = self.diagnostics.save_report(final_report, "final_diagnostics.json")
            logger.info(f"Final diagnostic report saved to {final_report_path}")
            
        except Exception as e:
            logger.warning(f"Failed to generate final diagnostic report: {e}")
        
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
    
    def _get_entity_type(self) -> str:
        """Получить тип сущности для пайплайна."""
        return "documents"
    
    def _create_qc_validator(self) -> Any:
        """Создать QC валидатор для пайплайна."""
        from library.common.qc_profiles import DocumentQCValidator, QCProfile
        
        # Создаем базовый QC профиль для документов
        qc_profile = QCProfile(
            name="document_qc",
            description="Quality control profile for documents",
            rules=[]
        )
        
        return DocumentQCValidator(qc_profile)
    
    def _create_postprocessor(self) -> Any:
        """Создать постпроцессор для пайплайна."""
        from library.common.postprocess_base import DocumentPostprocessor
        return DocumentPostprocessor(self.config)
    
    def _create_etl_writer(self) -> Any:
        """Создать ETL writer для пайплайна."""
        from library.common.writer_base import create_etl_writer
        return create_etl_writer(self.config, "documents")
    
