"""Refactored document ETL pipeline orchestration."""

from __future__ import annotations

import logging
import os
import re
from collections.abc import Generator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from library.clients.chembl import ChEMBLClient
from library.clients.crossref import CrossrefClient
from library.clients.openalex import OpenAlexClient
from library.clients.pubmed import PubMedClient
from library.clients.semantic_scholar import SemanticScholarClient
from library.config import APIClientConfig
from library.documents.config import DocumentConfig
from library.documents.normalize import DocumentNormalizer
from library.documents.quality import DocumentQualityFilter
from library.documents.validate import DocumentValidator
from library.etl.enhanced_correlation import (
    build_correlation_insights,
    build_enhanced_correlation_analysis,
    build_enhanced_correlation_reports,
    prepare_data_for_correlation_analysis,
)
from library.etl.load import write_deterministic_csv
from library.io.meta import create_dataset_metadata
from library.tools.citation_formatter import add_citation_column
from library.tools.journal_normalizer import normalize_journal_columns


class DocumentPipelineError(RuntimeError):
    """Base class for document pipeline errors."""


class DocumentValidationError(DocumentPipelineError):
    """Raised when the input data does not meet schema expectations."""


class DocumentHTTPError(DocumentPipelineError):
    """Raised when upstream HTTP requests fail irrecoverably."""


class DocumentQCError(DocumentPipelineError):
    """Raised when QC checks do not pass configured thresholds."""


class DocumentIOError(DocumentPipelineError):
    """Raised when reading or writing files fails."""


@dataclass(slots=True)
class DocumentETLResult:
    """Container for ETL artefacts."""

    documents: pd.DataFrame
    qc: pd.DataFrame
    meta: dict[str, Any]
    correlation_analysis: dict[str, Any] | None = None
    correlation_reports: dict[str, pd.DataFrame] | None = None
    correlation_insights: list[dict[str, Any]] | None = None


_REQUIRED_COLUMNS = {"document_chembl_id", "doi", "title"}

# Get logger for this module
logger = logging.getLogger(__name__)


def read_document_input(path: Path) -> pd.DataFrame:
    """Load the input CSV containing document identifiers."""

    try:
        frame = pd.read_csv(path)
    except FileNotFoundError as exc:
        raise DocumentIOError(f"Input CSV not found: {path}") from exc
    except pd.errors.EmptyDataError as exc:
        raise DocumentValidationError("Input CSV is empty") from exc
    except OSError as exc:  # pragma: no cover - filesystem-level errors are rare
        raise DocumentIOError(f"Failed to read input CSV: {exc}") from exc
    return frame


def _normalise_columns(frame: pd.DataFrame) -> pd.DataFrame:
    """Normalize input columns."""
    normalised = frame.copy()
    
    # Remove postcodes column if it exists (deprecated field)
    if 'postcodes' in normalised.columns:
        normalised = normalised.drop(columns=['postcodes'])
    
    # Check for required columns
    present = {column for column in normalised.columns}
    missing = _REQUIRED_COLUMNS - present
    if missing:
        raise DocumentValidationError(f"Input data is missing required columns: {missing}")

    # Normalize document_chembl_id
    if "document_chembl_id" in normalised.columns:
        normalised["document_chembl_id"] = normalised["document_chembl_id"].astype(str).str.strip()
        normalised = normalised[normalised["document_chembl_id"] != ""]
    
    return normalised.sort_values("document_chembl_id").reset_index(drop=True)


def _create_api_client(source: str, config: DocumentConfig) -> Any:
    """Create an API client for the specified source."""
    from library.config import RateLimitSettings, RetrySettings
    
    # Get source-specific configuration
    source_config = config.sources.get(source)
    if not source_config:
        raise DocumentValidationError(f"Source '{source}' not found in configuration")
    
    # Use source-specific timeout or fallback to global
    timeout = source_config.http.timeout_sec or config.http.global_.timeout_sec
    if source == "chembl":
        timeout = max(timeout, 60.0)  # At least 60 seconds for ChEMBL
    
    # Merge headers: default + global + source-specific
    default_headers = _get_headers(source)
    headers = {**default_headers, **config.http.global_.headers, **source_config.http.headers}
    
    # Process secret placeholders in headers
    processed_headers = {}
    for key, value in headers.items():
        if isinstance(value, str):
            def replace_placeholder(match):
                secret_name = match.group(1)
                env_var = os.environ.get(secret_name.upper())
                return env_var if env_var is not None else match.group(0)
            processed_value = re.sub(r'\{([^}]+)\}', replace_placeholder, value)
            # Only include header if the value is not empty after processing and not a placeholder
            if (processed_value and processed_value.strip() and 
                not processed_value.startswith('{') and not processed_value.endswith('}')):
                processed_headers[key] = processed_value
        else:
            processed_headers[key] = value
    headers = processed_headers
    
    # Use source-specific base_url or fallback to default
    base_url = source_config.http.base_url or _get_base_url(source)
    
    # Use source-specific retry settings or fallback to global
    retry_settings = RetrySettings(
        total=source_config.http.retries.get('total', config.http.global_.retries.total),
        backoff_multiplier=source_config.http.retries.get('backoff_multiplier', config.http.global_.retries.backoff_multiplier)
    )
    
    # Create rate limit settings if configured
    rate_limit = None
    if source_config.rate_limit:
        # Convert various rate limit formats to max_calls/period
        max_calls = source_config.rate_limit.get('max_calls')
        period = source_config.rate_limit.get('period')
        
        # If not in max_calls/period format, try to convert from other formats
        if max_calls is None or period is None:
            requests_per_second = source_config.rate_limit.get('requests_per_second')
            if requests_per_second is not None:
                max_calls = 1
                period = 1.0 / requests_per_second
            else:
                # Skip rate limiting if we can't determine the format
                rate_limit = None
        
        # Create RateLimitSettings object if we have valid max_calls and period
        if max_calls is not None and period is not None:
            rate_limit = RateLimitSettings(max_calls=max_calls, period=period)
    
    # Create base API client config
    api_config = APIClientConfig(
        name=source,
        base_url=base_url,
        headers=headers,
        timeout=timeout,
        retries=retry_settings,
        rate_limit=rate_limit,
    )
   
    if source == "chembl":
        return ChEMBLClient(api_config, timeout=timeout)
    elif source == "crossref":
        return CrossrefClient(api_config, timeout=timeout)
    elif source == "openalex":
        return OpenAlexClient(api_config, timeout=timeout)
    elif source == "pubmed":
        return PubMedClient(api_config, timeout=timeout)
    elif source == "semantic_scholar":
        return SemanticScholarClient(api_config, timeout=timeout)
    else:
        raise DocumentValidationError(f"Unsupported source: {source}")


def _get_base_url(source: str) -> str:
    """Get the base URL for the specified source."""
    urls = {
        "chembl": "https://www.ebi.ac.uk/chembl/api/data",
        "crossref": "https://api.crossref.org/works",
        "openalex": "https://api.openalex.org/works",
        "pubmed": "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/",
        "semantic_scholar": "https://api.semanticscholar.org/graph/v1/paper",
    }
    return urls.get(source, "")


def _get_headers(source: str) -> dict[str, str]:
    """Get default headers for the specified source."""
    headers = {
        "User-Agent": "bioactivity-data-acquisition/0.1.0",
        "Accept": "application/json",  # All sources should accept JSON
    }
    
    return headers


def _batch_dataframe(df: pd.DataFrame, batch_size: int) -> Generator[pd.DataFrame, None, None]:
    """Split DataFrame into batches."""
    for i in range(0, len(df), batch_size):
        yield df.iloc[i:i + batch_size]


def _extract_data_from_source_batch(
    source: str, 
    client: Any, 
    frame: pd.DataFrame, 
    config: DocumentConfig
) -> pd.DataFrame:
    """Extract data from a specific source for a batch."""
    enriched_data = []
    
    # Define default values for all possible columns to ensure they exist
    default_columns = {
        # ChEMBL columns
        "chembl_title": None, "chembl_doi": None, "chembl_pmid": None,
        "chembl_journal": None, "chembl_year": None, "chembl_volume": None, 
        "chembl_issue": None,
        # Crossref columns  
        "crossref_doi": None, "crossref_title": None, "crossref_doc_type": None, "crossref_subject": None, 
        "crossref_error": None,
        # OpenAlex columns
        "openalex_doi": None, "openalex_title": None, "openalex_type": None, "openalex_concepts": None,
        "openalex_error": None,
        # PubMed columns
        "pubmed_doi": None, "pubmed_title": None, "pubmed_abstract": None, "pubmed_authors": None,
        "pubmed_journal": None, "pubmed_issn": None, "pubmed_volume": None, "pubmed_issue": None,
        "pubmed_pages": None, "pubmed_year": None, "pubmed_month": None, "pubmed_day": None,
        "pubmed_pmcid": None, "pubmed_error": None,
        # Semantic Scholar columns
        "semantic_scholar_doi": None, "semantic_scholar_title": None, "semantic_scholar_abstract": None,
        "semantic_scholar_authors": None, "semantic_scholar_venue": None, "semantic_scholar_year": None,
        "semantic_scholar_citation_count": None, "semantic_scholar_error": None,
    }
    
    for _, row in frame.iterrows():
        try:
            # Start with row data and add default columns
            row_data = row.to_dict()
            for key, default_value in default_columns.items():
                if key not in row_data:
                    row_data[key] = default_value
            
            # Extract data based on source
            if source == "chembl":
                if pd.notna(row.get("document_chembl_id")):
                    data = client.fetch_by_document_id(str(row["document_chembl_id"]).strip())
                    data.pop("source", None)
                    for key, value in data.items():
                        if value is not None:
                            row_data[key] = value
            elif source == "crossref":
                if pd.notna(row.get("doi")) and str(row["doi"]).strip():
                    data = client.fetch_by_doi(str(row["doi"]).strip())
                    data.pop("source", None)
                    for key, value in data.items():
                        if value is not None:
                            row_data[key] = value
            elif source == "openalex":
                if pd.notna(row.get("doi")) and str(row["doi"]).strip():
                    data = client.fetch_by_doi(str(row["doi"]).strip())
                    data.pop("source", None)
                    for key, value in data.items():
                        if value is not None:
                            row_data[key] = value
            elif source == "pubmed":
                if pd.notna(row.get("document_pubmed_id")) and str(row["document_pubmed_id"]).strip():
                    data = client.fetch_by_pmid(str(row["document_pubmed_id"]).strip())
                    data.pop("source", None)
                    for key, value in data.items():
                        if value is not None:
                            row_data[key] = value
            elif source == "semantic_scholar":
                if pd.notna(row.get("document_pubmed_id")) and str(row["document_pubmed_id"]).strip():
                    data = client.fetch_by_pmid(str(row["document_pubmed_id"]).strip())
                    data.pop("source", None)
                    for key, value in data.items():
                        if value is not None:
                            row_data[key] = value
            
            enriched_data.append(row_data)
                    
        except Exception as exc:
            # Log error but continue processing other records
            doc_id = row.get('document_chembl_id', 'unknown')
            logger.error(f"Error extracting data from {source} for row {doc_id}: {exc}")
            logger.error(f"Error type: {type(exc).__name__}")
            
            # Ensure error row also has all columns
            error_row = row.to_dict()
            error_row.update(default_columns)
            
            # Set error flag for this source with more detailed error info
            error_msg = f"{type(exc).__name__}: {str(exc)}"
            
            # Специальная обработка для ошибок rate limiting
            if ("429" in str(exc) or "Rate limited" in str(exc) or 
                "RateLimitError" in str(type(exc).__name__)):
                error_msg = f"Rate limited by API: {str(exc)}"
                logger.warning(f"Rate limiting detected for {source}, continuing with next record...")
            
            if source == "crossref":
                error_row["crossref_error"] = error_msg
            elif source == "openalex":
                error_row["openalex_error"] = error_msg
            elif source == "pubmed":
                error_row["pubmed_error"] = error_msg
            elif source == "semantic_scholar":
                error_row["semantic_scholar_error"] = error_msg
            elif source == "chembl":
                error_row["chembl_error"] = error_msg
            
            enriched_data.append(error_row)
    
    return pd.DataFrame(enriched_data)


def run_document_etl(config: DocumentConfig, frame: pd.DataFrame) -> DocumentETLResult:
    """Execute the document ETL pipeline returning enriched artefacts."""
    
    # Step 1: Validate raw input data
    validator = DocumentValidator(config.model_dump() if hasattr(config, 'model_dump') else {})
    validated_raw = validator.validate_raw(frame)
    
    # Step 2: Normalize input columns
    normalised = _normalise_columns(validated_raw)
    
    if config.runtime.limit is not None:
        normalised = normalised.head(config.runtime.limit)

    # Check for duplicates
    duplicates = normalised["document_chembl_id"].duplicated()
    if bool(duplicates.any()):
        raise DocumentQCError("Duplicate document_chembl_id values detected")

    # Step 3: Extract data from multiple sources
    batch_size = getattr(config.runtime, "batch_size", 100)
    total_rows = 0
    document_batches = []
    enabled_sources = config.enabled_sources()
    
    logger.info(f"Using streaming batch processing with batch_size={batch_size}")
    logger.info(f"Processing {len(normalised)} documents in batches")
    
    # Split documents into batches for streaming processing
    for batch_index, batch_frame in enumerate(_batch_dataframe(normalised, batch_size)):
        logger.info(f"Processing batch {batch_index + 1}: {len(batch_frame)} documents")
        
        # Initialize all possible columns with default values for this batch
        enriched_batch = batch_frame.copy()
        
        # Process each source for this batch
        for source in enabled_sources:
            try:
                logger.info(f"Extracting data from {source} for batch {batch_index + 1}...")
                client = _create_api_client(source, config)
                
                # Use batch processing where available
                enriched_batch = _extract_data_from_source_batch(source, client, enriched_batch, config)
                
                # Log success statistics for this batch
                if source == "chembl":
                    success_count = enriched_batch["chembl_title"].notna().sum()
                elif source == "crossref":
                    success_count = (enriched_batch["crossref_title"].notna() | 
                                   enriched_batch["crossref_error"].notna()).sum()
                elif source == "openalex":
                    success_count = enriched_batch["openalex_title"].notna().sum()
                elif source == "pubmed":
                    success_count = enriched_batch["pubmed_pmid"].notna().sum()
                elif source == "semantic_scholar":
                    success_count = enriched_batch["semantic_scholar_pmid"].notna().sum()
                else:
                    success_count = 0
                    
                logger.info(f"Successfully extracted data from {source} for batch {batch_index + 1}: "
                           f"{success_count}/{len(enriched_batch)} records")
                          
            except Exception as exc:
                logger.warning(f"Failed to extract data from {source} for batch {batch_index + 1}: {exc}")
                logger.warning(f"Error type: {type(exc).__name__}")
                # Continue with other sources even if one fails
        
        total_rows += len(enriched_batch)
        document_batches.append(enriched_batch)
        
        logger.info(f"Completed batch {batch_index + 1}: {len(enriched_batch)} documents, total={total_rows}")
        
        # Check limit
        if config.runtime.limit is not None and total_rows >= config.runtime.limit:
            logger.info(f"Reached global limit of {config.runtime.limit} documents")
            break
    
    if not document_batches:
        logger.info("No documents processed in streaming mode")
        enriched_frame = normalised.copy()
    else:
        enriched_frame = pd.concat(document_batches, ignore_index=True)
        if config.runtime.limit is not None and len(enriched_frame) > config.runtime.limit:
            enriched_frame = enriched_frame.head(config.runtime.limit)
    
    logger.info(f"Successfully processed {len(enriched_frame)} documents in {len(document_batches)} batches")

    # Step 4: Normalize data
    normalizer = DocumentNormalizer(config.model_dump() if hasattr(config, 'model_dump') else {})
    normalized_df = normalizer.normalize_documents(enriched_frame)
    
    # Step 5: Validate normalized data
    validated_normalized = validator.validate_normalized(normalized_df)
    
    # Step 6: Apply quality filters
    quality_filter = DocumentQualityFilter(config.model_dump() if hasattr(config, 'model_dump') else {})
    
    # Apply strict quality filter
    accepted_df, rejected_df = quality_filter.apply_strict_quality_filter(validated_normalized)
    
    # Build QC report
    qc_report = quality_filter.build_quality_profile(accepted_df)
    
    # Apply QC thresholds
    qc_passed = quality_filter.apply_qc_thresholds(qc_report, accepted_df)
    if not qc_passed:
        raise DocumentQCError("QC thresholds not met")
    
    # Step 7: Post-processing
    # Add citation column
    if config.postprocess.citation.enabled:
        accepted_df = add_citation_column(accepted_df)
    
    # Normalize journal columns
    if config.postprocess.journal_normalization.enabled:
        accepted_df = normalize_journal_columns(accepted_df)
    
    # Step 8: Build metadata
    meta = create_dataset_metadata(
        dataset_name="documents",
        config=config.api if hasattr(config, 'api') else None,
        logger=logger
    ).to_dict()
    
    # Step 9: Correlation analysis (if enabled)
    correlation_analysis = None
    correlation_reports = None
    correlation_insights = None
    
    if config.postprocess.correlation.enabled:
        try:
            logger.info("Building correlation analysis...")
            
            # Prepare data for correlation analysis
            analysis_df = prepare_data_for_correlation_analysis(accepted_df, "documents")
            
            # Build enhanced correlation analysis
            correlation_analysis = build_enhanced_correlation_analysis(analysis_df, "documents")
            
            # Build correlation reports
            correlation_reports = build_enhanced_correlation_reports(analysis_df, "documents")
            
            # Build correlation insights
            correlation_insights = build_correlation_insights(correlation_analysis, "documents")
            
            logger.info("Correlation analysis completed successfully")
            
        except Exception as exc:
            logger.warning(f"Error during correlation analysis: {exc}")
            logger.warning(f"Error type: {type(exc).__name__}")
            # Continue without correlation analysis

    return DocumentETLResult(
        documents=accepted_df,
        qc=qc_report,
        meta=meta,
        correlation_analysis=correlation_analysis,
        correlation_reports=correlation_reports,
        correlation_insights=correlation_insights
    )


def write_document_outputs(
    result: DocumentETLResult,
    output_dir: Path,
    date_tag: str,
    config: DocumentConfig
) -> dict[str, Path]:
    """Write document ETL outputs to files."""
    
    # Ensure output directory exists
    output_dir.mkdir(parents=True, exist_ok=True)
    
    outputs = {}
    
    # Main data file
    data_path = output_dir / f"documents_{date_tag}.csv"
    write_deterministic_csv(
        result.documents,
        data_path,
        config.determinism
    )
    outputs["data"] = data_path
    
    # QC report
    qc_path = output_dir / f"documents_{date_tag}_qc.csv"
    result.qc.to_csv(qc_path, index=False)
    outputs["qc"] = qc_path
    
    # Metadata
    meta_path = output_dir / f"documents_{date_tag}_meta.yaml"
    import yaml
    with open(meta_path, 'w', encoding='utf-8') as f:
        yaml.dump(result.meta, f, default_flow_style=False, allow_unicode=True)
    outputs["meta"] = meta_path
    
    # Correlation reports (if available)
    if result.correlation_reports:
        corr_dir = output_dir / f"documents_{date_tag}_correlation_report"
        corr_dir.mkdir(exist_ok=True)
        
        for report_name, report_df in result.correlation_reports.items():
            report_path = corr_dir / f"{report_name}.csv"
            report_df.to_csv(report_path, index=False)
            outputs[f"correlation_{report_name}"] = report_path
    
    return outputs
