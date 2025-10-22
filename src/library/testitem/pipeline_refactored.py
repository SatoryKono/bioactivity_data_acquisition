"""Refactored testitem ETL pipeline orchestration."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from library.clients.chembl import TestitemChEMBLClient
from library.clients.pubchem import PubChemClient
from library.config import APIClientConfig, RateLimitSettings, RetrySettings
from library.etl.enhanced_correlation import (
    build_correlation_insights,
    build_enhanced_correlation_analysis,
    build_enhanced_correlation_reports,
    prepare_data_for_correlation_analysis,
)
from library.io.meta import create_dataset_metadata
from library.testitem.config import TestitemConfig
from library.testitem.extract import extract_batch_data
from library.testitem.normalize import TestitemNormalizer
from library.testitem.validate import TestitemValidator
from library.testitem.quality import TestitemQualityFilter
from library.testitem.writer import TestitemWriter

logger = logging.getLogger(__name__)


class TestitemPipelineError(RuntimeError):
    """Base class for testitem pipeline errors."""


class TestitemValidationError(TestitemPipelineError):
    """Raised when the input data does not meet schema expectations."""


class TestitemHTTPError(TestitemPipelineError):
    """Raised when upstream HTTP requests fail irrecoverably."""


class TestitemQCError(TestitemPipelineError):
    """Raised when QC checks do not pass configured thresholds."""


class TestitemIOError(TestitemPipelineError):
    """Raised when reading or writing files fails."""


@dataclass(slots=True)
class TestitemETLResult:
    """Container for ETL artefacts."""

    testitems: pd.DataFrame
    qc: pd.DataFrame
    meta: dict[str, Any]
    correlation_analysis: dict[str, Any] | None = None
    correlation_reports: dict[str, pd.DataFrame] | None = None
    correlation_insights: list[dict[str, Any]] | None = None


def _create_chembl_client(config: TestitemConfig) -> TestitemChEMBLClient:
    """Create ChEMBL API client."""
    
    # Get ChEMBL-specific configuration
    chembl_config = config.sources.get("chembl")
    if not chembl_config:
        raise TestitemValidationError("ChEMBL source not found in configuration")
    
    # Use source-specific timeout or fallback to global
    timeout = chembl_config.http.timeout_sec or config.http.global_.timeout_sec
    if timeout is None:
        timeout = 60.0  # At least 60 seconds for ChEMBL
    
    # Merge headers: default + global + source-specific
    default_headers = _get_chembl_headers()
    headers = {**default_headers, **config.http.global_.headers, **chembl_config.http.headers}
    
    # Process secret placeholders in headers
    import os
    import re
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
    base_url = chembl_config.http.base_url or "https://www.ebi.ac.uk/chembl/api/data"
    
    # Use source-specific retry settings or fallback to global
    retry_settings = RetrySettings(
        total=chembl_config.http.retries.get('total', config.http.global_.retries.total),
        backoff_multiplier=chembl_config.http.retries.get('backoff_multiplier', config.http.global_.retries.backoff_multiplier)
    )
    
    # Create rate limit settings if configured
    rate_limit = None
    if chembl_config.rate_limit:
        # Convert various rate limit formats to max_calls/period
        max_calls = chembl_config.rate_limit.get('max_calls')
        period = chembl_config.rate_limit.get('period')
        
        # If not in max_calls/period format, try to convert from other formats
        if max_calls is None or period is None:
            requests_per_second = chembl_config.rate_limit.get('requests_per_second')
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
        name="chembl",
        base_url=base_url,
        headers=headers,
        timeout=timeout,
        retries=retry_settings,
        rate_limit=rate_limit,
    )
    
    return TestitemChEMBLClient(api_config, timeout=timeout)


def _create_pubchem_client(config: TestitemConfig) -> PubChemClient:
    """Create PubChem API client."""
    
    # Get PubChem-specific configuration
    pubchem_config = config.sources.get("pubchem")
    if not pubchem_config:
        raise TestitemValidationError("PubChem source not found in configuration")
    
    # Use source-specific timeout or fallback to global
    timeout = pubchem_config.http.timeout_sec or config.http.global_.timeout_sec
    if timeout is None:
        timeout = 30.0  # Default timeout for PubChem
    
    # Merge headers: default + global + source-specific
    default_headers = _get_pubchem_headers()
    headers = {**default_headers, **config.http.global_.headers, **pubchem_config.http.headers}
    
    # Use source-specific base_url or fallback to default
    base_url = pubchem_config.http.base_url or "https://pubchem.ncbi.nlm.nih.gov/rest/pug"
    
    # Use source-specific retry settings or fallback to global
    retry_settings = RetrySettings(
        total=pubchem_config.http.retries.get('total', config.http.global_.retries.total),
        backoff_multiplier=pubchem_config.http.retries.get('backoff_multiplier', config.http.global_.retries.backoff_multiplier)
    )
    
    # Create rate limit settings if configured
    rate_limit = None
    if pubchem_config.rate_limit:
        max_calls = pubchem_config.rate_limit.get('max_calls')
        period = pubchem_config.rate_limit.get('period')
        
        if max_calls is None or period is None:
            requests_per_second = pubchem_config.rate_limit.get('requests_per_second')
            if requests_per_second is not None:
                max_calls = 1
                period = 1.0 / requests_per_second
        
        if max_calls is not None and period is not None:
            rate_limit = RateLimitSettings(max_calls=max_calls, period=period)
    
    # Create base API client config
    api_config = APIClientConfig(
        name="pubchem",
        base_url=base_url,
        headers=headers,
        timeout=timeout,
        retries=retry_settings,
        rate_limit=rate_limit,
    )
    
    return PubChemClient(api_config, timeout=timeout)


def _get_chembl_headers() -> dict[str, str]:
    """Get default headers for ChEMBL API."""
    return {
        "User-Agent": "bioactivity-data-acquisition/0.1.0",
        "Accept": "application/json",
    }


def _get_pubchem_headers() -> dict[str, str]:
    """Get default headers for PubChem API."""
    return {
        "User-Agent": "bioactivity-data-acquisition/0.1.0",
        "Accept": "application/json",
    }


def read_testitem_input(path: Path) -> pd.DataFrame:
    """Load the input CSV containing testitem identifiers."""
    try:
        frame = pd.read_csv(path)
    except FileNotFoundError as exc:
        raise TestitemIOError(f"Input CSV not found: {path}") from exc
    except pd.errors.EmptyDataError as exc:
        raise TestitemValidationError("Input CSV is empty") from exc
    except OSError as exc:  # pragma: no cover - filesystem-level errors are rare
        raise TestitemIOError(f"Failed to read input CSV: {exc}") from exc
    return frame


def run_testitem_etl(
    config: TestitemConfig,
    input_data: pd.DataFrame | None = None,
    input_path: Path | None = None
) -> TestitemETLResult:
    """Execute the testitem ETL pipeline returning enriched artefacts."""
    
    # Step 1: Load input data
    if input_data is not None:
        frame = input_data.copy()
    elif input_path is not None:
        frame = read_testitem_input(input_path)
    else:
        raise TestitemValidationError("Either input_data or input_path must be provided")
    
    # Step 2: Validate raw input data
    validator = TestitemValidator(config.model_dump() if hasattr(config, 'model_dump') else {})
    validated_raw = validator.validate_raw(frame)
    
    # Step 3: Extract data from APIs
    logger.info("Starting data extraction from APIs...")
    
    # Create API clients
    chembl_client = _create_chembl_client(config)
    pubchem_client = _create_pubchem_client(config)
    
    # Extract data from ChEMBL
    logger.info("Extracting data from ChEMBL...")
    chembl_data = extract_batch_data(chembl_client, validated_raw, config)
    
    # Extract data from PubChem
    logger.info("Extracting data from PubChem...")
    pubchem_data = extract_batch_data(pubchem_client, validated_raw, config)
    
    # Combine data from all sources
    enriched_frame = pd.concat([chembl_data, pubchem_data], ignore_index=True)
    
    logger.info(f"Data extraction completed. Total records: {len(enriched_frame)}")
    
    # Step 4: Normalize data
    normalizer = TestitemNormalizer(config.model_dump() if hasattr(config, 'model_dump') else {})
    normalized_df = normalizer.normalize_testitems(enriched_frame)
    
    # Step 5: Validate normalized data
    validated_normalized = validator.validate_normalized(normalized_df)
    
    # Step 6: Apply business rules validation
    validated_normalized = validator.validate_business_rules(validated_normalized)
    
    # Step 7: Apply quality filters
    quality_filter = TestitemQualityFilter(config.model_dump() if hasattr(config, 'model_dump') else {})
    
    # Apply strict quality filter
    accepted_df, rejected_df = quality_filter.apply_strict_quality_filter(validated_normalized)
    
    # Build QC report
    qc_report = quality_filter.build_quality_profile(accepted_df)
    
    # Apply QC thresholds
    qc_passed = quality_filter.apply_qc_thresholds(qc_report, accepted_df)
    if not qc_passed:
        raise TestitemQCError("QC thresholds not met")
    
    # Step 8: Build metadata
    meta = create_dataset_metadata(
        data=accepted_df,
        config=config,
        pipeline_version="1.0.0"
    )
    
    # Step 9: Correlation analysis (if enabled)
    correlation_analysis = None
    correlation_reports = None
    correlation_insights = None
    
    if config.postprocess.correlation.enabled:
        try:
            logger.info("Building correlation analysis...")
            
            # Prepare data for correlation analysis
            analysis_df = prepare_data_for_correlation_analysis(accepted_df, "testitems")
            
            # Build enhanced correlation analysis
            correlation_analysis = build_enhanced_correlation_analysis(analysis_df, "testitems")
            
            # Build correlation reports
            correlation_reports = build_enhanced_correlation_reports(analysis_df, "testitems")
            
            # Build correlation insights
            correlation_insights = build_correlation_insights(correlation_analysis, "testitems")
            
            logger.info("Correlation analysis completed successfully")
            
        except Exception as exc:
            logger.warning(f"Error during correlation analysis: {exc}")
            logger.warning(f"Error type: {type(exc).__name__}")
            # Continue without correlation analysis

    return TestitemETLResult(
        testitems=accepted_df,
        qc=qc_report,
        meta=meta,
        correlation_analysis=correlation_analysis,
        correlation_reports=correlation_reports,
        correlation_insights=correlation_insights
    )


def write_testitem_outputs(
    result: TestitemETLResult,
    output_dir: Path,
    config: TestitemConfig
) -> dict[str, Path]:
    """Write testitem ETL outputs to files."""
    writer = TestitemWriter(config)
    return writer.write_testitem_outputs(result, output_dir, config)
