"""Main ETL pipeline orchestration for testitem data."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from library.config import APIClientConfig, RateLimitSettings, RetrySettings
from library.clients.chembl import TestitemChEMBLClient
from library.clients.pubchem import PubChemClient
from library.testitem.config import TestitemConfig
from library.testitem.extract import extract_batch_data
from library.testitem.normalize import normalize_testitem_data
from library.testitem.persist import persist_testitem_data
from library.testitem.validate import validate_testitem_data

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
    import re
    import os
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
        total=getattr(chembl_config.http.retries, 'total', config.http.global_.retries.total),
        backoff_multiplier=getattr(chembl_config.http.retries, 'backoff_multiplier', config.http.global_.retries.backoff_multiplier)
    )
    
    # Create rate limit settings if configured
    rate_limit = None
    if config.http.global_.rate_limit:
        max_calls = config.http.global_.rate_limit.get('max_calls')
        period = config.http.global_.rate_limit.get('period')
        
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
        timeout = 45.0  # Default for PubChem
    
    # Merge headers: default + global + source-specific
    default_headers = _get_pubchem_headers()
    headers = {**default_headers, **config.http.global_.headers, **pubchem_config.http.headers}
    
    # Use source-specific base_url or fallback to default
    base_url = pubchem_config.http.base_url or "https://pubchem.ncbi.nlm.nih.gov/rest/pug"
    
    # Use source-specific retry settings or fallback to global
    retry_settings = RetrySettings(
        total=getattr(pubchem_config.http.retries, 'total', config.http.global_.retries.total),
        backoff_multiplier=getattr(pubchem_config.http.retries, 'backoff_multiplier', config.http.global_.retries.backoff_multiplier)
    )
    
    # Create rate limit settings if configured
    rate_limit = None
    if config.http.global_.rate_limit:
        max_calls = config.http.global_.rate_limit.get('max_calls')
        period = config.http.global_.rate_limit.get('period')
        
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


def _normalise_columns(frame: pd.DataFrame) -> pd.DataFrame:
    """Normalize input columns."""
    normalised = frame.copy()
    
    # Check for required columns (at least one identifier must be present)
    has_molecule_id = "molecule_chembl_id" in normalised.columns
    has_molregno = "molregno" in normalised.columns
    
    if not has_molecule_id and not has_molregno:
        raise TestitemValidationError(
            "Input data must contain at least one of: molecule_chembl_id, molregno"
        )
    
    # Normalize molecule_chembl_id if present
    if has_molecule_id:
        normalised["molecule_chembl_id"] = normalised["molecule_chembl_id"].astype(str).str.strip()
        # Remove rows with empty molecule_chembl_id
        normalised = normalised[normalised["molecule_chembl_id"].str.len() > 0]
    
    # Normalize molregno if present
    if has_molregno:
        normalised["molregno"] = pd.to_numeric(normalised["molregno"], errors='coerce')
    
    # Normalize optional fields
    optional_fields = ["parent_chembl_id", "parent_molregno", "pubchem_cid"]
    for field in optional_fields:
        if field in normalised.columns:
            if field in ["parent_chembl_id"]:
                normalised[field] = normalised[field].astype(str).str.strip()
            elif field in ["parent_molregno"]:
                normalised[field] = pd.to_numeric(normalised[field], errors='coerce')
            elif field in ["pubchem_cid"]:
                normalised[field] = pd.to_numeric(normalised[field], errors='coerce')
    
    # Sort by molecule_chembl_id if available, otherwise by molregno
    if has_molecule_id:
        normalised = normalised.sort_values("molecule_chembl_id").reset_index(drop=True)
    else:
        normalised = normalised.sort_values("molregno").reset_index(drop=True)
    
    return normalised


def run_testitem_etl(
    config: TestitemConfig,
    input_data: pd.DataFrame | None = None,
    input_path: Path | None = None
) -> TestitemETLResult:
    """Execute the testitem ETL pipeline returning enriched artefacts."""

    # Load input data if not provided
    if input_data is None:
        if input_path is None:
            raise TestitemValidationError("Either input_data or input_path must be provided")
        input_data = read_testitem_input(input_path)

    # Normalize input columns
    normalised = _normalise_columns(input_data)

    # Apply limit if configured
    if config.runtime.limit is not None:
        normalised = normalised.head(config.runtime.limit)

    # Check for duplicates
    if "molecule_chembl_id" in normalised.columns:
        duplicates = normalised["molecule_chembl_id"].duplicated()
        if bool(duplicates.any()):
            raise TestitemQCError("Duplicate molecule_chembl_id values detected")

    if normalised.empty:
        logger.warning("No testitem data to process")
        return TestitemETLResult(
            testitems=pd.DataFrame(),
            qc=pd.DataFrame([{"metric": "row_count", "value": 0}]),
            meta={"pipeline_version": config.pipeline_version, "row_count": 0}
        )

    # S01: Get ChEMBL status and release
    logger.info("S01: Getting ChEMBL status and release...")
    chembl_client = _create_chembl_client(config)
    try:
        status_info = chembl_client.get_chembl_status()
        chembl_release = status_info.get("chembl_release", "unknown")
        if chembl_release is None:
            chembl_release = "unknown"
        logger.info(f"ChEMBL release: {chembl_release}")
    except Exception as e:
        logger.warning(f"Failed to get ChEMBL status: {e}")
        chembl_release = "unknown"

    # Create PubChem client if enabled
    pubchem_client = None
    if config.enable_pubchem:
        try:
            pubchem_client = _create_pubchem_client(config)
            logger.info("PubChem client created successfully")
        except Exception as e:
            logger.warning(f"Failed to create PubChem client: {e}")
            logger.warning("Continuing without PubChem enrichment")

    # S02-S10: Extract, normalize, validate data
    logger.info("S02-S10: Extracting, normalizing, and validating data...")
    
    # Extract data from APIs
    extracted_frame = extract_batch_data(chembl_client, pubchem_client, normalised, config)
    
    if extracted_frame.empty:
        logger.warning("No data extracted from APIs")
        return TestitemETLResult(
            testitems=pd.DataFrame(),
            qc=pd.DataFrame([{"metric": "row_count", "value": 0}]),
            meta={"pipeline_version": config.pipeline_version, "row_count": 0}
        )

    # Add chembl_release to all records
    extracted_frame["chembl_release"] = chembl_release

    # Normalize data
    normalized_frame = normalize_testitem_data(extracted_frame)

    # Validate data
    validated_frame = validate_testitem_data(normalized_frame)

    # Calculate QC metrics
    qc_metrics = [
        {"metric": "row_count", "value": int(len(validated_frame))},
        {"metric": "chembl_release", "value": chembl_release},
        {"metric": "pubchem_enabled", "value": config.enable_pubchem},
    ]
    
    # Add source-specific metrics
    if "source_system" in validated_frame.columns:
        source_counts = validated_frame["source_system"].value_counts().to_dict()
        qc_metrics.append({"metric": "source_counts", "value": source_counts})
    
    # Add PubChem enrichment metrics
    if "pubchem_cid" in validated_frame.columns:
        pubchem_count = validated_frame["pubchem_cid"].notna().sum()
        qc_metrics.append({"metric": "pubchem_enriched_records", "value": int(pubchem_count)})
    
    # Add error metrics
    if "error" in validated_frame.columns:
        error_count = validated_frame["error"].notna().sum()
        qc_metrics.append({"metric": "records_with_errors", "value": int(error_count)})
    
    qc = pd.DataFrame(qc_metrics)

    # Create metadata
    meta = {
        "pipeline_version": config.pipeline_version,
        "chembl_release": chembl_release,
        "row_count": len(validated_frame),
        "extraction_parameters": {
            "total_molecules": len(validated_frame),
            "pubchem_enabled": config.enable_pubchem,
            "allow_parent_missing": config.allow_parent_missing,
            "batch_size": getattr(config.runtime, 'batch_size', 200),
            "retries": getattr(config.runtime, 'retries', 5),
            "timeout_sec": getattr(config.runtime, 'timeout_sec', 30)
        }
    }

    return TestitemETLResult(
        testitems=validated_frame, 
        qc=qc,
        meta=meta
    )


def write_testitem_outputs(
    result: TestitemETLResult, 
    output_dir: Path, 
    config: TestitemConfig
) -> dict[str, Path]:
    """Persist ETL artefacts to disk and return the generated paths."""

    return persist_testitem_data(result.testitems, output_dir, config)


__all__ = [
    "TestitemETLResult",
    "TestitemHTTPError",
    "TestitemIOError",
    "TestitemPipelineError",
    "TestitemQCError",
    "TestitemValidationError",
    "run_testitem_etl",
    "write_testitem_outputs",
]
