"""Main ETL pipeline orchestration for testitem data."""

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
from library.testitem.config import TestitemConfig
from library.testitem.extract import extract_batch_data
from library.testitem.normalize import normalize_testitem_data
from library.testitem.persist import persist_testitem_data
from library.testitem.validate import validate_testitem_data

logger = logging.getLogger(__name__)


<<<<<<< Updated upstream
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
   
    # Pass optional cache directory for PubChem GET requests if provided in runtime
    cache_dir = None
    if hasattr(config, 'runtime') and getattr(config.runtime, 'pubchem_cache_dir', None):
        cache_dir = config.runtime.pubchem_cache_dir
    return PubChemClient(api_config, timeout=timeout, cache_dir=cache_dir)


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

    # Check for duplicates and drop them deterministically (keep first)
    if "molecule_chembl_id" in normalised.columns:
        duplicates_mask = normalised["molecule_chembl_id"].duplicated()
        if bool(duplicates_mask.any()):
            dup_count = int(duplicates_mask.sum())
            logger.warning(f"Found {dup_count} duplicate molecule_chembl_id values — keeping first occurrences and dropping the rest")
            normalised = normalised.loc[~duplicates_mask].reset_index(drop=True)

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

    # Perform correlation analysis if enabled in config
    correlation_analysis = None
    correlation_reports = None
    correlation_insights = None
    
    if (hasattr(config, 'postprocess') and 
        hasattr(config.postprocess, 'correlation') and 
        config.postprocess.correlation.enabled and 
        len(validated_frame) > 1):
        try:
            logger.info("Performing correlation analysis...")
            logger.info(f"Input data shape: {validated_frame.shape}")
            logger.info(f"Input columns: {list(validated_frame.columns)}")
            
            # Подготавливаем данные для корреляционного анализа
            analysis_df = prepare_data_for_correlation_analysis(
                validated_frame, 
                data_type="testitems", 
                logger=logger
            )
            
            logger.info(f"Prepared data shape: {analysis_df.shape}")
            logger.info(f"Correlation analysis: {len(analysis_df.columns)} numeric columns, {len(analysis_df)} rows")
            logger.info(f"Columns for analysis: {list(analysis_df.columns)}")
            
            if len(analysis_df.columns) > 1:
                logger.info("Starting enhanced correlation analysis...")
                # Perform correlation analysis
                correlation_analysis = build_enhanced_correlation_analysis(analysis_df, logger)
                logger.info("Enhanced correlation analysis completed")
                
                logger.info("Building correlation reports...")
                correlation_reports = build_enhanced_correlation_reports(analysis_df, logger)
                logger.info(f"Generated {len(correlation_reports)} correlation reports")
                
                logger.info("Building correlation insights...")
                correlation_insights = build_correlation_insights(analysis_df, logger)
                logger.info(f"Correlation analysis completed. Found {len(correlation_insights)} insights.")
            else:
                logger.warning("Not enough numeric columns for correlation analysis")
                logger.warning(f"Available columns: {list(analysis_df.columns)}")
                
        except Exception as exc:
            logger.warning(f"Error during correlation analysis: {exc}")
            logger.warning(f"Error type: {type(exc).__name__}")
            import traceback
            logger.warning(f"Traceback: {traceback.format_exc()}")
            # Continue without correlation analysis

    return TestitemETLResult(
        testitems=validated_frame, 
        qc=qc,
        meta=meta,
        correlation_analysis=correlation_analysis,
        correlation_reports=correlation_reports,
        correlation_insights=correlation_insights
    )
=======
class TestitemPipeline(PipelineBase[TestitemConfig]):
    """Testitem ETL pipeline using unified PipelineBase."""

    def __init__(self, config: TestitemConfig) -> None:
        """Initialize testitem pipeline with configuration."""
        super().__init__(config)
        self.validator = TestitemValidator(config.model_dump() if hasattr(config, "model_dump") else {})
        self.normalizer = TestitemNormalizer(config.model_dump() if hasattr(config, "model_dump") else {})
        self.quality_filter = TestitemQualityFilter(config.model_dump() if hasattr(config, "model_dump") else {})

    def _setup_clients(self) -> None:
        """Initialize HTTP clients for testitem sources."""
        self.clients = {}

        # ChEMBL client
        if "chembl" in self.config.sources and self.config.sources["chembl"].enabled:
            self.clients["chembl"] = self._create_chembl_client()

        # PubChem client
        if "pubchem" in self.config.sources and self.config.sources["pubchem"].enabled:
            self.clients["pubchem"] = self._create_pubchem_client()

    def _create_chembl_client(self) -> ChEMBLClient:
        """Create ChEMBL client."""
        from library.settings import RateLimitSettings

        source_config = self.config.sources["chembl"]
        timeout = source_config.http.timeout_sec or self.config.http.global_.timeout_sec
        timeout = max(timeout, 60.0)  # At least 60 seconds for ChEMBL

        headers = self._get_headers("chembl")
        headers.update(self.config.http.global_.headers)
        headers.update(source_config.http.headers)

        processed_headers = self._process_headers(headers)

        from library.settings import APIClientConfig

        # Get retry settings
        retries = source_config.http.retries or self.config.http.global_.retries

        # Get rate limit settings if available
        rate_limit = None
        if hasattr(source_config, "rate_limit") and source_config.rate_limit:
            rate_limit = RateLimitSettings(
                max_calls=source_config.rate_limit.max_calls,
                period=source_config.rate_limit.period,
            )

        client_config = APIClientConfig(
            name="chembl",
            base_url=source_config.http.base_url,
            timeout=timeout,
            retries=retries,
            rate_limit=rate_limit,
            headers=processed_headers,
        )

        return ChEMBLClient(client_config)

    def _create_pubchem_client(self) -> Any:
        """Create PubChem client."""
        import requests

        # Create a simple requests session for PubChem
        session = requests.Session()

        # Set headers
        headers = self._get_headers("pubchem")
        headers.update(self.config.http.global_.headers)
        session.headers.update(headers)

        return session

    def _get_headers(self, source: str) -> dict[str, str]:
        """Get default headers for a source."""
        return {
            "Accept": "application/json",
            "User-Agent": "bioactivity-data-acquisition/0.1.0",
        }

    def _process_headers(self, headers: dict[str, str]) -> dict[str, str]:
        """Process headers with secret placeholders."""
        import os

        processed = {}
        for key, value in headers.items():
            if isinstance(value, str) and value.startswith("${") and value.endswith("}"):
                env_var = value[2:-1]
                processed[key] = os.getenv(env_var, value)
            else:
                processed[key] = value
        return processed

    def _is_valid_value(self, value) -> bool:
        """Check if a value is valid (not null, not empty array, not empty string)."""
        import numpy as np

        # Check for None
        if value is None:
            return False

        # Check for numpy arrays first (before pd.isna)
        if isinstance(value, np.ndarray):
            return value.size > 0

        # Check for pandas NA (but not for numpy arrays)
        try:
            # Skip pd.isna for numpy arrays as they're already handled above
            if not isinstance(value, np.ndarray) and pd.isna(value):
                return False
        except (ValueError, TypeError):
            # If pd.isna fails, continue with other checks
            pass

        # Check for empty strings
        if isinstance(value, str) and value.strip() == "":
            return False

        # Check for empty lists
        if isinstance(value, list) and len(value) == 0:
            return False

        return True

    def extract(self, input_data: pd.DataFrame) -> pd.DataFrame:
        """Extract testitem data from multiple sources."""
        logger.info(f"Extracting testitem data for {len(input_data)} testitems")

        # Apply limit if specified
        if getattr(self.config.runtime, "limit", None) is not None:
            input_data = input_data.head(self.config.runtime.limit)

        # Check for duplicates
        duplicates = input_data["molecule_chembl_id"].duplicated()
        if duplicates.any():
            raise ValueError("Duplicate molecule_chembl_id values detected")

        # Extract data from each enabled source
        extracted_data = input_data.copy()

        # ChEMBL extraction
        if "chembl" in self.clients:
            try:
                logger.info("Extracting data from ChEMBL")
                chembl_data = self._extract_from_chembl(extracted_data)
                extracted_data = self._merge_chembl_data(extracted_data, chembl_data)
            except Exception as e:
                logger.error("Failed to extract from ChEMBL: %s", e)
                if not getattr(self.config.runtime, "allow_incomplete_sources", False):
                    raise

        # PubChem extraction
        if "pubchem" in self.clients:
            try:
                logger.info("Extracting data from PubChem")
                pubchem_data = self._extract_from_pubchem(extracted_data)
                extracted_data = self._merge_pubchem_data(extracted_data, pubchem_data)
            except Exception as e:
                logger.error("Failed to extract from PubChem: %s", e)
                if not getattr(self.config.runtime, "allow_incomplete_sources", False):
                    raise

        # Validate input data first
        self.validator.validate_input(input_data)

        # Return the extracted and enriched data
        logger.info(f"Extracted data for {len(extracted_data)} testitems")
        return extracted_data

    def _extract_from_chembl(self, data: pd.DataFrame) -> pd.DataFrame:
        """Extract data from ChEMBL."""
        if data.empty:
            return pd.DataFrame()

        # Get ChEMBL client
        client = self.clients.get("chembl")
        if not client:
            logger.warning("ChEMBL client not available")
            return pd.DataFrame()

        # Extract molecule IDs
        molecule_ids = data["molecule_chembl_id"].dropna().unique().tolist()
        if not molecule_ids:
            logger.warning("No valid molecule_chembl_id found for ChEMBL extraction")
            return pd.DataFrame()

        logger.info(f"Extracting ChEMBL data for {len(molecule_ids)} molecules")

        # ChEMBL fields to extract
        chembl_fields = [
            "molecule_chembl_id",
            "molregno",
            "pref_name",
            "max_phase",
            "therapeutic_flag",
            "structure_type",
            "alogp",
            "hba",
            "hbd",
            "psa",
            "rtb",
            "ro3_pass",
            "qed_weighted",
            "oral",
            "parenteral",
            "topical",
            "withdrawn_flag",
            "parent_chembl_id",
            "molecule_type",
            "first_approval",
            "black_box_warning",
            "natural_product",
            "first_in_class",
            "chirality",
            "prodrug",
            "inorganic_flag",
            "polymer_flag",
            "usan_year",
            "availability_type",
            "usan_stem",
            "usan_substem",
            "usan_stem_definition",
            "indication_class",
            "withdrawn_year",
            "withdrawn_country",
            "withdrawn_reason",
            "mechanism_of_action",
            "direct_interaction",
            "molecular_mechanism",
            "drug_chembl_id",
            "drug_name",
            "drug_type",
            "drug_substance_flag",
            "drug_indication_flag",
            "drug_antibacterial_flag",
            "drug_antiviral_flag",
            "drug_antifungal_flag",
            "drug_antiparasitic_flag",
            "drug_antineoplastic_flag",
            "drug_immunosuppressant_flag",
            "drug_antiinflammatory_flag",
        ]

        # Process in batches
        batch_size = 25  # ChEMBL API limit
        all_records = []

        for i in range(0, len(molecule_ids), batch_size):
            batch_ids = molecule_ids[i : i + batch_size]
            try:
                batch_data = self._fetch_chembl_batch(batch_ids, chembl_fields, client)
                if not batch_data.empty:
                    all_records.append(batch_data)
            except Exception as e:
                logger.error(f"Failed to fetch ChEMBL batch {i // batch_size + 1}: {e}")
                continue

        if not all_records:
            logger.warning("No ChEMBL data extracted")
            return pd.DataFrame()

        # Combine all records
        result = pd.concat(all_records, ignore_index=True)
        result["retrieved_at"] = pd.Timestamp.now().isoformat()

        logger.info(f"ChEMBL extraction completed: {len(result)} records")
        return result

    def _fetch_chembl_batch(self, molecule_ids: list[str], fields: list[str], client) -> pd.DataFrame:
        """Fetch a batch of molecules from ChEMBL API."""
        from urllib.parse import urlencode

        import requests

        params = {"format": "json", "limit": "1000", "fields": ",".join(fields)}

        # Add molecule filter
        if len(molecule_ids) == 1:
            params["molecule_chembl_id"] = molecule_ids[0]
        else:
            params["molecule_chembl_id__in"] = ",".join(molecule_ids)

        query_string = urlencode(params)

        try:
            # Make request using ChEMBL client's _request method
            response = client._request("GET", f"molecule.json?{query_string}")
            if not response:
                logger.warning("No response from ChEMBL for batch: %s", molecule_ids)
                return pd.DataFrame()

            # response is already parsed data from ChEMBL client
            if "molecules" not in response:
                logger.warning("No molecules found in ChEMBL response for batch: %s", molecule_ids)
                return pd.DataFrame()

            # Convert to DataFrame and flatten nested structures
            molecules = response["molecules"]
            if not molecules:
                return pd.DataFrame()

            # Flatten nested structures
            flattened_molecules = []
            for molecule in molecules:
                flattened = self._flatten_molecule_data(molecule)
                flattened_molecules.append(flattened)

            df = pd.DataFrame(flattened_molecules)
            return df

        except requests.RequestException as e:
            logger.error(f"ChEMBL API request failed: {e}")
            raise
        except Exception as e:
            logger.error(f"Failed to process ChEMBL response: {e}")
            raise

    def _flatten_molecule_data(self, molecule: dict) -> dict:
        """Flatten nested molecule structures into top-level fields."""
        import json

        flattened = molecule.copy()

        # Extract from molecule_hierarchy
        if "molecule_hierarchy" in molecule and molecule["molecule_hierarchy"]:
            hierarchy = molecule["molecule_hierarchy"]
            flattened["parent_chembl_id"] = hierarchy.get("parent_chembl_id")
            flattened["parent_molregno"] = hierarchy.get("parent_molregno")

        # Extract from molecule_properties
        if "molecule_properties" in molecule and molecule["molecule_properties"]:
            props = molecule["molecule_properties"]

            # Физико-химические свойства
            flattened["alogp"] = props.get("alogp")
            flattened["aromatic_rings"] = props.get("aromatic_rings")
            flattened["hba"] = props.get("hba")
            flattened["hbd"] = props.get("hbd")
            flattened["hba_lipinski"] = props.get("hba_lipinski")
            flattened["hbd_lipinski"] = props.get("hbd_lipinski")
            flattened["heavy_atoms"] = props.get("heavy_atoms")
            flattened["mw_freebase"] = props.get("mw_freebase")
            flattened["full_mwt"] = props.get("full_mwt")
            flattened["mw_monoisotopic"] = props.get("mw_monoisotopic")
            flattened["full_molformula"] = props.get("full_molformula")
            flattened["molecular_species"] = props.get("molecular_species")
            flattened["num_ro5_violations"] = props.get("num_ro5_violations")
            flattened["num_lipinski_ro5_violations"] = props.get("num_lipinski_ro5_violations")
            flattened["psa"] = props.get("psa")
            flattened["qed_weighted"] = props.get("qed_weighted")
            flattened["ro3_pass"] = props.get("ro3_pass")
            flattened["rtb"] = props.get("rtb")
            flattened["acd_logd"] = props.get("acd_logd")
            flattened["acd_logp"] = props.get("acd_logp")
            flattened["acd_most_apka"] = props.get("acd_most_apka")
            flattened["acd_most_bpka"] = props.get("acd_most_bpka")

            # Механизм действия
            flattened["mechanism_of_action"] = props.get("mechanism_of_action")
            flattened["molecular_mechanism"] = props.get("molecular_mechanism")
            flattened["direct_interaction"] = props.get("direct_interaction")

        # Extract from molecule_structures
        if "molecule_structures" in molecule and molecule["molecule_structures"]:
            structures = molecule["molecule_structures"]
            flattened["canonical_smiles"] = structures.get("canonical_smiles")
            flattened["standard_inchi"] = structures.get("standard_inchi")
            # ChEMBL API may not always return standard_inchi_key
            flattened["standard_inchi_key"] = structures.get("standard_inchi_key") if structures.get("standard_inchi_key") else None

        # Extract from molecule_synonyms
        if "molecule_synonyms" in molecule and molecule["molecule_synonyms"]:
            synonyms = molecule["molecule_synonyms"]
            if isinstance(synonyms, list) and synonyms:
                # Конкатенируем все синонимы в одну строку для all_names
                synonym_names = []
                for syn in synonyms:
                    if isinstance(syn, dict) and "molecule_synonym" in syn:
                        synonym_names.append(syn["molecule_synonym"])
                    elif isinstance(syn, str):
                        synonym_names.append(syn)
                flattened["all_names"] = "; ".join(synonym_names) if synonym_names else None
                # Сериализуем как JSON string для molecule_synonyms
                flattened["molecule_synonyms"] = json.dumps(synonyms)
            else:
                flattened["all_names"] = None
                flattened["molecule_synonyms"] = None
        else:
            flattened["all_names"] = None
            flattened["molecule_synonyms"] = None

        # Extract from atc_classifications
        if "atc_classifications" in molecule and molecule["atc_classifications"]:
            atc_classifications = molecule["atc_classifications"]
            if isinstance(atc_classifications, list) and atc_classifications:
                # Сериализуем как JSON string для сохранения структуры
                flattened["atc_classifications"] = json.dumps(atc_classifications)
            else:
                flattened["atc_classifications"] = None

        # Extract from cross_references
        if "cross_references" in molecule and molecule["cross_references"]:
            cross_refs = molecule["cross_references"]
            if isinstance(cross_refs, list) and cross_refs:
                # Сериализуем как JSON string
                flattened["cross_references"] = json.dumps(cross_refs)
            else:
                flattened["cross_references"] = None

        # Extract from biotherapeutic
        if "biotherapeutic" in molecule and molecule["biotherapeutic"]:
            biotherapeutic = molecule["biotherapeutic"]
            if isinstance(biotherapeutic, dict):
                # Извлекаем основные поля биотерапевтического соединения
                flattened["biotherapeutic"] = json.dumps(biotherapeutic)
                # Можно добавить отдельные поля если нужно:
                # flattened["peptide_sequence"] = biotherapeutic.get("peptide_sequence")
                # flattened["helm_notation"] = biotherapeutic.get("helm_notation")
            else:
                flattened["biotherapeutic"] = None

        # Extract molecule_hierarchy as JSON
        if "molecule_hierarchy" in molecule and molecule["molecule_hierarchy"]:
            hierarchy = molecule["molecule_hierarchy"]
            flattened["molecule_hierarchy"] = json.dumps(hierarchy)

        # Extract molecule_properties as JSON
        if "molecule_properties" in molecule and molecule["molecule_properties"]:
            props = molecule["molecule_properties"]
            flattened["molecule_properties"] = json.dumps(props)

        # Extract molecule_structures as JSON
        if "molecule_structures" in molecule and molecule["molecule_structures"]:
            structures = molecule["molecule_structures"]
            flattened["molecule_structures"] = json.dumps(structures)

        # Extract boolean flags
        if "orphan" in molecule:
            flattened["orphan"] = bool(molecule.get("orphan", False))

        if "veterinary" in molecule:
            flattened["veterinary"] = bool(molecule.get("veterinary", False))

        if "chemical_probe" in molecule:
            flattened["chemical_probe"] = bool(molecule.get("chemical_probe", False))

        # Extract helm_notation if present
        if "helm_notation" in molecule:
            flattened["helm_notation"] = molecule.get("helm_notation")

        # Extract chirality_chembl
        if "chirality_chembl" in molecule:
            flattened["chirality_chembl"] = molecule.get("chirality_chembl")

        # Extract molecule_type_chembl
        if "molecule_type_chembl" in molecule:
            flattened["molecule_type_chembl"] = molecule.get("molecule_type_chembl")

        return flattened

    def _extract_from_pubchem(self, data: pd.DataFrame) -> pd.DataFrame:
        """Extract data from PubChem."""
        if data.empty:
            return pd.DataFrame()

        # Get PubChem client
        client = self.clients.get("pubchem")
        if not client:
            logger.warning("PubChem client not available")
            return pd.DataFrame()

        # Check if PubChem is enabled
        if not getattr(self.config.sources["pubchem"], "enabled", True):
            logger.info("PubChem extraction disabled")
            return pd.DataFrame()

        # Get InChI Keys for PubChem lookup
        # Check if standard_inchi_key column exists
        if "standard_inchi_key" not in data.columns:
            logger.warning("No standard_inchi_key column found for PubChem extraction")
            return pd.DataFrame()

        inchi_keys = data["standard_inchi_key"].dropna().unique().tolist()
        if not inchi_keys:
            logger.warning("No InChI Keys found for PubChem extraction")
            return pd.DataFrame()

        logger.info(f"Extracting PubChem data for {len(inchi_keys)} InChI Keys")

        # PubChem fields to extract
        pubchem_fields = [
            "pubchem_cid",
            "pubchem_molecular_formula",
            "pubchem_molecular_weight",
            "pubchem_canonical_smiles",
            "pubchem_inchi",
            "pubchem_inchi_key",
            "pubchem_registry_id",
            "pubchem_rn",
        ]

        # Process in batches (PubChem rate limit: 5 requests/second)
        all_records = []

        for i, inchi_key in enumerate(inchi_keys):
            try:
                # Rate limiting: wait 0.2 seconds between requests (5 req/sec)
                if i > 0:
                    import time

                    time.sleep(0.2)

                record_data = self._fetch_pubchem_record(inchi_key, pubchem_fields, client)
                if not record_data.empty:
                    all_records.append(record_data)

            except Exception as e:
                logger.error(f"Failed to fetch PubChem data for InChI Key {inchi_key}: {e}")
                continue

        if not all_records:
            logger.warning("No PubChem data extracted")
            return pd.DataFrame()

        # Combine all records
        result = pd.concat(all_records, ignore_index=True)

        logger.info(f"PubChem extraction completed: {len(result)} records")
        return result

    def _fetch_pubchem_record(self, inchi_key: str, fields: list[str], client) -> pd.DataFrame:
        """Fetch a single record from PubChem API using InChI Key."""
        import requests

        # Build URL for PubChem PUG-REST API
        base_url = self.config.sources["pubchem"].http.base_url
        url = f"{base_url}/compound/inchikey/{inchi_key}/property/MolecularFormula,MolecularWeight,ConnectivitySMILES,InChI,InChIKey/JSON"

        try:
            # Make request
            response = client.get(url, timeout=self.config.sources["pubchem"].http.timeout_sec)
            response.raise_for_status()

            data = response.json()
            if "PropertyTable" not in data or "Properties" not in data["PropertyTable"]:
                logger.warning("No properties found in PubChem response for InChI Key: %s", inchi_key)
                return pd.DataFrame()

            properties = data["PropertyTable"]["Properties"]
            if not properties:
                return pd.DataFrame()

            # Convert to DataFrame
            record = properties[0]  # Take first property set

            # Map PubChem fields to our schema
            mapped_record = {
                "inchi_key_from_mol": inchi_key,
                "pubchem_cid": record.get("CID"),
                "pubchem_molecular_formula": record.get("MolecularFormula"),
                "pubchem_molecular_weight": record.get("MolecularWeight"),
                "pubchem_canonical_smiles": record.get("ConnectivitySMILES"),
                "pubchem_inchi": record.get("InChI"),
                "pubchem_inchi_key": record.get("InChIKey"),
                "pubchem_registry_id": None,  # Not available in this API endpoint
                "pubchem_rn": None,  # Not available in this API endpoint
            }

            df = pd.DataFrame([mapped_record])
            return df

        except requests.RequestException as e:
            logger.error(f"PubChem API request failed for InChI Key {inchi_key}: {e}")
            raise
        except Exception as e:
            logger.error(f"Failed to process PubChem response for InChI Key {inchi_key}: {e}")
            raise

    def _merge_chembl_data(self, base_data: pd.DataFrame, chembl_data: pd.DataFrame) -> pd.DataFrame:
        """Merge ChEMBL data into base data."""
        if chembl_data.empty:
            logger.warning("No ChEMBL data to merge")
            return base_data

        # Remove duplicates from ChEMBL data (keep first occurrence)
        chembl_data = chembl_data.drop_duplicates(subset=["molecule_chembl_id"], keep="first")

        # Create a mapping from molecule_chembl_id to ChEMBL data
        chembl_dict = chembl_data.set_index("molecule_chembl_id").to_dict("index")

        # Start with base data
        merged_data = base_data.copy()

        # For each row in base data, enrich with ChEMBL data
        for idx, row in merged_data.iterrows():
            molecule_id = row["molecule_chembl_id"]
            if molecule_id in chembl_dict:
                chembl_row = chembl_dict[molecule_id]

                # Update existing columns with ChEMBL data (prefer non-null ChEMBL values)
                for col, value in chembl_row.items():
                    if col in merged_data.columns:
                        # Only update if ChEMBL value is not null and not empty
                        if self._is_valid_value(value):
                            # Convert value to appropriate type if needed
                            try:
                                # Try to maintain the original column dtype
                                original_dtype = merged_data[col].dtype
                                if original_dtype == "float64" and isinstance(value, str):
                                    # Try to convert string to float
                                    try:
                                        converted_value = float(value)
                                        merged_data.at[idx, col] = converted_value
                                    except (ValueError, TypeError):
                                        # If conversion fails, keep original value
                                        pass
                                elif original_dtype == "int64" and isinstance(value, str):
                                    # Try to convert string to int
                                    try:
                                        converted_value = int(float(value))  # int(float()) handles "123.0"
                                        merged_data.at[idx, col] = converted_value
                                    except (ValueError, TypeError):
                                        # If conversion fails, keep original value
                                        pass
                                else:
                                    merged_data.at[idx, col] = value
                            except (ValueError, TypeError):
                                # If type conversion fails, keep original value
                                pass
                    else:
                        # Add new column if it doesn't exist
                        if col not in merged_data.columns:
                            merged_data[col] = None
                        try:
                            merged_data.at[idx, col] = value
                        except (ValueError, TypeError):
                            # If type conversion fails, convert to string
                            merged_data.at[idx, col] = str(value)

        # Debug: log what columns we have after merge
        logger.debug("Columns after ChEMBL merge: %s", list(merged_data.columns))
        if "parent_chembl_id" in merged_data.columns:
            logger.debug("parent_chembl_id values: %s", merged_data["parent_chembl_id"].tolist())
        else:
            logger.debug("parent_chembl_id column not found after merge")

        # Log statistics
        enriched_count = 0
        if "molregno" in merged_data.columns:
            enriched_count = merged_data["molregno"].notna().sum()
        elif "molecule_chembl_id" in merged_data.columns:
            # Count records that have ChEMBL data by checking if any ChEMBL field is not null
            chembl_fields = ["parent_chembl_id", "mechanism_of_action", "direct_interaction"]
            for field in chembl_fields:
                if field in merged_data.columns:
                    enriched_count = merged_data[field].notna().sum()
                    break

        total_count = len(merged_data)
        enrichment_rate = enriched_count / total_count if total_count > 0 else 0

        # Ensure numeric columns maintain their proper dtypes
        numeric_columns = [
            "molregno",
            "parent_molregno",
            "max_phase",
            "mw_freebase",
            "alogp",
            "hba",
            "hbd",
            "psa",
            "rtb",
            "ro3_pass",
            "num_ro5_violations",
            "acd_most_apka",
            "acd_most_bpka",
            "acd_logp",
            "acd_logd",
            "full_mwt",
            "aromatic_rings",
            "heavy_atoms",
            "qed_weighted",
            "mw_monoisotopic",
            "hba_lipinski",
            "hbd_lipinski",
            "num_lipinski_ro5_violations",
            "first_approval",
            "usan_year",
            "withdrawn_year",
            "pubchem_molecular_weight",
            "pubchem_cid",
        ]

        for col in numeric_columns:
            if col in merged_data.columns:
                try:
                    merged_data[col] = pd.to_numeric(merged_data[col], errors="coerce")
                except (ValueError, TypeError):
                    # If conversion fails, keep as is
                    pass

        logger.info("ChEMBL data merge completed: %d/%d records enriched (%.1f%%)", int(enriched_count), int(total_count), float(enrichment_rate * 100))

        return merged_data

    def _merge_pubchem_data(self, base_data: pd.DataFrame, pubchem_data: pd.DataFrame) -> pd.DataFrame:
        """Merge PubChem data into base data."""
        if pubchem_data.empty:
            logger.warning("No PubChem data to merge")
            return base_data

        # Remove duplicates from PubChem data (keep first occurrence)
        pubchem_data = pubchem_data.drop_duplicates(subset=["inchi_key_from_mol"], keep="first")

        # Perform left join on InChI Key
        merged_data = base_data.merge(pubchem_data, left_on="standard_inchi_key", right_on="inchi_key_from_mol", how="left", suffixes=("", "_pubchem"))

        # Log statistics
        enriched_count = merged_data["pubchem_cid"].notna().sum()
        total_count = len(merged_data)
        enrichment_rate = enriched_count / total_count if total_count > 0 else 0

        logger.info("PubChem data merge completed: %d/%d records enriched (%.1f%%)", int(enriched_count), int(total_count), float(enrichment_rate * 100))

        return merged_data

    def normalize(self, raw_data: pd.DataFrame) -> pd.DataFrame:
        """Normalize testitem data."""
        logger.info("Normalizing testitem data")

        # Apply testitem normalization
        normalized_data = self.normalizer.normalize_testitems(raw_data)

        logger.info(f"Normalized {len(normalized_data)} testitems")
        return normalized_data

    def validate(self, data: pd.DataFrame) -> pd.DataFrame:
        """Validate testitem data."""
        logger.info("Validating testitem data")

        # Validate normalized data
        validated_data = self.validator.validate_normalized(data)

        logger.info(f"Validated {len(validated_data)} testitems")
        return validated_data

    def filter_quality(self, data: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Filter testitems by quality."""
        logger.info("Filtering testitems by quality")

        # Apply quality filters
        accepted_data, rejected_data = self.quality_filter.apply_moderate_quality_filter(data)

        logger.info(f"Quality filtering: {len(accepted_data)} accepted, {len(rejected_data)} rejected")
        return accepted_data, rejected_data

    def _get_entity_type(self) -> str:
        """Получить тип сущности для пайплайна."""
        return "testitems"

    def _create_qc_validator(self) -> Any:
        """Создать QC валидатор для пайплайна."""
        from library.common.qc_profiles import QCProfile, TestitemQCValidator

        # Создаем базовый QC профиль для теститемов
        qc_profile = QCProfile(name="testitem_qc", description="Quality control profile for testitems", rules=[])

        return TestitemQCValidator(qc_profile)

    def _create_postprocessor(self) -> Any:
        """Создать постпроцессор для пайплайна."""
        from library.common.postprocess_base import TestitemPostprocessor

        return TestitemPostprocessor(self.config)

    def _create_etl_writer(self) -> Any:
        """Создать ETL writer для пайплайна."""
        from library.common.writer_base import create_etl_writer

        return create_etl_writer(self.config, "testitems")

    def _build_metadata(
        self,
        data: pd.DataFrame,
        accepted_data: pd.DataFrame | None = None,
        rejected_data: pd.DataFrame | None = None,
        correlation_analysis: dict[str, Any] | None = None,
        correlation_insights: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Build metadata for testitem pipeline."""
        # Use accepted_data as data if not provided
        if accepted_data is None:
            accepted_data = data
        if rejected_data is None:
            rejected_data = pd.DataFrame()

        # Create base metadata dictionary
        current_time = pd.Timestamp.now().isoformat()

        # Calculate source statistics
        source_counts = {}
        if accepted_data is not None and not accepted_data.empty:
            # ChEMBL enrichment - check for any ChEMBL data
            chembl_enriched = 0
            if "molecule_chembl_id" in accepted_data.columns:
                # Count records that have molecule_chembl_id (main indicator of ChEMBL data)
                chembl_enriched = accepted_data["molecule_chembl_id"].notna().sum()
            source_counts["chembl"] = chembl_enriched

            # PubChem enrichment
            pubchem_enriched = accepted_data["pubchem_cid"].notna().sum() if "pubchem_cid" in accepted_data.columns else 0
            source_counts["pubchem"] = pubchem_enriched

        # PubChem enrichment statistics
        pubchem_enrichment = {"enabled": getattr(self.config.sources["pubchem"], "enabled", True), "enrichment_rate": 0.0, "records_with_pubchem_data": 0}

        if accepted_data is not None and not accepted_data.empty and "pubchem_cid" in accepted_data.columns:
            total_records = len(accepted_data)
            pubchem_records = accepted_data["pubchem_cid"].notna().sum()
            pubchem_enrichment["enrichment_rate"] = pubchem_records / total_records if total_records > 0 else 0.0
            pubchem_enrichment["records_with_pubchem_data"] = int(pubchem_records)

        metadata = {
            "pipeline": {
                "pipeline_version": "2.0.0",
                "config": self.config.model_dump() if hasattr(self.config, "model_dump") else {},
                "source_counts": source_counts,
                "pubchem_enrichment": pubchem_enrichment,
            },
            "execution": {
                "run_id": f"testitem_run_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}",
                "pipeline_name": "testitems",
                "pipeline_version": "2.0.0",
                "entity_type": "testitems",
                "sources_enabled": [name for name, source in self.config.sources.items() if source.enabled],
                "started_at": current_time,
                "completed_at": current_time,
                "extraction_timestamp": current_time,
                "config": self.config.model_dump() if hasattr(self.config, "model_dump") else {},
            },
            "data": {
                "total_testitems": len(accepted_data) if accepted_data is not None else 0,
                "accepted_testitems": len(accepted_data) if accepted_data is not None else 0,
                "rejected_testitems": len(rejected_data) if rejected_data is not None else 0,
                "columns": list(accepted_data.columns) if accepted_data is not None and not accepted_data.empty else [],
            },
            "validation": {
                "validation_passed": True,
                "quality_filter_passed": len(rejected_data) == 0 if rejected_data is not None else True,
            },
        }

        # Add correlation analysis if provided
        if correlation_analysis is not None:
            metadata["correlation_analysis"] = correlation_analysis
        if correlation_insights is not None:
            metadata["correlation_insights"] = correlation_insights

        return metadata
>>>>>>> Stashed changes


def write_testitem_outputs(
    result: TestitemETLResult, 
    output_dir: Path, 
    config: TestitemConfig
) -> dict[str, Path]:
    """Persist ETL artefacts to disk and return the generated paths."""

    # Get basic output paths from persist function
    result_paths = persist_testitem_data(result.testitems, output_dir, config)
    
    # Save correlation reports if available
    if result.correlation_reports:
        try:
            from datetime import datetime
            date_tag = datetime.now().strftime("%Y%m%d")
            correlation_dir = output_dir / f"testitem_correlation_report_{date_tag}"
            correlation_dir.mkdir(exist_ok=True)
            
            correlation_paths = {}
            for report_name, report_df in result.correlation_reports.items():
                report_path = correlation_dir / f"{report_name}.csv"
                report_df.to_csv(report_path, index=False)
                correlation_paths[report_name] = report_path
                
            logger.info(f"Saved {len(correlation_paths)} correlation reports to {correlation_dir}")
            result_paths["correlation_reports"] = correlation_paths
            
            # Save correlation insights if available
            if result.correlation_insights:
                import json
                insights_path = correlation_dir / "correlation_insights.json"
                with open(insights_path, 'w', encoding='utf-8') as f:
                    json.dump(result.correlation_insights, f, ensure_ascii=False, indent=2)
                result_paths["correlation_insights"] = insights_path
                
        except Exception as exc:
            logger.warning(f"Failed to save correlation reports: {exc}")
    
    return result_paths


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
