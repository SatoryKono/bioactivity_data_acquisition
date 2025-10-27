"""Main ETL pipeline orchestration for testitem data."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import requests

from library.clients.chembl import ChEMBLClient, TestitemChEMBLClient
from library.clients.pubchem import PubChemClient
from library.common.pipeline_base import PipelineBase
from library.config import APIClientConfig, RateLimitSettings, RetrySettings
from library.testitem.config import TestitemConfig
from library.testitem.normalize import TestitemNormalizer
from library.testitem.persist import persist_testitem_data
from library.testitem.quality import TestitemQualityFilter
from library.testitem.validate import TestitemValidator
from library.testitem.constants import TESTITEM_CHEMBL_FIELDS

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
        frame = pd.read_csv(path, low_memory=False)
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
        normalised["molecule_chembl_id"] = normalised["molecule_chembl_id"].astype(str).str.strip().replace(["None", "nan", "NaN", "none", "NULL", "null"], pd.NA)
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
                normalised[field] = normalised[field].astype(str).str.strip().replace(["None", "nan", "NaN", "none", "NULL", "null"], pd.NA)
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

    # Create and run pipeline
    pipeline = TestitemPipeline(config)
    
    # Run the pipeline
    result = pipeline.run(normalised)
    
    return result
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
        logger.info(f"Setting up clients. Available sources: {list(self.config.sources.keys())}")

        # ChEMBL client
        if "chembl" in self.config.sources and self.config.sources["chembl"].enabled:
            logger.info("Creating ChEMBL client")
            self.clients["chembl"] = self._create_chembl_client()

        # PubChem client
        if "pubchem" in self.config.sources and self.config.sources["pubchem"].enabled:
            logger.info("Creating PubChem client")
            self.clients["pubchem"] = self._create_pubchem_client()
        
        logger.info(f"Clients created: {list(self.clients.keys())}")

    def _create_chembl_client(self) -> TestitemChEMBLClient:
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
        
        # Get retry settings as dict
        retries_dict = {
            "total": retries.total,
            "backoff_multiplier": retries.backoff_multiplier,
            "backoff_max": getattr(retries, "backoff_max", 120.0)
        }

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
            retries=retries_dict,
            rate_limit=rate_limit,
            headers=processed_headers,
        )

        return TestitemChEMBLClient(client_config)

    def _create_pubchem_client(self) -> Any:
        """Create PubChem client."""
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

        # Get ChEMBL status and release
        logger.info("Getting ChEMBL status and release...")
        chembl_client = self.clients.get("chembl")
        if chembl_client:
            try:
                status_info = chembl_client.get_chembl_status()
                self.chembl_version = status_info.get("version", "unknown")
                release_date = status_info.get("release_date")
                if self.chembl_version is None:
                    self.chembl_version = "unknown"
                logger.info(f"ChEMBL version: {self.chembl_version}, release date: {release_date}")
            except Exception as e:
                logger.warning(f"Failed to get ChEMBL version: {e}")
                self.chembl_version = "unknown"
        else:
            logger.warning("ChEMBL client not available, using unknown for chembl_release")
            self.chembl_version = "unknown"

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
        else:
            logger.warning(f"PubChem client not found in clients dict. Available clients: {list(self.clients.keys())}")

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

        # Use fields from constants - these are the fields we request from ChEMBL API
        chembl_fields = list(TESTITEM_CHEMBL_FIELDS)

        # Use streaming batch processing with configurable batch size
        batch_size = getattr(self.config.runtime, 'batch_size', 25)
        logger.info(f"Using streaming batch processing with batch_size={batch_size}")
        
        all_records = []
        batch_index = 0
        
        # Use client's streaming batch API for better memory efficiency
        for batch_ids, batch_results in client.fetch_molecules_batch_streaming(molecule_ids, batch_size, fields=chembl_fields):
            batch_index += 1
            logger.info(f"Processing ChEMBL batch {batch_index}: {len(batch_ids)} molecules")
            
            try:
                # Convert batch results to DataFrame
                batch_data = self._convert_batch_to_dataframe(batch_results, chembl_fields)
                if not batch_data.empty:
                    all_records.append(batch_data)
                    logger.info(f"Batch {batch_index} completed: {len(batch_data)} records")
                else:
                    logger.warning(f"Batch {batch_index} returned no data")
            except Exception as e:
                logger.error(f"Failed to process ChEMBL batch {batch_index}: {e}")
                continue

        if not all_records:
            logger.warning("No ChEMBL data extracted")
            return pd.DataFrame()

        # Combine all records
        result = pd.concat(all_records, ignore_index=True)
        result["retrieved_at"] = pd.Timestamp.now().isoformat()

        logger.info(f"ChEMBL extraction completed: {len(result)} records from {batch_index} batches")
        return result

    def _convert_batch_to_dataframe(self, batch_results: dict[str, dict[str, Any]], chembl_fields: list[str]) -> pd.DataFrame:
        """Convert batch results from client to DataFrame."""
        if not batch_results:
            return pd.DataFrame()
        
        # Convert batch results to list of records
        records = []
        for molecule_id, molecule_data in batch_results.items():
            if molecule_data:
                # Flatten nested structures
                flattened = self._flatten_molecule_data(molecule_data)
                records.append(flattened)
        
        if not records:
            return pd.DataFrame()
        
        # Convert to DataFrame
        df = pd.DataFrame(records)
        
        # Ensure all expected fields are present
        for field in chembl_fields:
            if field not in df.columns:
                df[field] = None
        
        return df

    def _flatten_molecule_data(self, molecule: dict) -> dict:
        """Flatten nested molecule structures into top-level fields."""
        import json

        flattened = molecule.copy()
        
        # Ensure top-level fields are preserved
        # These fields come directly from the API response (not nested)
        top_level_fields = [
            "molecule_chembl_id", "molregno", "pref_name", "parent_molecule_chembl_id",
            "max_phase", "molecule_type", "first_approval", "structure_type",
            "therapeutic_flag", "dosed_ingredient",
            "oral", "parenteral", "topical", "black_box_warning",
            "natural_product", "first_in_class", "chirality", "prodrug",
            "inorganic_flag", "polymer_flag",
            "usan_year", "availability_type", "usan_stem", "usan_substem",
            "usan_stem_definition", "indication_class",
            "withdrawn_flag", "withdrawn_year", "withdrawn_country", "withdrawn_reason",
            "mechanism_of_action", "direct_interaction", "molecular_mechanism",
            "drug_chembl_id", "drug_name", "drug_type",
            "drug_substance_flag", "drug_indication_flag", "drug_antibacterial_flag",
            "drug_antiviral_flag", "drug_antifungal_flag", "drug_antiparasitic_flag",
            "drug_antineoplastic_flag", "drug_immunosuppressant_flag", "drug_antiinflammatory_flag",
            "orphan", "veterinary", "chemical_probe", "helm_notation"
        ]
        
        # These should already be in flattened, but we ensure they're present
        for field in top_level_fields:
            if field in molecule and field not in flattened:
                flattened[field] = molecule.get(field)

        # Extract from molecule_hierarchy
        if "molecule_hierarchy" in molecule and molecule["molecule_hierarchy"]:
            hierarchy = molecule["molecule_hierarchy"]
            flattened["parent_chembl_id"] = hierarchy.get("parent_chembl_id")
            flattened["parent_molregno"] = hierarchy.get("parent_molregno")
            # Note: parent_chembl_id should map to parent_chembl_id (not parent_molecule_chembl_id)
            # which is different from parent_molecule_chembl_id field

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
            
            # Additional properties that might be available
            # These are included to ensure we capture all data if available

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
        
        # Derive pref_name_key from pref_name (lowercase for searching)
        if "pref_name" in flattened and flattened["pref_name"] and pd.notna(flattened["pref_name"]):
            try:
                flattened["pref_name_key"] = str(flattened["pref_name"]).lower().strip()
            except Exception:
                flattened["pref_name_key"] = None
        else:
            flattened["pref_name_key"] = None
        
        # Derive salt_chembl_id: if molecule has a parent and it's different from itself, it's a salt
        if "molecule_chembl_id" in flattened and "parent_chembl_id" in flattened:
            molecule_id = flattened.get("molecule_chembl_id")
            parent_id = flattened.get("parent_chembl_id")
            
            # Check if this is a salt (has parent and parent is different from itself)
            if parent_id and molecule_id and parent_id != molecule_id:
                flattened["salt_chembl_id"] = molecule_id
            else:
                flattened["salt_chembl_id"] = None
        else:
            flattened["salt_chembl_id"] = None

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

        # Use configurable batch processing for PubChem
        pubchem_batch_size = getattr(self.config.runtime, 'pubchem_batch_size', 100)
        logger.info(f"Using PubChem batch processing with batch_size={pubchem_batch_size}")
        
        all_records = []
        batch_index = 0
        
        # Process InChI Keys in batches
        for i in range(0, len(inchi_keys), pubchem_batch_size):
            batch_keys = inchi_keys[i:i + pubchem_batch_size]
            batch_index += 1
            logger.info(f"Processing PubChem batch {batch_index}: {len(batch_keys)} InChI Keys")
            
            batch_records = []
            for j, inchi_key in enumerate(batch_keys):
                try:
                    # Rate limiting: wait 0.2 seconds between requests (5 req/sec)
                    if j > 0:
                        import time
                        time.sleep(0.2)
                    
                    record_data = self._fetch_pubchem_record(inchi_key, pubchem_fields, client)
                    if not record_data.empty:
                        batch_records.append(record_data)
                        
                except Exception as e:
                    logger.error(f"Failed to fetch PubChem data for InChI Key {inchi_key}: {e}")
                    continue
            
            if batch_records:
                all_records.extend(batch_records)
                logger.info(f"PubChem batch {batch_index} completed: {len(batch_records)} records")
            else:
                logger.warning(f"PubChem batch {batch_index} returned no data")

        if not all_records:
            logger.warning("No PubChem data extracted")
            return pd.DataFrame()

        # Combine all records
        result = pd.concat(all_records, ignore_index=True)

        logger.info(f"PubChem extraction completed: {len(result)} records from {batch_index} batches")
        return result

    def _fetch_pubchem_record(self, inchi_key: str, fields: list[str], client) -> pd.DataFrame:
        """Fetch a single record from PubChem API using InChI Key."""
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
            if hasattr(e, 'response') and e.response is not None:
                if e.response.status_code == 404:
                    logger.warning(f"PubChem record not found for InChI Key {inchi_key}: {e}")
                    return pd.DataFrame()  # Return empty DataFrame instead of raising
                else:
                    logger.error(f"PubChem API request failed for InChI Key {inchi_key}: {e}")
                    raise
            else:
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

        # Collect all new columns that need to be added
        new_columns = {}
        for chembl_row in chembl_dict.values():
            for col in chembl_row.keys():
                if col not in merged_data.columns:
                    new_columns[col] = None

        # Add all new columns at once to avoid fragmentation
        if new_columns:
            for col, default_value in new_columns.items():
                merged_data[col] = default_value

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
                        # This should not happen now since we added all columns upfront
                        try:
                            merged_data.at[idx, col] = value
                        except (ValueError, TypeError):
                            # If type conversion fails, convert to string
                            merged_data.at[idx, col] = str(value)

        # Debug: log what columns we have after merge
        logger.debug(f"Columns after ChEMBL merge: {list(merged_data.columns)}")
        if "parent_chembl_id" in merged_data.columns:
            logger.debug(f"parent_chembl_id values: {merged_data['parent_chembl_id'].tolist()}")
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

        logger.info(f"ChEMBL data merge completed: {int(enriched_count)}/{int(total_count)} records enriched ({float(enrichment_rate * 100):.1f}%)")

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

        logger.info(f"PubChem data merge completed: {int(enriched_count)}/{int(total_count)} records enriched ({float(enrichment_rate * 100):.1f}%)")

        return merged_data

    def normalize(self, raw_data: pd.DataFrame) -> pd.DataFrame:
        """Normalize testitem data."""
        logger.info("Normalizing testitem data")

        # Apply testitem normalization
        normalized_data = self.normalizer.normalize_testitems(raw_data)

        # Add chembl_release to all records
        if hasattr(self, 'chembl_version'):
            normalized_data["chembl_release"] = self.chembl_version
        else:
            normalized_data["chembl_release"] = "unknown"
            logger.warning("chembl_version not set, using 'unknown' for chembl_release")

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
                "chembl_release": getattr(self, 'chembl_version', 'unknown'),
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


def write_testitem_outputs(
    result: TestitemETLResult, 
    output_dir: Path, 
    config: TestitemConfig
) -> dict[str, Path]:
    """Persist ETL artefacts to disk and return the generated paths."""

    # Get basic output paths from persist function
    result_paths = persist_testitem_data(result.data, output_dir, config)
    
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
