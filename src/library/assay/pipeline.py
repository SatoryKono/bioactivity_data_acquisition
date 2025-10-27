"""Assay ETL pipeline orchestration."""

from __future__ import annotations

import hashlib
import logging
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from library.assay.client import AssayChEMBLClient
from library.assay.config import AssayConfig
from library.config import APIClientConfig
from library.etl.enhanced_correlation import (
    build_enhanced_correlation_analysis,
    build_enhanced_correlation_reports,
    build_correlation_insights,
    prepare_data_for_correlation_analysis,
)
from library.etl.load import write_deterministic_csv
from library.schemas.assay_schema import AssayNormalizedSchema

logger = logging.getLogger(__name__)


class AssayPipelineError(RuntimeError):
    """Base class for assay pipeline errors."""


class AssayValidationError(AssayPipelineError):
    """Raised when the input data does not meet schema expectations."""


class AssayHTTPError(AssayPipelineError):
    """Raised when upstream HTTP requests fail irrecoverably."""


class AssayQCError(AssayPipelineError):
    """Raised when QC checks do not pass configured thresholds."""


class AssayIOError(AssayPipelineError):
    """Raised when reading or writing files fails."""


@dataclass(slots=True)
class AssayETLResult:
    """Container for ETL artefacts."""

    assays: pd.DataFrame
    qc: pd.DataFrame
    meta: dict[str, Any]
    correlation_analysis: dict[str, Any] | None = None
    correlation_reports: dict[str, pd.DataFrame] | None = None
    correlation_insights: list[dict[str, Any]] | None = None


_REQUIRED_COLUMNS = {"assay_chembl_id"}


def _create_api_client(config: AssayConfig) -> AssayChEMBLClient:
    """Create an API client for ChEMBL."""
    from library.config import RateLimitSettings, RetrySettings
    
    # Get ChEMBL-specific configuration
    chembl_config = config.sources.get("chembl")
    if not chembl_config:
        raise AssayValidationError("ChEMBL source not found in configuration")
    
    # Use source-specific timeout or fallback to global
    timeout = chembl_config.http.timeout_sec or config.http.global_.timeout_sec
    if timeout is None:
        timeout = 60.0  # At least 60 seconds for ChEMBL
    
    # Merge headers: default + global + source-specific
    default_headers = _get_headers()
    headers = {**default_headers, **config.http.global_.headers, **chembl_config.http.headers}
    
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
    base_url = chembl_config.http.base_url or "https://www.ebi.ac.uk/chembl/api/data"
    
    # Use source-specific retry settings or fallback to global
    retry_settings = RetrySettings(
        total=chembl_config.http.retries.get('total', config.http.global_.retries.total),
        backoff_multiplier=chembl_config.http.retries.get('backoff_multiplier', config.http.global_.retries.backoff_multiplier)
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
   
    return AssayChEMBLClient(api_config, timeout=timeout)


def _get_headers() -> dict[str, str]:
    """Get default headers for ChEMBL API."""
    return {
        "User-Agent": "bioactivity-data-acquisition/0.1.0",
        "Accept": "application/json",
    }


def read_assay_input(path: Path) -> pd.DataFrame:
    """Load the input CSV containing assay identifiers."""

    try:
        frame = pd.read_csv(path)
    except FileNotFoundError as exc:
        raise AssayIOError(f"Input CSV not found: {path}") from exc
    except pd.errors.EmptyDataError as exc:
        raise AssayValidationError("Input CSV is empty") from exc
    except OSError as exc:  # pragma: no cover - filesystem-level errors are rare
        raise AssayIOError(f"Failed to read input CSV: {exc}") from exc
    return frame


def _normalise_columns(frame: pd.DataFrame) -> pd.DataFrame:
    """Normalize input columns."""
    normalised = frame.copy()
    
    present = {column for column in normalised.columns}
    missing = _REQUIRED_COLUMNS - present
    if missing:
        raise AssayValidationError(
            f"Input data is missing required columns: {', '.join(sorted(missing))}"
        )
    
    # Normalize assay_chembl_id
    normalised["assay_chembl_id"] = normalised["assay_chembl_id"].astype(str).str.strip()
    
    # Normalize target_chembl_id if present
    if "target_chembl_id" in normalised.columns:
        normalised["target_chembl_id"] = normalised["target_chembl_id"].astype(str).str.strip()
    
    normalised = normalised.sort_values("assay_chembl_id").reset_index(drop=True)
    return normalised


def _extract_assay_data(
    client: AssayChEMBLClient, 
    frame: pd.DataFrame, 
    config: AssayConfig
) -> pd.DataFrame:
    """Extract assay data from ChEMBL API using streaming batch processing."""
    
    # Define default values for all possible columns to ensure they exist
    default_columns = {
        # Core assay fields
        "assay_chembl_id": None, "src_id": None, "src_assay_id": None,
        "assay_type": None, "assay_type_description": None, "bao_format": None, "bao_label": None,
        "assay_category": None, "assay_classifications": None,
        "target_chembl_id": None, "relationship_type": None, "confidence_score": None,
        "assay_organism": None, "assay_tax_id": None, "assay_cell_type": None,
        "assay_tissue": None, "assay_strain": None, "assay_subcellular_fraction": None,
        "description": None, "assay_parameters": None, "assay_format": None,
        "source_system": "ChEMBL", "extracted_at": None, "hash_row": None, "hash_business_key": None,
        "chembl_release": None,
        
        # Assay Classification
        "assay_class_id": None,
        "assay_class_bao_id": None,
        "assay_class_type": None,
        "assay_class_l1": None,
        "assay_class_l2": None,
        "assay_class_l3": None,
        "assay_class_description": None,
        
        # Assay Parameters expanded
        "assay_parameters_json": None,
        "assay_param_type": None,
        "assay_param_relation": None,
        "assay_param_value": None,
        "assay_param_units": None,
        "assay_param_text_value": None,
        "assay_param_standard_type": None,
        "assay_param_standard_value": None,
        "assay_param_standard_units": None,
        
        # Variant Sequences
        "variant_id": None,
        "variant_base_accession": None,
        "variant_mutation": None,
        "variant_sequence": None,
        "variant_accession_reported": None,
        "variant_sequence_json": None,
        
        # Additional metadata
        "document_chembl_id": None,
        "tissue_chembl_id": None,
        "cell_chembl_id": None,
        "assay_group": None,
        "assay_test_type": None,
        "bao_endpoint": None,
        "confidence_description": None,
        "relationship_description": None,
        "pipeline_version": "1.0.0",
        "assay_description": None,  # alias для description
    }
    
    # Extract valid assay IDs
    valid_assay_ids = []
    row_mapping = {}
    
    for idx, (_, row) in enumerate(frame.iterrows()):
        assay_id = str(row["assay_chembl_id"]).strip()
        if assay_id and assay_id != "nan":
            valid_assay_ids.append(assay_id)
            row_mapping[assay_id] = row.to_dict()
        else:
            logger.warning(f"Skipping empty assay_chembl_id at row {idx}")
    
    if not valid_assay_ids:
        logger.warning("No valid assay IDs found")
        return pd.DataFrame()
    
    logger.info(f"Fetching {len(valid_assay_ids)} assays using streaming batches...")

    # Streaming batches for better memory usage
    try:
        batch_size = getattr(config.runtime, "batch_size", 100) or 100
        total_rows = 0
        batches: list[pd.DataFrame] = []
        batch_index = 0

        for requested_ids, results_batch in client.fetch_assays_batch_streaming(valid_assay_ids, batch_size):
            batch_index += 1
            # Map fetched results by assay_id
            fetched_data: dict[str, dict[str, Any]] = {}
            for result in results_batch:
                assay_id = result.get("assay_chembl_id")
                if assay_id:
                    fetched_data[assay_id] = result

            # Build enriched rows for this batch
            enriched_rows: list[dict[str, Any]] = []
            # Важно сохранять порядок исходного батча и добавлять строки даже при отсутствии ответа
            for assay_id in requested_ids:
                row_data = row_mapping[assay_id].copy()

                # Ensure all expected columns exist
                for key, default_value in default_columns.items():
                    if key not in row_data:
                        row_data[key] = default_value

                fetched = fetched_data.get(assay_id, {})
                fetched.pop("source", None)
                for key, value in fetched.items():
                    if value is not None:
                        row_data[key] = value

                enriched_rows.append(row_data)

            batch_df = pd.DataFrame(enriched_rows)
            total_rows += len(batch_df)
            logger.info(
                f"Processed batch {batch_index}: size={len(batch_df)}, total_rows={total_rows}, batch_size={batch_size}"
            )

            batches.append(batch_df)

            # Respect global limit if provided
            if getattr(config.runtime, "limit", None) is not None and total_rows >= int(config.runtime.limit):
                logger.info(f"Reached global limit of {config.runtime.limit} assays; stopping batches")
                break

        if not batches:
            logger.info("No assays fetched in streaming mode")
            return pd.DataFrame()

        result_df = pd.concat(batches, ignore_index=True)
        if getattr(config.runtime, "limit", None) is not None and len(result_df) > int(config.runtime.limit):
            result_df = result_df.head(int(config.runtime.limit))
        logger.info(f"Successfully processed {len(result_df)} assays in {batch_index} batches")
        return result_df

    except Exception as exc:
        logger.error(f"Error in streaming batch extraction: {exc}")
        # Fallback to individual requests
        logger.info("Falling back to individual requests...")
        return _extract_assay_data_individual(client, frame, config, default_columns)


def _extract_assay_data_individual(
    client: AssayChEMBLClient, 
    frame: pd.DataFrame, 
    config: AssayConfig,
    default_columns: dict[str, Any]
) -> pd.DataFrame:
    """Fallback method for individual assay requests with small chunking."""
    enriched_rows_chunk: list[dict[str, Any]] = []
    chunks: list[pd.DataFrame] = []
    chunk_size = max(10, min(1000, getattr(config.runtime, "batch_size", 100)))

    for idx, (_, row) in enumerate(frame.iterrows()):
        try:
            row_data = row.to_dict()
            for key, default_value in default_columns.items():
                if key not in row_data:
                    row_data[key] = default_value

            assay_id = str(row.get("assay_chembl_id", "")).strip()
            if assay_id and assay_id != "nan":
                data = client.fetch_by_assay_id(assay_id)
                data.pop("source", None)
                for key, value in data.items():
                    if value is not None:
                        row_data[key] = value

            else:
                logger.warning(f"Skipping empty assay_chembl_id at row {idx}")

            enriched_rows_chunk.append(row_data)

            if len(enriched_rows_chunk) >= chunk_size:
                chunks.append(pd.DataFrame(enriched_rows_chunk))
                enriched_rows_chunk = []

        except Exception as exc:
            assay_id_dbg = row.get('assay_chembl_id', 'unknown')
            logger.error(f"Error extracting assay data for {assay_id_dbg}: {exc}")
            error_row = row.to_dict()
            error_row.update(default_columns)
            enriched_rows_chunk.append(error_row)

            if len(enriched_rows_chunk) >= chunk_size:
                chunks.append(pd.DataFrame(enriched_rows_chunk))
                enriched_rows_chunk = []

    if enriched_rows_chunk:
        chunks.append(pd.DataFrame(enriched_rows_chunk))

    if not chunks:
        return pd.DataFrame()

    return pd.concat(chunks, ignore_index=True)


def _extract_assay_data_by_target(
    client: AssayChEMBLClient, 
    target_chembl_id: str,
    filters: dict[str, Any] | None,
    config: AssayConfig
) -> pd.DataFrame:
    """Extract assay data by target ChEMBL ID."""
    try:
        logger.info(f"Extracting assays for target: {target_chembl_id}")
        assays_data = client.fetch_by_target_id(target_chembl_id, filters)
        
        if not assays_data:
            logger.warning(f"No assays found for target: {target_chembl_id}")
            return pd.DataFrame()
        
        # Convert list of dicts to DataFrame
        enriched_frame = pd.DataFrame(assays_data)
        logger.info(f"Successfully extracted {len(enriched_frame)} assays for target {target_chembl_id}")
        
        return enriched_frame
        
    except Exception as exc:
        logger.error(f"Error extracting assays for target {target_chembl_id}: {exc}")
        logger.error(f"Error type: {type(exc).__name__}")
        raise AssayHTTPError(f"Failed to extract assays for target {target_chembl_id}: {exc}") from exc


def _enrich_with_source_data(client: AssayChEMBLClient, assays_df: pd.DataFrame) -> pd.DataFrame:
    """Enrich assay data with source information."""
    enriched = assays_df.copy()
    
    # Get unique src_id values
    unique_src_ids = assays_df["src_id"].dropna().unique()
    
    if len(unique_src_ids) == 0:
        logger.warning("No source IDs found in assay data")
        # Add src_name column with None values
        enriched["src_name"] = None
        return enriched
    
    # Cache source data
    source_cache = {}
    for src_id in unique_src_ids:
        try:
            source_data = client.fetch_source_info(int(src_id))
            source_cache[src_id] = source_data
        except Exception as e:
            logger.warning(f"Failed to fetch source {src_id}: {e}")
            source_cache[src_id] = {
                "src_id": src_id,
                "src_name": None,
                "src_short_name": None,
                "src_url": None
            }
    
    # Enrich data
    enriched["src_name"] = enriched["src_id"].map(
        lambda x: source_cache.get(x, {}).get("src_name") if pd.notna(x) else None
    )
    
    return enriched


def _normalize_assay_fields(assays_df: pd.DataFrame) -> pd.DataFrame:
    """Normalize assay fields."""
    normalized = assays_df.copy()
    
    # String fields: strip, collapse spaces
    string_columns = [
        "assay_chembl_id", "src_assay_id", "assay_type_description",
        "bao_format", "bao_label", "description", "assay_format",
        "assay_organism", "assay_cell_type", "assay_tissue",
        "assay_strain", "assay_subcellular_fraction", "target_chembl_id"
    ]
    
    for col in string_columns:
        if col in normalized.columns:
            normalized[col] = normalized[col].astype(str).str.strip()
            # Replace string representations of empty values with pd.NA
            normalized[col] = normalized[col].replace(["None", "nan", "NaN", "none", "NULL", "null"], pd.NA)
            normalized[col] = normalized[col].replace("", pd.NA)
    
    # Mapping assay_type to description
    assay_type_mapping = {
        "B": "Binding",
        "F": "Functional", 
        "P": "Physicochemical",
        "U": "Unclassified"
    }
    
    if "assay_type" in normalized.columns:
        normalized["assay_type_description"] = normalized["assay_type"].map(
            assay_type_mapping
        ).fillna(normalized["assay_type_description"])
    
    # Normalize list fields
    list_columns = ["assay_category", "assay_classifications"]
    for col in list_columns:
        if col in normalized.columns:
            normalized[col] = normalized[col].apply(_normalize_list_field)
    
    # Validate relationship_type
    valid_relationship_types = {"D", "E", "H", "U", "X", "Y", "Z"}
    if "relationship_type" in normalized.columns:
        invalid_rels = ~normalized["relationship_type"].isin(valid_relationship_types)
        if invalid_rels.any():
            logger.warning(f"Found invalid relationship_type values: {normalized.loc[invalid_rels, 'relationship_type'].unique()}")
    
    # Validate confidence_score
    if "confidence_score" in normalized.columns:
        invalid_conf = (normalized["confidence_score"] < 0) | (normalized["confidence_score"] > 9)
        if invalid_conf.any():
            logger.warning(f"Found invalid confidence_score values: {normalized.loc[invalid_conf, 'confidence_score'].unique()}")
    
    # Limit description length
    if "description" in normalized.columns:
        max_desc_length = 1000
        normalized["description"] = normalized["description"].str[:max_desc_length]
    
    # Rename description to assay_description for consistency with config
    if "description" in normalized.columns and "assay_description" not in normalized.columns:
        normalized["assay_description"] = normalized["description"]
    
    # Normalize variant_sequence pattern
    if "variant_sequence" in normalized.columns:
        normalized["variant_sequence"] = normalized["variant_sequence"].astype(str).str.upper()
        # Replace string representations of empty values with pd.NA
        normalized["variant_sequence"] = normalized["variant_sequence"].replace(["None", "NONE", "nan", "NaN", "none", "NULL", "null", "NIL"], pd.NA)
        normalized["variant_sequence"] = normalized["variant_sequence"].replace("", pd.NA)
    
    # Validate BAO patterns
    for bao_field in ["assay_class_bao_id", "bao_endpoint"]:
        if bao_field in normalized.columns:
            # Log invalid BAO IDs but don't fail
            invalid_mask = normalized[bao_field].notna() & ~normalized[bao_field].astype(str).str.match(r"^BAO_\d{7}$", na=False)
            if invalid_mask.any():
                logger.warning(f"Found {invalid_mask.sum()} invalid {bao_field} values")
    
    return normalized


def _normalize_list_field(value) -> list[str] | None:
    """Normalize list field."""
    if value is None or (hasattr(value, '__len__') and len(value) == 0) or (not hasattr(value, '__len__') and pd.isna(value)):
        return None
    
    if isinstance(value, list):
        # Deduplication and sorting for determinism
        unique_items = list(set(str(item).strip() for item in value if str(item).strip()))
        return sorted(unique_items) if unique_items else None
    
    if isinstance(value, str):
        # Parse string as JSON or split by delimiters
        try:
            import json
            parsed = json.loads(value)
            if isinstance(parsed, list):
                unique_items = list(set(str(item).strip() for item in parsed if str(item).strip()))
                return sorted(unique_items) if unique_items else None
        except (json.JSONDecodeError, TypeError):
            pass
        
        # Split by commas or semicolons
        items = [item.strip() for item in value.replace(";", ",").split(",") if item.strip()]
        unique_items = list(set(items))
        return sorted(unique_items) if unique_items else None
    
    return None


def _validate_assay_schema(assays_df: pd.DataFrame) -> pd.DataFrame:
    """Validate assay schema using Pandera."""
    try:
        # Convert extracted_at to datetime
        if "extracted_at" in assays_df.columns:
            assays_df["extracted_at"] = pd.to_datetime(assays_df["extracted_at"])
        
        # Add chembl_release if not present
        if "chembl_release" not in assays_df.columns:
            assays_df["chembl_release"] = "unknown"
        
        # Fill None values for required non-nullable fields
        required_fields = {
            "source_system": "ChEMBL",
            "extracted_at": pd.Timestamp.now().isoformat(),
            "chembl_release": "unknown",
            "hash_row": "unknown",
            "hash_business_key": "unknown"
        }
        
        for field, default_value in required_fields.items():
            if field in assays_df.columns:
                null_count = assays_df[field].isnull().sum()
                if null_count > 0:
                    logger.warning(f"Filling {null_count} null values in {field} with default: {default_value}")
                    assays_df[field] = assays_df[field].fillna(default_value)
        
        # Enable schema validation
        validated_df = AssayNormalizedSchema.validate(assays_df)
        logger.info(f"Schema validation passed for {len(validated_df)} assays")
        return validated_df
    except Exception as e:
        logger.error(f"Schema validation failed: {e}")
        raise AssayValidationError(f"Schema validation failed: {e}") from e


def _calculate_checksum(file_path: Path) -> str:
    """Calculate SHA256 checksum of a file."""
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            sha256_hash.update(chunk)
    return sha256_hash.hexdigest()


def run_assay_etl(
    config: AssayConfig, 
    assay_ids: list[str] | None = None,
    target_chembl_id: str | None = None,
    filters: dict[str, Any] | None = None
) -> AssayETLResult:
    """Execute the assay ETL pipeline returning enriched artefacts."""

    # S01: Get ChEMBL status and release
    logger.info("S01: Getting ChEMBL status and release...")
    client = _create_api_client(config)
    try:
        status_info = client.get_chembl_status()
        chembl_release = status_info.get("chembl_release", "unknown")
        if chembl_release is None:
            chembl_release = "unknown"
        logger.info(f"ChEMBL release: {chembl_release}")
    except Exception as e:
        logger.warning(f"Failed to get ChEMBL status: {e}")
        chembl_release = "unknown"

    # S02: Fetch assay core data
    logger.info("S02: Fetching assay core data...")
    if assay_ids:
        # Create DataFrame from assay IDs
        input_frame = pd.DataFrame({"assay_chembl_id": assay_ids})
        input_frame = _normalise_columns(input_frame)
        
        if config.runtime.limit is not None:
            input_frame = input_frame.head(config.runtime.limit)
        
        # Check for duplicates
        duplicates = input_frame["assay_chembl_id"].duplicated()
        if bool(duplicates.any()):
            raise AssayQCError("Duplicate assay_chembl_id values detected")
        
        # Log streaming batch params
        try:
            batch_size = getattr(config.runtime, "batch_size", 100)
            limit = getattr(config.runtime, "limit", None)
            total_ids = int(len(input_frame))
            logger.info(f"Assay extraction: streaming batches enabled (batch_size={batch_size}, limit={limit}, total_ids={total_ids})")
        except Exception as e:
            logger.warning(f"Failed to log batch parameters: {e}")

        # Extract data
        enriched_frame = _extract_assay_data(client, input_frame, config)
        
    elif target_chembl_id:
        # Extract by target
        enriched_frame = _extract_assay_data_by_target(client, target_chembl_id, filters, config)
        
        if config.runtime.limit is not None:
            enriched_frame = enriched_frame.head(config.runtime.limit)
    else:
        raise AssayValidationError("Either assay_ids or target_chembl_id must be provided")

    if enriched_frame.empty:
        logger.warning("No assay data extracted")
        return AssayETLResult(
            assays=pd.DataFrame(),
            qc=pd.DataFrame([{"metric": "row_count", "value": 0}]),
            meta={"chembl_release": chembl_release, "row_count": 0}
        )

    # S03: Enrich with source data
    logger.info("S03: Enriching with source data...")
    enriched_frame = _enrich_with_source_data(client, enriched_frame)

    # S04: Normalize fields
    logger.info("S04: Normalizing fields...")
    enriched_frame = _normalize_assay_fields(enriched_frame)

    # Add chembl_release to all records
    enriched_frame["chembl_release"] = chembl_release

    # Generate hashes for all records
    enriched_frame["hash_business_key"] = enriched_frame["assay_chembl_id"].apply(
        lambda x: hashlib.sha256(str(x).encode()).hexdigest() if pd.notna(x) else "unknown"
    )
    enriched_frame["hash_row"] = enriched_frame.apply(
        lambda row: hashlib.sha256(str(row.to_dict()).encode()).hexdigest(), axis=1
    )

    # S05: Validate schema
    logger.info("S05: Validating schema...")
    validated_frame = _validate_assay_schema(enriched_frame)

    # Calculate QC metrics
    qc_metrics = [
        {"metric": "row_count", "value": int(len(validated_frame))},
        {"metric": "unique_sources", "value": int(validated_frame["src_id"].nunique())},
        {"metric": "assay_types", "value": validated_frame["assay_type"].value_counts().to_dict()},
        {"metric": "relationship_types", "value": validated_frame["relationship_type"].value_counts().to_dict()},
        {"metric": "confidence_scores", "value": validated_frame["confidence_score"].value_counts().to_dict()},
    ]
    
    qc = pd.DataFrame(qc_metrics)
    # Унифицируем базовые QC метрики
    try:
        from library.etl.qc_common import ensure_common_qc
        qc = ensure_common_qc(validated_frame, qc, module_name="assay")
    except Exception as exc:
        import logging as _logging
        _logging.getLogger(__name__).warning(f"Failed to normalize QC metrics for assay: {exc}")

    # Create metadata
    meta = {
        "pipeline_version": "1.0.0",
        "chembl_release": chembl_release,
        "row_count": len(validated_frame),
        "extraction_parameters": {
            "total_assays": len(validated_frame),
            "unique_sources": validated_frame["src_id"].nunique(),
            "assay_types": validated_frame["assay_type"].value_counts().to_dict(),
            "relationship_types": validated_frame["relationship_type"].value_counts().to_dict()
        }
    }

    # Perform correlation analysis if enabled in config
    correlation_analysis = None
    correlation_reports = None
    correlation_insights = None
    
    if config.postprocess.correlation.enabled and len(validated_frame) > 1:
        try:
            logger.info("Performing correlation analysis...")
            
            # Подготавливаем данные для корреляционного анализа
            analysis_df = prepare_data_for_correlation_analysis(
                validated_frame, 
                data_type="assays", 
                logger=logger
            )
            
            logger.info(f"Correlation analysis: {len(analysis_df.columns)} numeric columns, {len(analysis_df)} rows")
            logger.info(f"Columns for analysis: {list(analysis_df.columns)}")
            
            if len(analysis_df.columns) > 1:
                # Perform correlation analysis
                correlation_analysis = build_enhanced_correlation_analysis(analysis_df)
                correlation_reports = build_enhanced_correlation_reports(analysis_df)
                correlation_insights = build_correlation_insights(analysis_df)
                
                logger.info(f"Correlation analysis completed. Found {len(correlation_insights)} insights.")
            else:
                logger.warning("Not enough numeric columns for correlation analysis")
                
        except Exception as exc:
            logger.warning(f"Error during correlation analysis: {exc}")
            logger.warning(f"Error type: {type(exc).__name__}")
            # Continue without correlation analysis

    return AssayETLResult(
        assays=validated_frame, 
        qc=qc,
        meta=meta,
        correlation_analysis=correlation_analysis,
        correlation_reports=correlation_reports,
        correlation_insights=correlation_insights
    )


def write_assay_outputs(
    result: AssayETLResult, output_dir: Path, date_tag: str, config: AssayConfig
) -> dict[str, Path]:
    """Persist ETL artefacts to disk and return the generated paths."""

    try:
        output_dir.mkdir(parents=True, exist_ok=True)
    except OSError as exc:  # pragma: no cover - filesystem permission issues
        raise AssayIOError(f"Failed to create output directory: {exc}") from exc

    # File paths
    csv_path = output_dir / f"assay_{date_tag}.csv"    
    qc_path = output_dir / f"assay_{date_tag}_qc.csv"
    meta_path = output_dir / f"assay_{date_tag}_meta.yaml"

    try:
        # S06: Persist data with deterministic serialization
        logger.info("S06: Persisting data...")
        
        # Save CSV with deterministic order
        write_deterministic_csv(
            result.assays,
            csv_path,
            determinism=config.determinism,
            output=config.io.output
        )
        
 
        # Save QC data (всегда создаём файл)
        if isinstance(result.qc, pd.DataFrame) and not result.qc.empty:
            result.qc.to_csv(qc_path, index=False)
        else:
            import pandas as _pd
            _pd.DataFrame([{"metric": "row_count", "value": int(len(result.assays))}]).to_csv(qc_path, index=False)
        
        # Save metadata
        with open(meta_path, 'w', encoding='utf-8') as f:
            yaml.dump(result.meta, f, default_flow_style=False, allow_unicode=True)
        
        # Save correlation reports if available
        correlation_paths = {}
        if result.correlation_reports:
            try:
                correlation_dir = output_dir / f"assay_correlation_report_{date_tag}"
                correlation_dir.mkdir(exist_ok=True)
                
                for report_name, report_df in result.correlation_reports.items():
                    report_path = correlation_dir / f"{report_name}.csv"
                    report_df.to_csv(report_path, index=False)
                    correlation_paths[report_name] = report_path
                    
                logger.info(f"Saved {len(correlation_paths)} correlation reports to {correlation_dir}")
                
            except Exception as exc:
                logger.warning(f"Failed to save correlation reports: {exc}")
        
        # Save correlation insights if available
        try:
            if result.correlation_insights:
                insights_dir = output_dir / f"assay_correlation_report_{date_tag}"
                insights_dir.mkdir(exist_ok=True)
                insights_path = insights_dir / "correlation_insights.json"
                import json as _json
                with insights_path.open("w", encoding="utf-8") as f:
                    _json.dump(result.correlation_insights, f, ensure_ascii=False, indent=2)
                correlation_paths["correlation_insights"] = insights_path
        except Exception:
            logger.warning("Failed to save correlation insights")

        # Add file checksums to metadata
        result.meta["file_checksums"] = {
            "csv": _calculate_checksum(csv_path),
            "qc": _calculate_checksum(qc_path),
        }
        
        # Update metadata file with checksums
        with open(meta_path, 'w', encoding='utf-8') as f:
            yaml.dump(result.meta, f, default_flow_style=False, allow_unicode=True)
        
    except OSError as exc:  # pragma: no cover - filesystem permission issues
        raise AssayIOError(f"Failed to write outputs: {exc}") from exc

    result_paths = {
        "csv": csv_path, 
        "qc": qc_path,
        "meta": meta_path
    }
    
    # Add correlation report paths if available
    if correlation_paths:
        result_paths["correlation_reports"] = correlation_paths
    
    return result_paths


__all__ = [
    "AssayETLResult",
    "AssayHTTPError",
    "AssayIOError",
    "AssayPipelineError",
    "AssayQCError",
    "AssayValidationError",
    "run_assay_etl",
    "write_assay_outputs",
]
