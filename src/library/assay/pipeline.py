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


<<<<<<< Updated upstream
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
        "chembl_release": None
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
=======
class AssayPipeline(PipelineBase[AssayConfig]):
    """Assay ETL pipeline using unified PipelineBase."""

    def __init__(self, config: AssayConfig) -> None:
        """Initialize assay pipeline with configuration."""
        super().__init__(config)

    def _setup_clients(self) -> None:
        """Initialize HTTP clients for assay sources."""
        self.clients = {}

        # ChEMBL client
        if "chembl" in self.config.sources and self.config.sources["chembl"].enabled:
            self.clients["chembl"] = self._create_chembl_client()

    def _create_chembl_client(self) -> ChEMBLClient:
        """Create ChEMBL client."""
        from library.settings import APIClientConfig, RateLimitSettings, RetrySettings

        source_config = self.config.sources["chembl"]
        timeout = source_config.http.timeout_sec or self.config.http.global_.timeout_sec
        timeout = max(timeout, 60.0)  # At least 60 seconds for ChEMBL

        headers = self._get_headers("chembl")
        headers.update(self.config.http.global_.headers)
        headers.update(source_config.http.headers)

        processed_headers = self._process_headers(headers)

        client_config = APIClientConfig(
            name="chembl",
            base_url=source_config.http.base_url or "https://www.ebi.ac.uk/chembl/api/data",
            timeout_sec=timeout,
            retries=RetrySettings(
                total=self.config.http.global_.retries.total,
                backoff_multiplier=self.config.http.global_.retries.backoff_multiplier,
                backoff_max=60.0,  # Default backoff max
            ),
            rate_limit=RateLimitSettings(
                max_calls=self.config.http.global_.rate_limit.get("max_calls", 10),
                period=self.config.http.global_.rate_limit.get("period", 1.0),
            ),
            headers=processed_headers,
            verify_ssl=True,
            follow_redirects=True,
        )

        # Create client with caching and fallback options
        return ChEMBLClient(
            api=client_config,
            retry=client_config.retries,
            chembl=client_config,
        )

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

    def extract(self, input_data: pd.DataFrame) -> pd.DataFrame:
        """Extract assay data from ChEMBL."""
        logger.info(f"Extracting assay data for {len(input_data)} assays")

        # Validate input data (placeholder)
        validated_data = input_data

        # Apply limit if specified
        if self.config.runtime.limit is not None:
            validated_data = validated_data.head(self.config.runtime.limit)

        # Check for duplicates
        duplicates = validated_data["assay_chembl_id"].duplicated()
        if duplicates.any():
            raise ValueError("Duplicate assay_chembl_id values detected")

        # Extract data from ChEMBL
        if "chembl" in self.clients:
            try:
                logger.info("Extracting data from ChEMBL")
                chembl_data = self._extract_from_chembl(validated_data)
                extracted_data = self._merge_chembl_data(validated_data, chembl_data)
            except Exception as e:
                logger.error(f"Failed to extract from ChEMBL: {e}")
                if not self.config.runtime.allow_incomplete_sources:
                    raise
        else:
            extracted_data = validated_data

        logger.info(f"Extracted data for {len(extracted_data)} assays")
        return extracted_data

    def _extract_from_chembl(self, data: pd.DataFrame) -> pd.DataFrame:
        """Extract data from ChEMBL with target enrichment."""
        logger.info(f"Extracting assay data from ChEMBL for {len(data)} assays")

        extracted_records = []
        chembl_client = self.clients["chembl"]

        # Track missing fields across all assays
        missing_fields = set()
        empty_fields = set()
        error_count = 0
        success_count = 0
        fallback_count = 0

        # Collect unique target IDs and assay class IDs for enrichment
        target_ids = set()
        assay_class_ids = set()

        # Get batch size from config (default to 25 for ChEMBL API limit)
        batch_size = 25  # TODO: Get from config when batch_size field is added to AssaySourceSettings

        # Extract assay IDs
        assay_ids = data["assay_chembl_id"].tolist()
        logger.info(f"Processing {len(assay_ids)} assays in batches of {batch_size}")

        # Process in batches
        all_assay_data = {}
        for i in range(0, len(assay_ids), batch_size):
            batch_ids = assay_ids[i : i + batch_size]
            logger.info(f"Processing batch {i // batch_size + 1}: {len(batch_ids)} assays")

            try:
                batch_data = chembl_client.fetch_assays_batch(batch_ids)
                all_assay_data.update(batch_data)
                logger.info(f"Successfully fetched {len(batch_data)} assays from batch")
            except Exception as e:
                logger.error(f"Failed to fetch batch {i // batch_size + 1}: {e}")
                # Continue with next batch
                continue

        # Process fetched data
        for assay_id in assay_ids:
            assay_data = all_assay_data.get(assay_id)
            if assay_data and "error" not in assay_data:
                # Check if this is fallback data
                if assay_data.get("source_system") == "ChEMBL_FALLBACK":
                    fallback_count += 1
                    logger.debug(f"Using fallback data for assay {assay_id}")
                else:
                    success_count += 1

                # Expand assay parameters
                assay_data = self._expand_assay_parameters(assay_data)

                # Expand variant sequence
                assay_data = self._expand_variant_sequence(assay_data)

                extracted_records.append(assay_data)

                # Collect target_chembl_id for enrichment
                target_chembl_id = assay_data.get("target_chembl_id")
                if target_chembl_id:
                    target_ids.add(target_chembl_id)

                # Collect assay_class_ids from assay_classifications
                classifications = assay_data.get("assay_classifications")
                if classifications:
                    try:
                        import json

                        class_data = json.loads(classifications)
                        if isinstance(class_data, list):
                            for class_item in class_data:
                                if isinstance(class_item, dict) and "assay_class_id" in class_item:
                                    assay_class_ids.add(class_item["assay_class_id"])
                    except (json.JSONDecodeError, TypeError) as e:
                        logger.debug(f"Failed to parse assay_classifications for {assay_id}: {e}")

                # Track which fields are missing or empty
                for field, value in assay_data.items():
                    if field not in missing_fields and field not in empty_fields:
                        if value is None or value == "" or (isinstance(value, str) and value.strip() == ""):
                            empty_fields.add(field)
            else:
                error_count += 1
                error_msg = assay_data.get("error", "No data returned") if assay_data else "Not found in batch"
                logger.warning(f"Failed to fetch assay {assay_id}: {error_msg}")
                # Add error record
                extracted_records.append({"assay_chembl_id": assay_id, "source_system": "ChEMBL", "extracted_at": datetime.utcnow().isoformat() + "Z", "error": error_msg})

        if extracted_records:
            extracted_df = pd.DataFrame(extracted_records)

            # Log extraction statistics
            total_assays = len(assay_ids)
            logger.info("Assay extraction summary:")
            logger.info(f"  Total assays: {total_assays}")
            logger.info(f"  Successfully extracted: {success_count}")
            logger.info(f"  Using fallback data: {fallback_count}")
            logger.info(f"  Errors: {error_count}")
            logger.info(f"  Success rate: {(success_count + fallback_count) / total_assays * 100:.1f}%")

            if fallback_count > 0:
                logger.warning(f"Used fallback data for {fallback_count} assays due to API issues")

            if error_count > 0:
                logger.warning(f"Failed to extract {error_count} assays")

            logger.info(f"Successfully extracted {len(extracted_df)} assay records from ChEMBL")

            # Ensure all expected fields are present
            extracted_df = self._ensure_all_fields_present(extracted_df)

            # Enrich with target data
            if target_ids:
                logger.info(f"Enriching {len(target_ids)} unique targets from /target endpoint")
                target_data = self._enrich_with_target_data(chembl_client, list(target_ids))
                if not target_data.empty:
                    # Merge target data into assay data
                    extracted_df = extracted_df.merge(target_data, on="target_chembl_id", how="left", suffixes=("", "_target"))
                    logger.info("Enriched assay data with target information")

            # Enrich with assay class data
            if assay_class_ids:
                logger.info(f"Enriching {len(assay_class_ids)} unique assay classes from /assay_class endpoint")
                class_data = self._enrich_with_assay_classes(chembl_client, list(assay_class_ids))
                if not class_data.empty:
                    # Merge class data into assay data
                    # Note: We need to handle the many-to-many relationship between assays and classes
                    # For now, we'll merge on the first assay_class_id found in classifications
                    extracted_df = self._merge_assay_class_data(extracted_df, class_data)
                    logger.info("Enriched assay data with assay class information")

            # Log field analysis
            if empty_fields:
                logger.warning(f"Empty/missing fields in assay data: {sorted(empty_fields)}")
                logger.info("This may indicate incomplete data in ChEMBL or limited API response.")

            # Log information about unavailable fields
            unavailable_fields = [
                "bao_endpoint",
                "bao_assay_format",
                "bao_assay_type",
                "bao_assay_type_label",
                "bao_assay_type_uri",
                "bao_assay_format_uri",
                "bao_assay_format_label",
                "bao_endpoint_uri",
                "bao_endpoint_label",
                "variant_id",
                "is_variant",
                "variant_accession",
                "variant_sequence_accession",
                "variant_sequence_mutation",
                "variant_mutations",
                "variant_text",
                "variant_sequence_id",
                "variant_organism",
                "assay_parameters_json",
                "assay_format",
            ]
            logger.info(f"Fields unavailable in ChEMBL API: {unavailable_fields}")
            logger.info("These fields are documented as unavailable in ChEMBL API v33+")

            # Final check: ensure all fields are present after all enrichments
            extracted_df = self._ensure_all_fields_present(extracted_df)

            return extracted_df
        else:
            logger.warning("No assay data extracted from ChEMBL")
            return pd.DataFrame()

    def _enrich_with_target_data(self, chembl_client, target_ids: list[str]) -> pd.DataFrame:
        """Enrich assay data with target information."""
        target_records = []

        for target_id in target_ids:
            try:
                target_data = chembl_client.fetch_by_target_id(target_id)
                if target_data and "error" not in target_data:
                    target_records.append(target_data)
            except Exception as e:
                logger.warning("Failed to fetch target %s: %s", target_id, e)
                continue

        if target_records:
            return pd.DataFrame(target_records)
        else:
            logger.warning("No target data extracted for enrichment")
            return pd.DataFrame()

    def _expand_assay_parameters(self, assay_data: dict[str, Any]) -> dict[str, Any]:
        """Expand first assay_parameter to flat fields with assay_param_ prefix."""
        params = assay_data.get("assay_parameters")
        if params and isinstance(params, list) and len(params) > 0:
            first_param = params[0]
            assay_data["assay_param_type"] = first_param.get("type")
            assay_data["assay_param_relation"] = first_param.get("relation")
            assay_data["assay_param_value"] = first_param.get("value")
            assay_data["assay_param_units"] = first_param.get("units")
            assay_data["assay_param_text_value"] = first_param.get("text_value")
            assay_data["assay_param_standard_type"] = first_param.get("standard_type")
            assay_data["assay_param_standard_value"] = first_param.get("standard_value")
            assay_data["assay_param_standard_units"] = first_param.get("standard_units")
        else:
            # Set NULL for all fields
            for field in ["type", "relation", "value", "units", "text_value", "standard_type", "standard_value", "standard_units"]:
                assay_data[f"assay_param_{field}"] = None
        return assay_data

    def _expand_variant_sequence(self, assay_data: dict[str, Any]) -> dict[str, Any]:
        """Expand variant_sequence object to flat fields with variant_ prefix."""
        variant = assay_data.get("variant_sequence")
        if variant and isinstance(variant, dict):
            assay_data["variant_id"] = variant.get("variant_id")
            assay_data["variant_base_accession"] = variant.get("accession") or variant.get("base_accession")
            assay_data["variant_mutation"] = variant.get("mutation")
            assay_data["variant_sequence"] = variant.get("sequence")
            assay_data["variant_accession_reported"] = variant.get("accession")
        else:
            for field in ["variant_id", "variant_base_accession", "variant_mutation", "variant_sequence", "variant_accession_reported"]:
                assay_data[field] = None
        return assay_data

    def _enrich_with_assay_classes(self, chembl_client, assay_class_ids: list[int]) -> pd.DataFrame:
        """Fetch assay class data for given IDs."""
        class_records = []
        for class_id in assay_class_ids:
            try:
                class_data = chembl_client.fetch_assay_class(class_id)
                if class_data and "error" not in class_data:
                    class_records.append(class_data)
            except Exception as e:
                logger.warning(f"Failed to fetch assay_class {class_id}: {e}")
        return pd.DataFrame(class_records) if class_records else pd.DataFrame()

    def _ensure_all_fields_present(self, df: pd.DataFrame) -> pd.DataFrame:
        """Ensure all expected fields are present in DataFrame with None defaults."""
        expected_fields = {
            # ASSAY_PARAMETERS
            "assay_param_type": None,
            "assay_param_relation": None,
            "assay_param_value": None,
            "assay_param_units": None,
            "assay_param_text_value": None,
            "assay_param_standard_type": None,
            "assay_param_standard_value": None,
            "assay_param_standard_units": None,
            # ASSAY_CLASS
            "assay_class_id": None,
            "assay_class_bao_id": None,
            "assay_class_type": None,
            "assay_class_l1": None,
            "assay_class_l2": None,
            "assay_class_l3": None,
            "assay_class_description": None,
            # VARIANT_SEQUENCES
            "variant_id": None,
            "variant_base_accession": None,
            "variant_mutation": None,
            "variant_sequence": None,
            "variant_accession_reported": None,
        }

        missing_fields = []
        for field, default_value in expected_fields.items():
            if field not in df.columns:
                df[field] = default_value
                missing_fields.append(field)

        if missing_fields:
            logger.debug(f"Initialized {len(missing_fields)} missing fields: {missing_fields}")

        return df

    def _merge_assay_class_data(self, assay_df: pd.DataFrame, class_df: pd.DataFrame) -> pd.DataFrame:
        """Merge assay class data into assay DataFrame using vectorized operations."""
        import json

        # Инициализировать все assay_class колонки как None
        class_columns = ["assay_class_id", "assay_class_bao_id", "assay_class_type", "assay_class_l1", "assay_class_l2", "assay_class_l3", "assay_class_description"]

        for col in class_columns:
            if col not in assay_df.columns:
                assay_df[col] = None

        if class_df.empty:
            return assay_df

        # Извлечь assay_class_id из assay_classifications для каждого ассея
        def extract_first_class_id(classifications_json):
            """Extract first assay_class_id from classifications JSON."""
            if not classifications_json:
                return None
            try:
                class_data = json.loads(classifications_json)
                if isinstance(class_data, list) and len(class_data) > 0:
                    first_class = class_data[0]
                    if isinstance(first_class, dict):
                        return first_class.get("assay_class_id")
            except (json.JSONDecodeError, TypeError, KeyError):
                return None
            return None

        # Создать временную колонку с assay_class_id для JOIN
        assay_df["_temp_class_id"] = assay_df["assay_classifications"].apply(extract_first_class_id)

        # Merge с class_df
        result = assay_df.merge(class_df, left_on="_temp_class_id", right_on="assay_class_id", how="left", suffixes=("", "_from_class"))

        # Удалить временную колонку и дубликаты
        result = result.drop(columns=["_temp_class_id"], errors="ignore")

        # Если есть дубликаты колонок с суффиксом _from_class, удалить их
        for col in result.columns:
            if col.endswith("_from_class"):
                result = result.drop(columns=[col])

        return result

    def _merge_chembl_data(self, base_data: pd.DataFrame, chembl_data: pd.DataFrame) -> pd.DataFrame:
        """Merge ChEMBL data into base data."""
        if chembl_data.empty:
            logger.warning("No ChEMBL data to merge")
            return base_data

        # Merge on assay_chembl_id
        merged_data = base_data.merge(chembl_data, on="assay_chembl_id", how="left", suffixes=("", "_chembl"))

        logger.info(f"Merged ChEMBL data: {len(merged_data)} records")
        return merged_data

    def normalize(self, raw_data: pd.DataFrame) -> pd.DataFrame:
        """Normalize assay data."""
        logger.info("Normalizing assay data")

        # Apply assay-specific normalization
        # TODO: Implement assay-specific normalization or import from correct module
        # from library.normalize.assay import normalize_assay_dataframe
        # normalized_data = normalize_assay_dataframe(raw_data)
        
        # For now, just return the raw data without normalization
        normalized_data = raw_data.copy()

        # Add system metadata fields
        from library.common.metadata_fields import (
            add_system_metadata_fields,
        )
        from library.clients.factory import create_api_client

        # Создаем ChEMBL клиент для получения версии
        config_dict = self.config.model_dump() if hasattr(self.config, "model_dump") else {}
        chembl_client = create_api_client("chembl", self.config, "assay")

        # Добавляем системные метаданные
        normalized_data = add_system_metadata_fields(normalized_data, config_dict, chembl_client)

        logger.info(f"Normalized {len(normalized_data)} assays")
        return normalized_data

    def validate(self, data: pd.DataFrame) -> pd.DataFrame:
        """Validate assay data."""
        logger.info("Validating assay data")

        # Validate normalized data (placeholder)
        validated_data = data

        logger.info(f"Validated {len(validated_data)} assays")
        return validated_data

    def filter_quality(self, data: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Filter assays by quality."""
        logger.info("Filtering assays by quality")

        # Apply quality filters (placeholder - accept all data)
        accepted_data = data
        rejected_data = pd.DataFrame()

        logger.info(f"Quality filtering: {len(accepted_data)} accepted, {len(rejected_data)} rejected")
        return accepted_data, rejected_data

    def _get_entity_type(self) -> str:
        """Get entity type for assay pipeline."""
        return "assays"

    def _create_qc_validator(self) -> QCValidator:
        """Create QC validator for assay pipeline."""
        profile = get_qc_profile("assays", "strict")
        return get_qc_validator("assays", profile)

    def _create_postprocessor(self) -> AssayPostprocessor:
        """Create postprocessor for assay pipeline."""
        return AssayPostprocessor(self.config)

    def _create_etl_writer(self) -> ETLWriter:
        """Create ETL writer for assay pipeline."""
        return create_etl_writer(self.config, "assays")

    def _build_metadata(
        self,
        data: pd.DataFrame,
        accepted_data: pd.DataFrame | None = None,
        rejected_data: pd.DataFrame | None = None,
        correlation_analysis: dict[str, Any] | None = None,
        correlation_insights: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Build metadata for assay pipeline."""
        # Use MetadataBuilder to create proper PipelineMetadata
        from library.common.metadata import MetadataBuilder

        metadata_builder = MetadataBuilder(self.config, "assays")

        # Prepare additional metadata
        additional_metadata = {}
        if correlation_analysis is not None:
            additional_metadata["correlation_analysis"] = correlation_analysis
        if correlation_insights is not None:
            additional_metadata["correlation_insights"] = correlation_insights

        # Build proper metadata using MetadataBuilder
        metadata = metadata_builder.build_metadata(
            df=data,
            accepted_df=accepted_data if accepted_data is not None else data,
            rejected_df=rejected_data if rejected_data is not None else pd.DataFrame(),
            output_files={},  # Will be filled by writer
            additional_metadata=additional_metadata if additional_metadata else None,
        )

        return metadata
>>>>>>> Stashed changes
