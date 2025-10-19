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
    build_correlation_insights,
    build_enhanced_correlation_analysis,
    build_enhanced_correlation_reports,
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

            processed_value = re.sub(r"\{([^}]+)\}", replace_placeholder, value)
            # Only include header if the value is not empty after processing and not a placeholder
            if processed_value and processed_value.strip() and not processed_value.startswith("{") and not processed_value.endswith("}"):
                processed_headers[key] = processed_value
        else:
            processed_headers[key] = value
    headers = processed_headers

    # Use source-specific base_url or fallback to default
    base_url = chembl_config.http.base_url or "https://www.ebi.ac.uk/chembl/api/data"

    # Use source-specific retry settings or fallback to global
    retry_settings = RetrySettings(
        total=chembl_config.http.retries.get("total", config.http.global_.retries.total),
        backoff_multiplier=chembl_config.http.retries.get("backoff_multiplier", config.http.global_.retries.backoff_multiplier),
    )

    # Create rate limit settings if configured
    rate_limit = None
    if chembl_config.rate_limit:
        # Convert various rate limit formats to max_calls/period
        max_calls = chembl_config.rate_limit.get("max_calls")
        period = chembl_config.rate_limit.get("period")

        # If not in max_calls/period format, try to convert from other formats
        if max_calls is None or period is None:
            requests_per_second = chembl_config.rate_limit.get("requests_per_second")
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
        raise AssayValidationError(f"Input data is missing required columns: {', '.join(sorted(missing))}")

    # Normalize assay_chembl_id
    normalised["assay_chembl_id"] = normalised["assay_chembl_id"].astype(str).str.strip()

    # Normalize target_chembl_id if present
    if "target_chembl_id" in normalised.columns:
        normalised["target_chembl_id"] = normalised["target_chembl_id"].astype(str).str.strip()

    normalised = normalised.sort_values("assay_chembl_id").reset_index(drop=True)
    return normalised


def _extract_assay_data(client: AssayChEMBLClient, frame: pd.DataFrame, config: AssayConfig) -> pd.DataFrame:
    """Extract assay data from ChEMBL API using optimized batch requests."""

    # Define default values for all possible columns to ensure they exist
    default_columns = {
        # Core assay fields
        "assay_chembl_id": None,
        "src_id": None,
        "src_assay_id": None,
        "assay_type": None,
        "assay_type_description": None,
        "bao_format": None,
        "bao_label": None,
        "assay_category": None,
        "assay_classifications": None,
        "target_chembl_id": None,
        "relationship_type": None,
        "confidence_score": None,
        "assay_organism": None,
        "assay_tax_id": None,
        "assay_cell_type": None,
        "assay_tissue": None,
        "assay_strain": None,
        "assay_subcellular_fraction": None,
        "description": None,
        "assay_parameters": None,
        "assay_format": None,
        "source_system": "ChEMBL",
        "extracted_at": None,
        "hash_row": None,
        "hash_business_key": None,
        "chembl_release": None,
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

    logger.info(f"Fetching {len(valid_assay_ids)} assays using batch requests...")

    # Use batch requests for better performance
    try:
        batch_size = 100  # Оптимальный размер батча для ChEMBL API
        batch_results = client.fetch_assays_batch(valid_assay_ids, batch_size)

        # Create a mapping from assay_id to fetched data
        fetched_data = {}
        for result in batch_results:
            assay_id = result.get("assay_chembl_id")
            if assay_id:
                fetched_data[assay_id] = result

        # Build enriched data
        enriched_data = []
        for assay_id in valid_assay_ids:
            row_data = row_mapping[assay_id].copy()

            # Add missing columns with default values
            for key, default_value in default_columns.items():
                if key not in row_data:
                    row_data[key] = default_value

            # Update with fetched data if available
            if assay_id in fetched_data:
                fetched = fetched_data[assay_id]
                # Remove source from data to avoid overwriting
                fetched.pop("source", None)
                # Only update non-None values to preserve existing data
                for key, value in fetched.items():
                    if value is not None:
                        row_data[key] = value

            enriched_data.append(row_data)

        logger.info(f"Successfully processed {len(enriched_data)} assays")
        return pd.DataFrame(enriched_data)

    except Exception as exc:
        logger.error(f"Error in batch extraction: {exc}")
        # Fallback to individual requests
        logger.info("Falling back to individual requests...")
        return _extract_assay_data_individual(client, frame, config, default_columns)


def _extract_assay_data_individual(client: AssayChEMBLClient, frame: pd.DataFrame, config: AssayConfig, default_columns: dict[str, Any]) -> pd.DataFrame:
    """Fallback method for individual assay requests."""
    enriched_data = []

    for idx, (_, row) in enumerate(frame.iterrows()):
        try:
            # Start with the original row data and ensure all columns exist
            row_data = row.to_dict()
            # Only add missing columns with default values, don't overwrite existing data
            for key, default_value in default_columns.items():
                if key not in row_data:
                    row_data[key] = default_value

            assay_id = str(row["assay_chembl_id"]).strip()
            if assay_id and assay_id != "nan":
                data = client.fetch_by_assay_id(assay_id)
                # Remove source from data to avoid overwriting
                data.pop("source", None)
                # Only update non-None values to preserve existing data
                for key, value in data.items():
                    if value is not None:
                        row_data[key] = value

                enriched_data.append(row_data)
            else:
                logger.warning(f"Skipping empty assay_chembl_id at row {idx}")
                enriched_data.append(row_data)

        except Exception as exc:
            # Log error but continue processing other records
            assay_id = row.get("assay_chembl_id", "unknown")
            logger.error(f"Error extracting assay data for {assay_id}: {exc}")

            # Ensure error row also has all columns
            error_row = row.to_dict()
            # Only add missing columns, don't overwrite existing ones
            for key, default_value in default_columns.items():
                if key not in error_row:
                    error_row[key] = default_value
            enriched_data.append(error_row)

    return pd.DataFrame(enriched_data)


def _extract_assay_data_by_target(client: AssayChEMBLClient, target_chembl_id: str, filters: dict[str, Any] | None, config: AssayConfig) -> pd.DataFrame:
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
            source_cache[src_id] = {"src_id": src_id, "src_name": None, "src_short_name": None, "src_url": None}

    # Enrich data
    enriched["src_name"] = enriched["src_id"].map(lambda x: source_cache.get(x, {}).get("src_name") if pd.notna(x) else None)

    return enriched


def _normalize_assay_fields(assays_df: pd.DataFrame) -> pd.DataFrame:
    """Normalize assay fields."""
    normalized = assays_df.copy()

    # String fields: strip, collapse spaces
    string_columns = [
        "assay_chembl_id",
        "src_assay_id",
        "assay_type_description",
        "bao_format",
        "bao_label",
        "description",
        "assay_format",
        "assay_organism",
        "assay_cell_type",
        "assay_tissue",
        "assay_strain",
        "assay_subcellular_fraction",
        "target_chembl_id",
    ]

    for col in string_columns:
        if col in normalized.columns:
            normalized[col] = normalized[col].astype(str).str.strip()
            normalized[col] = normalized[col].replace("", pd.NA)

    # Mapping assay_type to description
    assay_type_mapping = {"B": "Binding", "F": "Functional", "P": "Physicochemical", "U": "Unclassified"}

    if "assay_type" in normalized.columns:
        # Create assay_type_description column if it doesn't exist
        if "assay_type_description" not in normalized.columns:
            normalized["assay_type_description"] = None
        normalized["assay_type_description"] = normalized["assay_type"].map(assay_type_mapping).fillna(normalized["assay_type_description"])

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
    if value is None or (hasattr(value, "__len__") and len(value) == 0) or (not hasattr(value, "__len__") and pd.isna(value)):
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
            "hash_business_key": "unknown",
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


def run_assay_etl(config: AssayConfig, assay_ids: list[str] | None = None, target_chembl_id: str | None = None, filters: dict[str, Any] | None = None) -> AssayETLResult:
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
        return AssayETLResult(assays=pd.DataFrame(), qc=pd.DataFrame([{"metric": "row_count", "value": 0}]), meta={"chembl_release": chembl_release, "row_count": 0})

    # S03: Enrich with source data
    logger.info("S03: Enriching with source data...")
    enriched_frame = _enrich_with_source_data(client, enriched_frame)

    # S04: Normalize fields
    logger.info("S04: Normalizing fields...")
    enriched_frame = _normalize_assay_fields(enriched_frame)

    # Add chembl_release to all records
    enriched_frame["chembl_release"] = chembl_release

    # Generate hashes for all records
    enriched_frame["hash_business_key"] = enriched_frame["assay_chembl_id"].apply(lambda x: hashlib.sha256(str(x).encode()).hexdigest() if pd.notna(x) else "unknown")
    enriched_frame["hash_row"] = enriched_frame.apply(lambda row: hashlib.sha256(str(row.to_dict()).encode()).hexdigest(), axis=1)

    # S05: Validate schema
    logger.info("S05: Validating schema...")
    validated_frame = _validate_assay_schema(enriched_frame)

    # Calculate QC metrics (unified with documents: atomic metrics only)
    qc_metrics = [
        {"metric": "row_count", "value": int(len(validated_frame))},
        {"metric": "enabled_sources", "value": 1},
        {"metric": "chembl_records", "value": int(validated_frame["assay_chembl_id"].notna().sum())},
    ]

    qc = pd.DataFrame(qc_metrics)

    # Create metadata (aligned with documents)
    meta = {
        "pipeline_version": "1.0.0",
        "chembl_release": chembl_release,
        "row_count": len(validated_frame),
        "enabled_sources": ["chembl"],
        "extraction_parameters": {
            "total_assays": len(validated_frame),
            "unique_sources": int(validated_frame["src_id"].nunique()) if "src_id" in validated_frame.columns else 0,
            "assay_types": validated_frame["assay_type"].value_counts().to_dict() if "assay_type" in validated_frame.columns else {},
            "relationship_types": validated_frame["relationship_type"].value_counts().to_dict() if "relationship_type" in validated_frame.columns else {},
            "chembl_records": int(validated_frame["assay_chembl_id"].notna().sum()),
            "correlation_analysis_enabled": False,
            "correlation_insights_count": 0,
        },
    }

    # Perform correlation analysis if enabled in config
    correlation_analysis = None
    correlation_reports = None
    correlation_insights = None

    if config.postprocess.correlation.enabled and len(validated_frame) > 1:
        try:
            logger.info("Performing correlation analysis...")

            # Подготавливаем данные для корреляционного анализа
            analysis_df = prepare_data_for_correlation_analysis(validated_frame, data_type="assays", logger=logger)

            logger.info(f"Correlation analysis: {len(analysis_df.columns)} numeric columns, {len(analysis_df)} rows")
            logger.info(f"Columns for analysis: {list(analysis_df.columns)}")

            if len(analysis_df.columns) > 1:
                # Perform correlation analysis
                correlation_analysis = build_enhanced_correlation_analysis(analysis_df)
                correlation_reports = build_enhanced_correlation_reports(analysis_df)
                correlation_insights = build_correlation_insights(analysis_df)

                logger.info(f"Correlation analysis completed. Found {len(correlation_insights)} insights.")
                # Update metadata flags
                meta["extraction_parameters"]["correlation_analysis_enabled"] = True
                meta["extraction_parameters"]["correlation_insights_count"] = len(correlation_insights)
            else:
                logger.warning("Not enough numeric columns for correlation analysis")

        except Exception as exc:
            logger.warning(f"Error during correlation analysis: {exc}")
            logger.warning(f"Error type: {type(exc).__name__}")
            # Continue without correlation analysis

    return AssayETLResult(
        assays=validated_frame, qc=qc, meta=meta, correlation_analysis=correlation_analysis, correlation_reports=correlation_reports, correlation_insights=correlation_insights
    )


def write_assay_outputs(result: AssayETLResult, output_dir: Path, date_tag: str, config: AssayConfig) -> dict[str, Path]:
    """Persist ETL artefacts to disk and return the generated paths."""

    try:
        output_dir.mkdir(parents=True, exist_ok=True)
    except OSError as exc:  # pragma: no cover - filesystem permission issues
        raise AssayIOError(f"Failed to create output directory: {exc}") from exc

    # File paths (unified naming with documents)
    csv_path = output_dir / f"assays_{date_tag}.csv"
    qc_path = output_dir / f"assays_{date_tag}_qc.csv"
    meta_path = output_dir / f"assays_{date_tag}_meta.yaml"

    try:
        # S06: Persist data with deterministic serialization
        logger.info("S06: Persisting data...")

        # Save CSV with deterministic order
        write_deterministic_csv(result.assays, csv_path, determinism=config.determinism, output=config.io.output)

        # Save QC data (deterministic if possible)
        try:
            write_deterministic_csv(result.qc, qc_path, determinism=config.determinism, output=None)
        except Exception:
            # Fallback to plain CSV
            result.qc.to_csv(qc_path, index=False)

        # Save metadata
        with open(meta_path, "w", encoding="utf-8") as f:
            yaml.dump(result.meta, f, default_flow_style=False, allow_unicode=True)

        # Save correlation reports if available
        outputs: dict[str, Path] = {"csv": csv_path, "qc": qc_path, "meta": meta_path}

        if result.correlation_reports:
            try:
                correlation_dir = output_dir / f"assays_correlation_report_{date_tag}"
                correlation_dir.mkdir(exist_ok=True)

                # Save each correlation report and expose as flat keys similar to documents
                for report_name, report_df in result.correlation_reports.items():
                    if report_df is not None and not report_df.empty:
                        report_path = correlation_dir / f"{report_name}.csv"
                        report_df.to_csv(report_path, index=False)
                        outputs[f"correlation_{report_name}"] = report_path

                # Save insights as JSON if present
                if result.correlation_insights:
                    import json

                    insights_path = correlation_dir / "correlation_insights.json"
                    with open(insights_path, "w", encoding="utf-8") as f:
                        json.dump(result.correlation_insights, f, ensure_ascii=False, indent=2)
                    outputs["correlation_insights"] = insights_path

                logger.info(f"Correlation reports saved to: {correlation_dir}")

            except Exception as exc:
                logger.warning(f"Failed to save correlation reports: {exc}")

        # Add file checksums to metadata (include qc like documents)
        result.meta["file_checksums"] = {
            "csv": _calculate_checksum(csv_path),
            "qc": _calculate_checksum(qc_path),
        }

        # Update metadata file with checksums
        with open(meta_path, "w", encoding="utf-8") as f:
            yaml.dump(result.meta, f, default_flow_style=False, allow_unicode=True)

    except OSError as exc:  # pragma: no cover - filesystem permission issues
        raise AssayIOError(f"Failed to write outputs: {exc}") from exc

    return outputs


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
