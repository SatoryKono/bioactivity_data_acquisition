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
from library.config import APIClientConfig, RetrySettings
from library.etl.enhanced_correlation import (
    build_correlation_insights,
    build_enhanced_correlation_analysis,
    build_enhanced_correlation_reports,
    prepare_data_for_correlation_analysis,
)
from library.etl.load import write_deterministic_csv
from library.io.meta import create_dataset_metadata
from library.schemas.assay_schema import AssayNormalizedSchema
from library.utils.empty_value_handler import (
    is_empty_value,
    normalize_list_field,
    normalize_numeric_field,
    normalize_string_field,
)
from structlog import BoundLogger

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
    from library.config import RateLimitSettings

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
    if config.http.global_.rate_limit:
        max_calls = config.http.global_.rate_limit.get("max_calls")
        period = config.http.global_.rate_limit.get("period")

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


def _enrich_target_isoforms(assays_df: pd.DataFrame, client: AssayChEMBLClient, logger: BoundLogger) -> pd.DataFrame:
    """Enrich target_uniprot_accession and target_isoform through target_component."""

    # Get unique target IDs that are not null
    target_ids = assays_df["target_chembl_id"].dropna().unique()
    # Convert to list to avoid numpy array issues
    target_ids = list(target_ids)

    if len(target_ids) == 0:
        logger.info("No target IDs found for isoform enrichment")
        return assays_df

    logger.info(f"Enriching {len(target_ids)} target IDs with component data...")

    try:
        # Fetch target component data
        components_map = client.fetch_target_components_batch(target_ids)

        def extract_isoform(target_id):
            """Extract UniProt accession and isoform from target components."""
            if pd.isna(target_id):
                return pd.NA, pd.NA

            comps = components_map.get(target_id, [])
            if not comps:
                return pd.NA, pd.NA

            # Get first component's sequences
            first_comp = comps[0]
            component_sequences = first_comp.get("component_sequences", [])

            if not component_sequences:
                return pd.NA, pd.NA

            accession = component_sequences[0].get("accession")
            if not accession:
                return pd.NA, pd.NA

            # Parse UniProt accession: P12345-2 -> (P12345, 2)
            # Regex pattern for UniProt accession with optional isoform suffix
            match = re.match(r"([A-Z0-9]+)(?:-(\d+))?", accession)
            if match:
                base_acc = match.group(1)
                isoform = int(match.group(2)) if match.group(2) else pd.NA
                return base_acc, isoform
            else:
                # If no match, return accession as-is with no isoform
                return accession, pd.NA

        # Apply extraction to all target IDs - handle tuples properly
        isoform_results = []
        for target_id in assays_df["target_chembl_id"]:
            result = extract_isoform(target_id)
            isoform_results.append(result)

        # Convert results to separate columns
        accessions = []
        isoforms = []
        for result in isoform_results:
            if result is not None and isinstance(result, tuple) and len(result) == 2:
                accessions.append(result[0])
                isoforms.append(result[1])
            else:
                accessions.append(pd.NA)
                isoforms.append(pd.NA)

        # Assign to DataFrame
        assays_df["target_uniprot_accession"] = accessions
        assays_df["target_isoform"] = isoforms

        logger.info(f"Successfully enriched {len(components_map)} target records with isoform data")

    except Exception as exc:
        logger.error(f"Error enriching target isoform data: {exc}")
        # Set empty values for failed enrichment
        assays_df["target_uniprot_accession"] = None
        assays_df["target_isoform"] = None

    return assays_df


def _extract_variant_assays(client: AssayChEMBLClient, config: AssayConfig, logger: BoundLogger) -> pd.DataFrame:
    """Extract assays with variants using variant_sequence__isnull=false filter."""

    # Variant data is now extracted directly from /assay endpoint
    # This function is kept for backward compatibility but always returns empty
    logger.info("Variant data is now extracted directly from /assay endpoint")
    return pd.DataFrame()

    logger.info("Fetching assays with variants...")

    try:
        # Apply filters if configured
        filters = {}
        if hasattr(config, "filter_profiles") and config.filter_profiles:
            # Use variant_assays profile if available, otherwise use first available profile
            profile_config = None
            if "variant_assays" in config.filter_profiles:
                profile_config = config.filter_profiles["variant_assays"]
            else:
                # Fallback to first available profile
                for _, profile_config in config.filter_profiles.items():
                    if profile_config:
                        break

            if profile_config:
                filters.update({k: v for k, v in profile_config.__dict__.items() if v is not None and k not in ["assay_type__in", "relationship_type__in"]})
                # Handle assay_type__in specially
                if hasattr(profile_config, "assay_type__in") and profile_config.assay_type__in:
                    filters["assay_type__in"] = profile_config.assay_type__in
                # Handle relationship_type__in specially
                if hasattr(profile_config, "relationship_type__in") and profile_config.relationship_type__in:
                    filters["relationship_type"] = profile_config.relationship_type__in

        # Fetch variant assays
        variant_assays = client.fetch_assays_with_variants(filters=filters, batch_size=config.variants.fetch.batch_size)

        if not variant_assays:
            logger.info("No variant assays found")
            return pd.DataFrame()

        # Convert to DataFrame
        variant_df = pd.DataFrame(variant_assays)
        logger.info(f"Fetched {len(variant_df)} variant assays")

        return variant_df

    except Exception as exc:
        logger.error(f"Error fetching variant assays: {exc}")
        return pd.DataFrame()


def _extract_assay_data(client: AssayChEMBLClient, frame: pd.DataFrame, config: AssayConfig) -> pd.DataFrame:
    """Extract assay data from ChEMBL API using streaming batch processing."""

    # Define default values for all possible columns to ensure they exist
    default_columns = {
        # Core assay fields
        "assay_chembl_id": pd.NA,
        "src_id": pd.NA,
        "src_assay_id": pd.NA,
        "assay_type": pd.NA,
        "assay_type_description": pd.NA,
        "bao_format": pd.NA,
        "bao_label": pd.NA,
        "assay_category": pd.NA,
        "assay_classifications": pd.NA,
        "target_chembl_id": pd.NA,
        "relationship_type": pd.NA,
        "confidence_score": pd.NA,
        # Variant fields from variant_sequence
        "variant_id": pd.NA,
        "variant_text": pd.NA,
        "variant_sequence_id": pd.NA,
        "isoform": pd.NA,
        "mutation": pd.NA,
        "sequence": pd.NA,
        "variant_accession": pd.NA,
        "variant_sequence_accession": pd.NA,
        "variant_sequence_mutation": pd.NA,
        "variant_organism": pd.NA,
        # Final normalized fields (will be populated in normalize step)
        "is_variant": False,
        "variant_mutations": pd.NA,
        "variant_sequence": pd.NA,
        "target_uniprot_accession": pd.NA,
        "target_isoform": pd.NA,
        # Source fields
        "src_name": pd.NA,
        "assay_organism": pd.NA,
        "assay_tax_id": pd.NA,
        "assay_cell_type": pd.NA,
        "assay_tissue": pd.NA,
        "assay_strain": pd.NA,
        "assay_subcellular_fraction": pd.NA,
        "description": pd.NA,
        "assay_parameters": pd.NA,
        "assay_parameters_json": "[]",
        "assay_format": pd.NA,
        "source_system": "ChEMBL",
        "extracted_at": pd.NA,
        "hash_row": pd.NA,
        "hash_business_key": pd.NA,
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
            logger.info(f"Processed batch {batch_index}: size={len(batch_df)}, total_rows={total_rows}, batch_size={batch_size}")

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


def _extract_assay_data_individual(client: AssayChEMBLClient, frame: pd.DataFrame, config: AssayConfig, default_columns: dict[str, Any]) -> pd.DataFrame:
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
            assay_id_dbg = row.get("assay_chembl_id", "unknown")
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


def _normalize_parameter_type(param_type: str | None) -> str | None:
    """Normalize assay parameter type field.

    Rules:
    - trim() + collapse whitespace
    - Title Case with exceptions for common terms
    """
    if is_empty_value(param_type):
        return pd.NA

    # Use unified string normalization
    normalized = normalize_string_field(param_type)
    if is_empty_value(normalized):
        return pd.NA

    # Collapse whitespace
    normalized = re.sub(r"\s+", " ", str(normalized))

    # Exact matches with special casing (case-insensitive check, but preserve exact form)
    type_lower = normalized.lower()

    # Exact form exceptions
    exact_forms = {
        "ph": "pH",
        "temperature": "Temperature",
        "temp": "Temperature",
        "incubation time": "Incubation time",
        "substrate concentration": "Substrate concentration",
        "cofactor concentration": "Cofactor concentration",
        "protein source": "Protein source",
    }

    if type_lower in exact_forms:
        return exact_forms[type_lower]

    # Default: Title Case
    return normalized.title()


def _normalize_parameter_relation(relation: str | None) -> str | None:
    """Normalize assay parameter relation field.

    Rules:
    - Valid values: <, <=, =, >=, >, ~
    - Any other or empty → null
    """
    if is_empty_value(relation):
        return pd.NA

    normalized = normalize_string_field(relation)
    if is_empty_value(normalized):
        return pd.NA

    valid_relations = {"<", "<=", "=", ">=", ">", "~"}

    if normalized in valid_relations:
        return normalized

    return pd.NA


def _normalize_parameter_value(value: Any, text_value: str | None = None) -> tuple[float | None, str | None]:
    """Normalize assay parameter value field.

    Rules:
    - Parse as decimal with dot
    - NaN, empty, non-numeric → null
    - Returns tuple: (normalized_value, fallback_text_value)
    """
    # Use unified numeric normalization
    numeric_value = normalize_numeric_field(value)

    if pd.notna(numeric_value):
        return numeric_value, normalize_string_field(text_value)

    # If numeric value failed, preserve original as text
    fallback = normalize_string_field(value) if value else pd.NA
    return pd.NA, fallback or normalize_string_field(text_value)


def _normalize_parameter_units(units: str | None) -> str | None:
    """Normalize assay parameter units field.

    Rules:
    - Trim and normalize via dictionary
    - Empty/unknown → null
    """
    if is_empty_value(units):
        return pd.NA

    normalized = normalize_string_field(units)
    if is_empty_value(normalized):
        return pd.NA

    # Units normalization dictionary
    units_map = {
        "°C": "C",
        "degC": "C",
        "Celsius": "C",
        "celsius": "C",
        "min": "min",
        "mins": "min",
        "hr": "h",
        "hrs": "h",
        "hour": "h",
        "hours": "h",
        "µM": "uM",
        "uM": "uM",
        "microM": "uM",
        "nM": "nM",
        "mM": "mM",
    }

    # Try exact match first
    if normalized in units_map:
        return units_map[normalized]

    # Try case-insensitive match
    for key, value in units_map.items():
        if str(normalized).lower() == key.lower():
            return value

    # No match found, return as-is (trimmed)
    return normalized


def _normalize_parameter_text_value(text_value: str | None) -> str | None:
    """Normalize assay parameter text_value field.

    Rules:
    - If original text_value is non-empty → trim and write
    - Otherwise → null
    """
    return normalize_string_field(text_value)


def _serialize_parameters_to_json(params_list: list[dict[str, Any]], logger: BoundLogger) -> tuple[str, dict[str, Any]]:
    """Serialize assay parameters to deterministic JSON string.

    Args:
        params_list: List of raw parameter dictionaries from ChEMBL API
        logger: Bound logger for QC metrics

    Returns:
        Tuple of (json_string, qc_metrics_dict)
    """
    import json

    # QC metrics tracking
    qc_metrics = {
        "total_raw": len(params_list),
        "invalid_relations": [],
        "unrecognized_units": [],
        "unparsed_values": [],
        "total_after_normalization": 0,
        "total_after_dedup": 0,
        "truncated": False,
    }

    if not params_list:
        return "[]", qc_metrics

    # Step 1: Normalize each parameter
    normalized_params = []

    for param in params_list:
        # Normalize type
        norm_type = _normalize_parameter_type(param.get("type"))

        # Normalize relation
        norm_relation = _normalize_parameter_relation(param.get("relation"))
        if param.get("relation") and not norm_relation:
            # Track invalid relations
            invalid_rel = str(param.get("relation")).strip()
            if invalid_rel and invalid_rel not in qc_metrics["invalid_relations"]:
                if len(qc_metrics["invalid_relations"]) < 20:
                    qc_metrics["invalid_relations"].append(invalid_rel)

        # Normalize value and text_value
        norm_value, norm_text_value = _normalize_parameter_value(param.get("value"), param.get("text_value"))

        # Track unparsed values
        if param.get("value") is not None and norm_value is None:
            unparsed = str(param.get("value"))
            if len(qc_metrics["unparsed_values"]) < 10:
                qc_metrics["unparsed_values"].append(unparsed)

        # Normalize units
        norm_units = _normalize_parameter_units(param.get("units"))
        if param.get("units") and norm_units and norm_units not in ["C", "min", "h", "uM", "nM", "mM"]:
            # Track unrecognized units (not in standard set)
            if norm_units not in qc_metrics["unrecognized_units"]:
                if len(qc_metrics["unrecognized_units"]) < 20:
                    qc_metrics["unrecognized_units"].append(norm_units)

        # Finalize text_value
        final_text_value = _normalize_parameter_text_value(norm_text_value)

        # Build normalized parameter object
        normalized_params.append({"type": norm_type, "relation": norm_relation, "value": norm_value, "units": norm_units, "text_value": final_text_value})

    qc_metrics["total_after_normalization"] = len(normalized_params)

    # Step 2: Deduplication
    # Create tuples for dedup (convert None to special marker for hashability)
    def make_hashable(p):
        return (p["type"], p["relation"], p["value"], p["units"], p["text_value"])

    seen = set()
    deduped_params = []
    for param in normalized_params:
        key = make_hashable(param)
        if key not in seen:
            seen.add(key)
            deduped_params.append(param)

    qc_metrics["total_after_dedup"] = len(deduped_params)

    # Step 3: Deterministic sorting
    # Sort order: type (lex), units (lex, null last), value (numeric, null last), relation (custom order)
    relation_order = {"<": 0, "<=": 1, "=": 2, ">=": 3, ">": 4, "~": 5, None: 6}

    def sort_key(p):
        return (
            p["type"] or "",  # type: lexicographic, empty string for null
            p["units"] or "\uffff",  # units: lexicographic, high unicode for null (sorts last)
            p["value"] if p["value"] is not None else float("inf"),  # value: numeric, inf for null
            relation_order.get(p["relation"], 6),  # relation: custom order
        )

    sorted_params = sorted(deduped_params, key=sort_key)

    # Step 4: Serialize to JSON with deterministic formatting
    # Ensure keys are in correct order in each object
    ordered_params = []
    for param in sorted_params:
        ordered_params.append({"type": param["type"], "relation": param["relation"], "value": param["value"], "units": param["units"], "text_value": param["text_value"]})

    # Serialize with compact format
    json_str = json.dumps(ordered_params, separators=(",", ":"), ensure_ascii=True, sort_keys=False)  # Order already enforced

    # Step 5: Check length and truncate if needed
    MAX_LENGTH = 64 * 1024  # 64 KiB

    if len(json_str) > MAX_LENGTH:
        qc_metrics["truncated"] = True
        logger.warning(f"Assay parameters JSON exceeds 64 KiB ({len(json_str)} bytes), truncating...")

        # Truncate by removing elements from the end until we fit
        # Keep re-serializing to ensure valid JSON
        truncated_params = ordered_params[:]
        while len(json_str) > MAX_LENGTH and len(truncated_params) > 0:
            truncated_params.pop()
            json_str = json.dumps(truncated_params, separators=(",", ":"), ensure_ascii=True, sort_keys=False)

        logger.warning(f"Truncated to {len(truncated_params)} parameters ({len(json_str)} bytes)")

    return json_str, qc_metrics


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
            normalized[col] = normalized[col].apply(normalize_string_field)

    # Mapping assay_type to description
    assay_type_mapping = {"B": "Binding", "F": "Functional", "P": "Physicochemical", "U": "Unclassified"}

    if "assay_type" in normalized.columns:
        if "assay_type_description" in normalized.columns:
            normalized["assay_type_description"] = normalized["assay_type"].map(assay_type_mapping).fillna(normalized["assay_type_description"])
        else:
            normalized["assay_type_description"] = normalized["assay_type"].map(assay_type_mapping)

    # Normalize list fields
    list_columns = ["assay_category", "assay_classifications"]
    for col in list_columns:
        if col in normalized.columns:
            normalized[col] = normalized[col].apply(normalize_list_field)

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

    # Map variant_sequence fields to final normalized fields
    # isoform → target_isoform
    if "isoform" in normalized.columns:
        normalized["target_isoform"] = pd.to_numeric(normalized["isoform"], errors="coerce").astype("Int64")

    # mutation → variant_mutations
    if "mutation" in normalized.columns:
        # Применяем normalize_string_field к исходному полю mutation
        normalized["variant_mutations"] = normalized["mutation"].apply(normalize_string_field)

    # sequence → variant_sequence
    if "sequence" in normalized.columns:
        normalized["variant_sequence"] = normalized["sequence"]

    # variant_accession stays the same
    if "variant_accession" in normalized.columns:
        normalized["variant_accession"] = normalized["variant_accession"]

    # variant_sequence_accession stays the same
    if "variant_sequence_accession" in normalized.columns:
        normalized["variant_sequence_accession"] = normalized["variant_sequence_accession"]

    # variant_sequence_mutation stays the same
    if "variant_sequence_mutation" in normalized.columns:
        normalized["variant_sequence_mutation"] = normalized["variant_sequence_mutation"]

    # Set is_variant flag based on presence of variant data
    if "sequence" in normalized.columns and "mutation" in normalized.columns:
        normalized["is_variant"] = normalized["sequence"].notna() | normalized["mutation"].notna()
    elif "sequence" in normalized.columns:
        normalized["is_variant"] = normalized["sequence"].notna()
    elif "mutation" in normalized.columns:
        normalized["is_variant"] = normalized["mutation"].notna()

    # Extract variant_text from description if mutation is empty
    def extract_variant_text(row):
        """Extract variant text from description if not already present."""
        if pd.notna(row.get("mutation")) and str(row.get("mutation")).strip():
            return row["mutation"]

        desc = str(row.get("description", ""))
        if not desc or desc == "nan":
            return pd.NA

        # Regex for patterns: "mutant", "variant", "mutation"
        match = re.search(r"(mutant|variant|mutation)[:\s]+([\w\s,]+)", desc, re.IGNORECASE)
        return match.group(0) if match else pd.NA

    if "variant_text" in normalized.columns:
        normalized["variant_text"] = normalized.apply(extract_variant_text, axis=1)

    # Normalize variant string fields
    variant_string_columns = [
        "variant_text",
        "variant_accession",
        "variant_sequence_accession",
        "variant_sequence_mutation",
        "target_uniprot_accession",
        "variant_sequence",
        # variant_mutations уже нормализовано при маппинге
        "variant_organism",
    ]
    for col in variant_string_columns:
        if col in normalized.columns:
            normalized[col] = normalized[col].apply(normalize_string_field)

    # Remove intermediate columns that were mapped to final normalized fields
    intermediate_columns = ["isoform", "mutation", "sequence"]
    for col in intermediate_columns:
        if col in normalized.columns:
            normalized = normalized.drop(columns=[col])

    return normalized


def _validate_variant_rules(assays_df: pd.DataFrame, logger: BoundLogger) -> None:
    """Validate variant-specific business rules."""

    # Rule 1: is_variant=True ⇒ has at least one variant field
    if "is_variant" in assays_df.columns:
        variant_rows = assays_df[assays_df["is_variant"]]
        if len(variant_rows) > 0:
            invalid_variants = variant_rows[
                variant_rows["variant_sequence"].isna() & variant_rows["variant_mutations"].isna() & variant_rows["variant_accession"].isna() & variant_rows["variant_text"].isna()
            ]
            if len(invalid_variants) > 0:
                logger.warning(f"Found {len(invalid_variants)} records with is_variant=True but no variant data")

    # Rule 2: UniProt accession format validation
    if "variant_accession" in assays_df.columns:
        uniprot_pattern = r"^[A-Z][0-9][A-Z0-9]{3}[0-9]$"
        variant_accessions = assays_df[assays_df["variant_accession"].notna()]
        if len(variant_accessions) > 0:
            invalid_accessions = variant_accessions[~variant_accessions["variant_accession"].str.match(uniprot_pattern, na=False)]
            if len(invalid_accessions) > 0:
                logger.warning(f"Found {len(invalid_accessions)} invalid UniProt accessions in variant_accession")

    if "target_uniprot_accession" in assays_df.columns:
        uniprot_pattern = r"^[A-Z][0-9][A-Z0-9]{3}[0-9]$"
        target_accessions = assays_df[assays_df["target_uniprot_accession"].notna()]
        if len(target_accessions) > 0:
            invalid_accessions = target_accessions[~target_accessions["target_uniprot_accession"].str.match(uniprot_pattern, na=False)]
            if len(invalid_accessions) > 0:
                logger.warning(f"Found {len(invalid_accessions)} invalid UniProt accessions in target_uniprot_accession")

    # Rule 3: variant_id ⇒ target should be SINGLE PROTEIN (simplified check)
    if "variant_id" in assays_df.columns:
        variants_without_target = assays_df[assays_df["variant_id"].notna() & assays_df["target_chembl_id"].isna()]
        if len(variants_without_target) > 0:
            logger.error(f"Found {len(variants_without_target)} variants without target_chembl_id")
            raise AssayValidationError("Variants must have valid target_chembl_id")

    # Rule 4: target_isoform should be positive integer when present
    if "target_isoform" in assays_df.columns:
        invalid_isoforms = assays_df[assays_df["target_isoform"].notna() & (assays_df["target_isoform"] <= 0)]
        if len(invalid_isoforms) > 0:
            logger.warning(f"Found {len(invalid_isoforms)} records with invalid target_isoform (≤0)")


def _validate_assay_schema(assays_df: pd.DataFrame) -> pd.DataFrame:
    """Validate assay schema using Pandera."""
    try:
        # Convert extracted_at to datetime
        if "extracted_at" in assays_df.columns:
            assays_df["extracted_at"] = pd.to_datetime(assays_df["extracted_at"])

        # Add chembl_release if not present
        if "chembl_release" not in assays_df.columns:
            assays_df["chembl_release"] = "ChEMBL_33"  # Default release version

        # Fill None values for required non-nullable fields
        required_fields = {"source_system": "ChEMBL", "extracted_at": pd.Timestamp.now().isoformat(), "hash_row": "unknown", "hash_business_key": "unknown"}

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

    # S01: Create API client
    logger.info("S01: Creating API client...")
    client = _create_api_client(config)

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

        # Variant data is now extracted directly from /assay endpoint
        # No need for separate variant extraction

    elif target_chembl_id:
        # Extract by target
        enriched_frame = _extract_assay_data_by_target(client, target_chembl_id, filters, config)

        if config.runtime.limit is not None:
            enriched_frame = enriched_frame.head(config.runtime.limit)

        # Variant data is now extracted directly from /assay endpoint
        # No need for separate variant extraction
    else:
        raise AssayValidationError("Either assay_ids or target_chembl_id must be provided")

    if enriched_frame.empty:
        logger.warning("No assay data extracted")
        # Create metadata using new metadata system

        # Create API config for metadata
        from library.config import RetrySettings

        api_config = APIClientConfig(name="chembl", base_url="https://www.ebi.ac.uk/chembl/api/data", timeout=30, retries=RetrySettings(total=3))

        metadata_handler = create_dataset_metadata("chembl", api_config, logger)
        meta = metadata_handler.to_dict(api_config)
        meta["row_count"] = 0

        return AssayETLResult(assays=pd.DataFrame(), qc=pd.DataFrame([{"metric": "row_count", "value": 0}]), meta=meta)

    # S03: Enrich with source data
    logger.info("S03: Enriching with source data...")
    enriched_frame = _enrich_with_source_data(client, enriched_frame)

    # S03.1: Enrich with target isoform data (variant data already extracted from /assay)
    logger.info("S03.1: Enriching with target isoform data...")
    enriched_frame = _enrich_target_isoforms(enriched_frame, client, logger)

    # S03.2: Assay parameters already extracted from /assay endpoint
    logger.info("S03.2: Assay parameters already included in /assay response")
    parameters_qc = {}  # Empty QC for now

    # S04: Normalize fields
    logger.info("S04: Normalizing fields...")
    enriched_frame = _normalize_assay_fields(enriched_frame)

    # Note: chembl_release is now handled at metadata level, not per-record

    # Generate hashes for all records
    enriched_frame["hash_business_key"] = enriched_frame["assay_chembl_id"].apply(lambda x: hashlib.sha256(str(x).encode()).hexdigest() if pd.notna(x) else "unknown")
    enriched_frame["hash_row"] = enriched_frame.apply(lambda row: hashlib.sha256(str(row.to_dict()).encode()).hexdigest(), axis=1)

    # S05: Validate variant rules
    logger.info("S05: Validating variant rules...")
    _validate_variant_rules(enriched_frame, logger)

    # S06: Validate schema
    logger.info("S06: Validating schema...")
    validated_frame = _validate_assay_schema(enriched_frame)

    # Calculate QC metrics
    qc_metrics = [
        {"metric": "row_count", "value": int(len(validated_frame))},
        {"metric": "unique_sources", "value": int(validated_frame["src_id"].nunique())},
        {"metric": "assay_types", "value": validated_frame["assay_type"].value_counts().to_dict()},
        {"metric": "relationship_types", "value": validated_frame["relationship_type"].value_counts().to_dict()},
        {"metric": "confidence_scores", "value": validated_frame["confidence_score"].value_counts().to_dict()},
    ]

    # Add assay parameters QC metrics
    if parameters_qc:
        qc_metrics.append({"metric": "assay_parameters_qc", "value": parameters_qc})

    qc = pd.DataFrame(qc_metrics)
    # Унифицируем базовые QC метрики
    try:
        from library.etl.qc_common import ensure_common_qc

        qc = ensure_common_qc(validated_frame, qc, module_name="assay")
    except Exception as exc:
        import logging as _logging

        _logging.getLogger(__name__).warning(f"Failed to normalize QC metrics for assay: {exc}")

    # Create metadata using new metadata system

    # Create API config for metadata
    from library.config import RetrySettings

    api_config = APIClientConfig(name="chembl", base_url="https://www.ebi.ac.uk/chembl/api/data", timeout=30, retries=RetrySettings(total=3, backoff_multiplier=1.0))

    metadata_handler = create_dataset_metadata("chembl", api_config, logger)
    meta = metadata_handler.to_dict(api_config)

    # Add pipeline-specific metadata
    meta.update(
        {
            "pipeline_version": "1.0.0",
            "row_count": len(validated_frame),
            "extraction_parameters": {
                "total_assays": len(validated_frame),
                "unique_sources": validated_frame["src_id"].nunique(),
                "assay_types": validated_frame["assay_type"].value_counts().to_dict(),
                "relationship_types": validated_frame["relationship_type"].value_counts().to_dict(),
            },
        }
    )

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

    # File paths
    csv_path = output_dir / f"assay_{date_tag}.csv"
    qc_path = output_dir / f"assay_{date_tag}_qc.csv"
    meta_path = output_dir / f"assay_{date_tag}_meta.yaml"

    try:
        # S06: Persist data with deterministic serialization
        logger.info("S06: Persisting data...")

        # Save CSV with deterministic order
        write_deterministic_csv(result.assays, csv_path, determinism=config.determinism, output=config.io.output)

        # Save QC data (всегда создаём файл)
        if isinstance(result.qc, pd.DataFrame) and not result.qc.empty:
            result.qc.to_csv(qc_path, index=False)
        else:
            import pandas as _pd

            _pd.DataFrame([{"metric": "row_count", "value": int(len(result.assays))}]).to_csv(qc_path, index=False)

        # Save metadata
        with open(meta_path, "w", encoding="utf-8") as f:
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
        with open(meta_path, "w", encoding="utf-8") as f:
            yaml.dump(result.meta, f, default_flow_style=False, allow_unicode=True)

    except OSError as exc:  # pragma: no cover - filesystem permission issues
        raise AssayIOError(f"Failed to write outputs: {exc}") from exc

    result_paths: dict[str, Any] = {"csv": csv_path, "qc": qc_path, "meta": meta_path}

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
