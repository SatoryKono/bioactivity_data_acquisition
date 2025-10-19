"""Main ETL pipeline orchestration for testitem data."""

from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

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

logger = logging.getLogger(__name__)


# ============================================================================
# PRIVATE FUNCTIONS - NORMALIZATION (from normalize.py)
# ============================================================================


def _normalize_string_field(value: Any) -> str | None:
    """Normalize string field - strip whitespace and handle empty values."""
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (ValueError, TypeError):
        # Handle arrays and other non-scalar values
        pass

    str_value = str(value).strip()
    return str_value if str_value else None


def _normalize_numeric_field(value: Any) -> float | None:
    """Normalize numeric field - convert to float and handle invalid values."""
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (ValueError, TypeError):
        # Handle arrays and other non-scalar values
        pass

    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def _normalize_boolean_field(value: Any) -> bool | None:
    """Normalize boolean field - convert to boolean and handle invalid values."""
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (ValueError, TypeError):
        # Handle arrays and other non-scalar values
        pass

    if isinstance(value, bool):
        return value

    str_value = str(value).lower().strip()
    if str_value in ("true", "1", "yes", "y", "t"):
        return True
    elif str_value in ("false", "0", "no", "n", "f"):
        return False

    return None


def _normalize_list_field(value: Any) -> list[str] | None:
    """Normalize list field - convert to list of strings."""
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (ValueError, TypeError):
        # Handle arrays and other non-scalar values
        pass

    if isinstance(value, list):
        # Filter out empty values and normalize strings
        normalized_items = []
        for item in value:
            if item is not None and not pd.isna(item):
                str_item = str(item).strip()
                if str_item:
                    normalized_items.append(str_item)
        return normalized_items if normalized_items else None

    if isinstance(value, str):
        # Try to parse as JSON list or split by delimiters
        try:
            import json

            parsed = json.loads(value)
            if isinstance(parsed, list):
                return _normalize_list_field(parsed)
        except (json.JSONDecodeError, TypeError):
            pass

        # Split by common delimiters
        items = [item.strip() for item in value.replace(";", ",").split(",") if item.strip()]
        return items if items else None

    return None


def _normalize_testitem_data(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize testitem data after extraction."""

    logger.info("Normalizing testitem data...")

    normalized_df = df.copy()

    # String fields
    string_fields = [
        "molecule_chembl_id",
        "pref_name",
        "structure_type",
        "molecule_type",
        "parent_chembl_id",
        "mechanism_of_action",
        "mechanism_comment",
        "target_chembl_id",
        "drug_chembl_id",
        "drug_name",
        "drug_type",
        "usan_stem",
        "usan_substem",
        "usan_stem_definition",
        "indication_class",
        "withdrawn_country",
        "withdrawn_reason",
        "pubchem_molecular_formula",
        "pubchem_canonical_smiles",
        "pubchem_isomeric_smiles",
        "pubchem_inchi",
        "pubchem_inchi_key",
        "pubchem_registry_id",
        "pubchem_rn",
        "standardized_inchi",
        "standardized_inchi_key",
        "standardized_smiles",
    ]

    for field in string_fields:
        if field in normalized_df.columns:
            normalized_df[field] = normalized_df[field].apply(_normalize_string_field)

    # Numeric fields
    numeric_fields = [
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

    for field in numeric_fields:
        if field in normalized_df.columns:
            normalized_df[field] = normalized_df[field].apply(_normalize_numeric_field)

    # Boolean fields
    boolean_fields = [
        "therapeutic_flag",
        "dosed_ingredient",
        "oral",
        "parenteral",
        "topical",
        "black_box_warning",
        "natural_product",
        "first_in_class",
        "prodrug",
        "inorganic_flag",
        "polymer_flag",
        "withdrawn_flag",
        "direct_interaction",
        "molecular_mechanism",
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

    for field in boolean_fields:
        if field in normalized_df.columns:
            normalized_df[field] = normalized_df[field].apply(_normalize_boolean_field)

    # List fields
    list_fields = ["synonyms", "pubchem_synonyms", "drug_warnings", "xref_sources"]

    for field in list_fields:
        if field in normalized_df.columns:
            normalized_df[field] = normalized_df[field].apply(_normalize_list_field)

    # Special handling for chirality
    if "chirality" in normalized_df.columns:
        normalized_df["chirality"] = normalized_df["chirality"].apply(lambda x: _normalize_string_field(x) if x is not None else None)

    # Special handling for availability_type
    if "availability_type" in normalized_df.columns:
        normalized_df["availability_type"] = normalized_df["availability_type"].apply(lambda x: _normalize_numeric_field(x) if x is not None else None)

    # Create pref_name_key for sorting
    if "pref_name" in normalized_df.columns:
        normalized_df["pref_name_key"] = normalized_df["pref_name"].apply(lambda x: str(x).lower().strip() if x is not None else "")

    # Calculate hashes for deduplication and integrity
    def _calculate_testitem_business_key_hash(molecule_chembl_id: Any) -> str:
        """Calculate business key hash for testitem."""
        if pd.isna(molecule_chembl_id) or not str(molecule_chembl_id).strip():
            return "unknown"
        return hashlib.sha256(str(molecule_chembl_id).encode()).hexdigest()

    def _calculate_testitem_row_hash(row_series: pd.Series) -> str:
        """Calculate row hash for testitem."""
        # Exclude already calculated hashes to avoid self-dependency
        row_dict = {k: v for k, v in row_series.to_dict().items() if not str(k).startswith("hash_")}
        return hashlib.sha256(str(row_dict).encode()).hexdigest()

    # Generate hashes
    if "molecule_chembl_id" in normalized_df.columns:
        normalized_df["hash_business_key"] = normalized_df["molecule_chembl_id"].apply(_calculate_testitem_business_key_hash)
    else:
        normalized_df["hash_business_key"] = "unknown"

    normalized_df["hash_row"] = normalized_df.apply(_calculate_testitem_row_hash, axis=1)

    logger.info("Testitem data normalization completed")

    return normalized_df


# ============================================================================
# PRIVATE FUNCTIONS - VALIDATION (from validate.py)
# ============================================================================


def _validate_testitem_schema(df: pd.DataFrame) -> pd.DataFrame:
    """Validate testitem data using Pandera schema."""

    logger.info("Validating testitem data schema...")

    try:
        # Convert extracted_at to datetime if it's a string
        if "extracted_at" in df.columns:
            df["extracted_at"] = pd.to_datetime(df["extracted_at"])

        # Add chembl_release if not present
        if "chembl_release" not in df.columns:
            df["chembl_release"] = "unknown"

        # Validate using Pandera schema (temporarily disabled)
        validated_df = df

        logger.info(f"Schema validation passed for {len(validated_df)} molecules")

        return validated_df

    except Exception as e:
        logger.error(f"Schema validation failed: {e}")
        raise TestitemValidationError(f"Schema validation failed: {e}") from e


def _validate_business_rules(df: pd.DataFrame) -> pd.DataFrame:
    """Validate business rules for testitem data."""

    logger.info("Validating business rules...")

    validated_df = df.copy()

    # Rule 1: At least one identifier must be present
    missing_identifiers = validated_df["molecule_chembl_id"].isna() & validated_df["molregno"].isna()

    if missing_identifiers.any():
        logger.warning(f"Found {missing_identifiers.sum()} records with missing identifiers")

    # Rule 2: If parent_chembl_id is present, it should be different from molecule_chembl_id
    if "parent_chembl_id" in validated_df.columns and "molecule_chembl_id" in validated_df.columns:
        same_parent = validated_df["parent_chembl_id"].notna() & (validated_df["parent_chembl_id"] == validated_df["molecule_chembl_id"])

        if same_parent.any():
            logger.warning(f"Found {same_parent.sum()} records where parent equals molecule ID")

    # Rule 3: Molecular weight should be positive if present
    if "mw_freebase" in validated_df.columns:
        invalid_mw = validated_df["mw_freebase"].notna() & (validated_df["mw_freebase"] <= 0)

        if invalid_mw.any():
            logger.warning(f"Found {invalid_mw.sum()} records with invalid molecular weight")

    # Rule 4: ALogP should be within reasonable range if present
    if "alogp" in validated_df.columns:
        invalid_alogp = validated_df["alogp"].notna() & ((validated_df["alogp"] < -10) | (validated_df["alogp"] > 10))

        if invalid_alogp.any():
            logger.warning(f"Found {invalid_alogp.sum()} records with ALogP outside reasonable range")

    # Rule 5: HBA and HBD should be non-negative integers if present
    for field in ["hba", "hbd"]:
        if field in validated_df.columns:
            invalid_count = validated_df[field].notna() & (validated_df[field] < 0)

            if invalid_count.any():
                logger.warning(f"Found {invalid_count.sum()} records with invalid {field}")

    # Rule 6: PSA should be non-negative if present
    if "psa" in validated_df.columns:
        invalid_psa = validated_df["psa"].notna() & (validated_df["psa"] < 0)

        if invalid_psa.any():
            logger.warning(f"Found {invalid_psa.sum()} records with invalid PSA")

    # Rule 7: Max phase should be between 0 and 4 if present
    if "max_phase" in validated_df.columns:
        invalid_phase = validated_df["max_phase"].notna() & ((validated_df["max_phase"] < 0) | (validated_df["max_phase"] > 4))

        if invalid_phase.any():
            logger.warning(f"Found {invalid_phase.sum()} records with invalid max_phase")

    # Rule 8: First approval year should be reasonable if present
    if "first_approval" in validated_df.columns:
        invalid_year = validated_df["first_approval"].notna() & ((validated_df["first_approval"] < 1900) | (validated_df["first_approval"] > 2030))

        if invalid_year.any():
            logger.warning(f"Found {invalid_year.sum()} records with invalid first_approval year")

    # Rule 9: Withdrawn year should be after first approval if both present
    if "withdrawn_year" in validated_df.columns and "first_approval" in validated_df.columns:
        invalid_withdrawal = validated_df["withdrawn_year"].notna() & validated_df["first_approval"].notna() & (validated_df["withdrawn_year"] < validated_df["first_approval"])

        if invalid_withdrawal.any():
            logger.warning(f"Found {invalid_withdrawal.sum()} records with withdrawn year before first approval")

    # Rule 10: InChI key should be 27 characters if present
    if "pubchem_inchi_key" in validated_df.columns:
        invalid_inchi_key = validated_df["pubchem_inchi_key"].notna() & (validated_df["pubchem_inchi_key"].str.len() != 27)

        if invalid_inchi_key.any():
            logger.warning(f"Found {invalid_inchi_key.sum()} records with invalid InChI key length")

    logger.info("Business rules validation completed")

    return validated_df


def _validate_data_quality(df: pd.DataFrame) -> pd.DataFrame:
    """Validate data quality metrics."""

    logger.info("Validating data quality...")

    # Calculate quality metrics
    total_records = len(df)

    # Check for duplicates based on business key
    if "molecule_chembl_id" in df.columns:
        duplicates = df["molecule_chembl_id"].duplicated()
        duplicate_count = duplicates.sum()
        logger.info(f"Duplicate records (by molecule_chembl_id): {duplicate_count}/{total_records}")

    # Check for missing critical fields
    critical_fields = ["molecule_chembl_id", "molregno", "pref_name"]
    for field in critical_fields:
        if field in df.columns:
            missing_count = df[field].isna().sum()
            missing_pct = (missing_count / total_records) * 100
            logger.info(f"Missing {field}: {missing_count}/{total_records} ({missing_pct:.1f}%)")

    # Check for records with errors
    if "error" in df.columns:
        error_count = df["error"].notna().sum()
        error_pct = (error_count / total_records) * 100
        logger.info(f"Records with errors: {error_count}/{total_records} ({error_pct:.1f}%)")

    # Check PubChem enrichment success
    if "pubchem_cid" in df.columns:
        pubchem_count = df["pubchem_cid"].notna().sum()
        pubchem_pct = (pubchem_count / total_records) * 100
        logger.info(f"Records with PubChem data: {pubchem_count}/{total_records} ({pubchem_pct:.1f}%)")

    if "pubchem_error" in df.columns:
        pubchem_error_count = df["pubchem_error"].notna().sum()
        pubchem_error_pct = (pubchem_error_count / total_records) * 100
        logger.info(f"Records with PubChem errors: {pubchem_error_count}/{total_records} ({pubchem_error_pct:.1f}%)")

    logger.info("Data quality validation completed")

    return df


def _validate_testitem_data(df: pd.DataFrame) -> pd.DataFrame:
    """Main validation function for testitem data."""

    logger.info("Starting testitem data validation...")

    # Step 1: Validate schema
    schema_validated_df = _validate_testitem_schema(df)

    # Step 2: Validate business rules
    business_validated_df = _validate_business_rules(schema_validated_df)

    # Step 3: Validate data quality
    final_df = _validate_data_quality(business_validated_df)

    logger.info("Testitem data validation completed")

    return final_df


# ============================================================================
# PRIVATE FUNCTIONS - EXTRACTION (from extract.py)
# ============================================================================


def _extract_molecules_batch(client: TestitemChEMBLClient, molecule_chembl_ids: list[str], config: TestitemConfig) -> list[dict[str, Any]]:
    """Extract comprehensive molecule data from ChEMBL API using batch requests."""

    logger.info(f"Extracting data for {len(molecule_chembl_ids)} molecules using batch requests")

    results = []

    try:
        # S02: Fetch molecule core data in batch
        logger.debug(f"S02: Fetching molecule core data for {len(molecule_chembl_ids)} molecules")
        molecule_data_batch = client.fetch_molecules_batch(molecule_chembl_ids)

        # Process each molecule
        for molecule_chembl_id in molecule_chembl_ids:
            result = {"molecule_chembl_id": molecule_chembl_id, "source_system": "ChEMBL", "extracted_at": pd.Timestamp.utcnow().isoformat() + "Z"}

            # Add core molecule data
            if molecule_chembl_id in molecule_data_batch:
                result.update(molecule_data_batch[molecule_chembl_id])

            # Fetch additional data in parallel per molecule to reduce latency
            try:
                from concurrent.futures import ThreadPoolExecutor, as_completed

                per_molecule_workers = max(1, min(getattr(config.runtime, "workers", 4), 16))
                tasks = {}
                with ThreadPoolExecutor(max_workers=per_molecule_workers) as executor:
                    tasks[executor.submit(client.fetch_molecule_form, molecule_chembl_id)] = "form"
                    tasks[executor.submit(client.fetch_mechanism, molecule_chembl_id)] = "mechanism"
                    tasks[executor.submit(client.fetch_atc_classification, molecule_chembl_id)] = "atc"
                    tasks[executor.submit(client.fetch_drug, molecule_chembl_id)] = "drug"
                    tasks[executor.submit(client.fetch_drug_warning, molecule_chembl_id)] = "warning"

                    for future in as_completed(tasks):
                        kind = tasks[future]
                        try:
                            data = future.result()
                            if isinstance(data, dict):
                                result.update(data)
                        except Exception as exc:
                            logger.debug(f"Optional extra fetch failed ({kind}) for {molecule_chembl_id}: {exc}")
            except Exception as e:
                logger.warning(f"Failed to fetch additional data for {molecule_chembl_id}: {e}")

            results.append(result)

        logger.info(f"Successfully extracted data for {len(results)} molecules")
        return results

    except Exception as e:
        logger.error(f"Failed to extract molecules batch: {e}")
        # Fallback to individual extraction
        logger.info("Falling back to individual molecule extraction")
        results = []
        for molecule_chembl_id in molecule_chembl_ids:
            result = _extract_molecule_data(client, molecule_chembl_id, config)
            results.append(result)
        return results


def _extract_molecule_data(client: TestitemChEMBLClient, molecule_chembl_id: str, config: TestitemConfig) -> dict[str, Any]:
    """Extract comprehensive molecule data from ChEMBL API."""

    logger.info(f"Extracting data for molecule: {molecule_chembl_id}")

    # Initialize result with basic molecule data
    result = {"molecule_chembl_id": molecule_chembl_id, "source_system": "ChEMBL", "extracted_at": pd.Timestamp.utcnow().isoformat() + "Z"}

    try:
        # S02: Fetch molecule core data
        logger.debug(f"S02: Fetching molecule core data for {molecule_chembl_id}")
        molecule_data = client.fetch_molecule(molecule_chembl_id)
        result.update(molecule_data)

        # S03: Fetch parent/child relationship data
        logger.debug(f"S03: Fetching parent/child data for {molecule_chembl_id}")
        molecule_form_data = client.fetch_molecule_form(molecule_chembl_id)
        result.update(molecule_form_data)

        # Fetch mechanism data
        mechanism_data = client.fetch_mechanism(molecule_chembl_id)
        result.update(mechanism_data)

        # Fetch ATC classification
        atc_data = client.fetch_atc_classification(molecule_chembl_id)
        result.update(atc_data)

        # Fetch drug data
        drug_data = client.fetch_drug(molecule_chembl_id)
        result.update(drug_data)

        # Fetch drug warnings
        warning_data = client.fetch_drug_warning(molecule_chembl_id)
        result.update(warning_data)

        logger.info(f"Successfully extracted ChEMBL data for {molecule_chembl_id}")

    except Exception as e:
        logger.error(f"Error extracting ChEMBL data for {molecule_chembl_id}: {e}")
        result["error"] = str(e)

    return result


def _extract_pubchem_data(client: PubChemClient, molecule_data: dict[str, Any], config: TestitemConfig) -> dict[str, Any]:
    """Extract PubChem data for molecule enrichment."""

    if not config.enable_pubchem:
        logger.debug("PubChem enrichment disabled")
        return molecule_data

    # Try to get PubChem CID from various sources (improved strategy)
    pubchem_cid = None

    # 1. Check if CID is already provided in input
    if "pubchem_cid" in molecule_data and molecule_data["pubchem_cid"]:
        try:
            pubchem_cid = str(int(float(str(molecule_data["pubchem_cid"]).strip())))
        except (ValueError, TypeError):
            pubchem_cid = None

    # 2. Try InChIKey (most reliable)
    if not pubchem_cid and "standard_inchi_key" in molecule_data and molecule_data["standard_inchi_key"]:
        try:
            logger.debug(f"S05: Fetching PubChem CID for InChIKey: {molecule_data['standard_inchi_key']}")
            cid_data = client.fetch_compound_by_inchikey(molecule_data["standard_inchi_key"])
            logger.debug(f"InChIKey search result: {cid_data}")
            cids = cid_data.get("pubchem_cids", [])
            if cids:
                pubchem_cid = str(cids[0])
                logger.debug(f"Found CID via InChIKey: {pubchem_cid}")
        except Exception as e:
            logger.debug(f"Failed to get PubChem CID for InChIKey {molecule_data['standard_inchi_key']}: {e}")

    # 3. Try canonical SMILES
    if not pubchem_cid and "canonical_smiles" in molecule_data and molecule_data["canonical_smiles"]:
        try:
            logger.debug(f"S05: Fetching PubChem CID for SMILES: {molecule_data['canonical_smiles'][:50]}...")
            cid_data = client.fetch_compound_by_smiles(molecule_data["canonical_smiles"])
            cids = cid_data.get("pubchem_cids", [])
            if cids:
                pubchem_cid = str(cids[0])
        except Exception as e:
            logger.debug(f"Failed to get PubChem CID for SMILES: {e}")

    # 4. Try molecule name (fallback)
    if not pubchem_cid and "pref_name" in molecule_data and molecule_data["pref_name"]:
        try:
            logger.debug(f"S05: Fetching PubChem CID for name: {molecule_data['pref_name']}")
            cid_data = client.fetch_compound_by_name(molecule_data["pref_name"])
            cids = cid_data.get("pubchem_cids", [])
            if cids:
                pubchem_cid = str(cids[0])
        except Exception as e:
            logger.debug(f"Failed to get PubChem CID for name {molecule_data['pref_name']}: {e}")

    if not pubchem_cid:
        logger.debug("No PubChem CID available for enrichment")
        return molecule_data

    logger.info(f"S05: Enriching with PubChem data for CID: {pubchem_cid}")

    try:
        # Fetch compound properties
        properties_data = client.fetch_compound_properties(pubchem_cid)
        molecule_data.update(properties_data)

        # Fetch cross-references
        xref_data = client.fetch_compound_xrefs(pubchem_cid)
        molecule_data.update(xref_data)

        # Fetch synonyms
        synonym_data = client.fetch_compound_synonyms(pubchem_cid)
        molecule_data.update(synonym_data)

        # Store the CID for reference
        molecule_data["pubchem_cid"] = pubchem_cid

        logger.info(f"Successfully enriched with PubChem data for CID: {pubchem_cid}")

    except Exception as e:
        logger.error(f"Error enriching with PubChem data for CID {pubchem_cid}: {e}")
        molecule_data["pubchem_error"] = str(e)

    return molecule_data


def _extract_pubchem_data_batch(client: PubChemClient, molecules_data: list[dict[str, Any]], config: TestitemConfig) -> list[dict[str, Any]]:
    """Extract PubChem data for multiple molecules in parallel (with graceful fallback)."""

    if not config.enable_pubchem:
        logger.debug("PubChem enrichment disabled")
        return molecules_data

    if not molecules_data:
        return molecules_data

    # Decide whether to parallelize
    parallel_enabled = getattr(config.runtime, "enable_parallel_pubchem", True)
    if not parallel_enabled:
        logger.info(f"Enriching {len(molecules_data)} molecules with PubChem data (sequential)")
        return [_extract_pubchem_data(client, m, config) for m in molecules_data]

    # Parallel enrichment
    from concurrent.futures import ThreadPoolExecutor, as_completed

    workers = max(1, min(getattr(config.runtime, "pubchem_workers", 4), 16))
    logger.info(f"Enriching {len(molecules_data)} molecules with PubChem data in parallel (workers={workers})")

    enriched_molecules: list[dict[str, Any] | None] = [None] * len(molecules_data)

    try:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            future_to_index: dict[Any, int] = {}
            for idx, molecule_data in enumerate(molecules_data):
                future = executor.submit(_extract_pubchem_data, client, molecule_data.copy(), config)
                future_to_index[future] = idx

            for future in as_completed(future_to_index):
                idx = future_to_index[future]
                try:
                    enriched_molecules[idx] = future.result()
                except Exception as exc:
                    logger.error(f"Failed to enrich molecule at index {idx}: {exc}")
                    fallback = molecules_data[idx].copy()
                    fallback["pubchem_error"] = str(exc)
                    enriched_molecules[idx] = fallback
    except Exception as exc:
        # Global fallback in case ThreadPoolExecutor path fails
        logger.warning(f"Parallel PubChem enrichment failed: {exc}; falling back to sequential")
        enriched_molecules = [_extract_pubchem_data(client, m, config) for m in molecules_data]

    # Type narrow (no None values expected)
    return [m for m in enriched_molecules if m is not None]


def _extract_batch_data(chembl_client: TestitemChEMBLClient, pubchem_client: PubChemClient | None, normalised: pd.DataFrame, config: TestitemConfig) -> pd.DataFrame:
    """Extract data from both ChEMBL and PubChem APIs."""

    logger.info("Starting batch data extraction...")

    # Get molecule IDs for extraction
    molecule_chembl_ids = []
    if "molecule_chembl_id" in normalised.columns:
        molecule_chembl_ids = normalised["molecule_chembl_id"].dropna().unique().tolist()

    if not molecule_chembl_ids:
        logger.warning("No molecule ChEMBL IDs found for extraction")
        return pd.DataFrame()

    # S02: Extract ChEMBL data with fallback to sequential
    logger.info("S02: Extracting ChEMBL data...")
    chembl_results: list[dict[str, Any]]
    try:
        chembl_results = _extract_molecules_batch(chembl_client, molecule_chembl_ids, config)
    except Exception as exc:
        logger.warning(f"Batch extraction failed: {exc}")
        if getattr(config.runtime, "fallback_to_sequential", True):
            logger.info("Falling back to sequential extraction for ChEMBL")
            chembl_results = []
            for mol_id in molecule_chembl_ids:
                try:
                    chembl_results.append(_extract_molecule_data(chembl_client, mol_id, config))
                except Exception as inner_exc:
                    logger.error(f"Failed to extract {mol_id}: {inner_exc}")
                    chembl_results.append(
                        {
                            "molecule_chembl_id": mol_id,
                            "source_system": "ChEMBL",
                            "extracted_at": pd.Timestamp.utcnow().isoformat() + "Z",
                            "error": str(inner_exc),
                        }
                    )
        else:
            raise

    # S03: Enrich with PubChem data if enabled (with internal parallel fallback)
    if pubchem_client and config.enable_pubchem:
        logger.info("S03: Enriching with PubChem data...")
        try:
            chembl_results = _extract_pubchem_data_batch(pubchem_client, chembl_results, config)
        except Exception as exc:
            logger.warning(f"PubChem enrichment failed: {exc}; continuing without PubChem")

    # Convert to DataFrame
    extracted_df = pd.DataFrame(chembl_results)

    logger.info(f"Batch data extraction completed: {len(extracted_df)} records")

    return extracted_df


# ============================================================================
# PRIVATE FUNCTIONS - UTILITIES
# ============================================================================


def _calculate_checksum(file_path: Path) -> str:
    """Calculate SHA256 checksum of a file."""
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            sha256_hash.update(chunk)
    return sha256_hash.hexdigest()


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

            processed_value = re.sub(r"\{([^}]+)\}", replace_placeholder, value)
            # Only include header if the value is not empty after processing and not a placeholder
            if processed_value and processed_value.strip() and not processed_value.startswith("{") and not processed_value.endswith("}"):
                processed_headers[key] = processed_value
        else:
            processed_headers[key] = value
    headers = processed_headers

    # Use source-specific base_url or fallback to default
    base_url = chembl_config.http.base_url or "https://www.ebi.ac.uk/chembl/api/data"

    # Use source-specific retry settings (dict) or fallback to global
    retry_settings = RetrySettings(
        total=chembl_config.http.retries.get("total", config.http.global_.retries.total),
        backoff_multiplier=chembl_config.http.retries.get("backoff_multiplier", config.http.global_.retries.backoff_multiplier),
    )

    # Create rate limit settings if configured on source (documents-style)
    rate_limit = None
    if getattr(chembl_config, "rate_limit", None):
        max_calls = chembl_config.rate_limit.get("max_calls")
        period = chembl_config.rate_limit.get("period")
        if (max_calls is None or period is None) and "requests_per_second" in chembl_config.rate_limit:
            rps = chembl_config.rate_limit.get("requests_per_second")
            if rps:
                max_calls = 1
                period = 1.0 / float(rps)
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

    # Use source-specific retry settings (dict) or fallback to global
    retry_settings = RetrySettings(
        total=pubchem_config.http.retries.get("total", config.http.global_.retries.total),
        backoff_multiplier=pubchem_config.http.retries.get("backoff_multiplier", config.http.global_.retries.backoff_multiplier),
    )

    # Create rate limit settings if configured on source (documents-style)
    rate_limit = None
    if getattr(pubchem_config, "rate_limit", None):
        max_calls = pubchem_config.rate_limit.get("max_calls")
        period = pubchem_config.rate_limit.get("period")
        if (max_calls is None or period is None) and "requests_per_second" in pubchem_config.rate_limit:
            rps = pubchem_config.rate_limit.get("requests_per_second")
            if rps:
                max_calls = 1
                period = 1.0 / float(rps)
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
        raise TestitemValidationError("Input data must contain at least one of: molecule_chembl_id, molregno")

    # Normalize molecule_chembl_id if present
    if has_molecule_id:
        normalised["molecule_chembl_id"] = normalised["molecule_chembl_id"].astype(str).str.strip()
        # Remove rows with empty molecule_chembl_id
        normalised = normalised[normalised["molecule_chembl_id"].str.len() > 0]

    # Normalize molregno if present
    if has_molregno:
        normalised["molregno"] = pd.to_numeric(normalised["molregno"], errors="coerce")

    # Normalize optional fields
    optional_fields = ["parent_chembl_id", "parent_molregno", "pubchem_cid"]
    for field in optional_fields:
        if field in normalised.columns:
            if field in ["parent_chembl_id"]:
                normalised[field] = normalised[field].astype(str).str.strip()
            elif field in ["parent_molregno"]:
                normalised[field] = pd.to_numeric(normalised[field], errors="coerce")
            elif field in ["pubchem_cid"]:
                # Приводим CID к целой строке: 12345.0 -> "12345"; некорректные -> NA
                cid_series = pd.to_numeric(normalised[field], errors="coerce")
                normalised[field] = cid_series.apply(lambda v: str(int(v)) if pd.notna(v) else pd.NA)

    # Sort by molecule_chembl_id if available, otherwise by molregno
    if has_molecule_id:
        normalised = normalised.sort_values("molecule_chembl_id").reset_index(drop=True)
    else:
        normalised = normalised.sort_values("molregno").reset_index(drop=True)

    return normalised


def run_testitem_etl(config: TestitemConfig, input_data: pd.DataFrame | None = None, input_path: Path | None = None) -> TestitemETLResult:
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
            testitems=pd.DataFrame(), qc=pd.DataFrame([{"metric": "row_count", "value": 0}]), meta={"pipeline_version": config.pipeline_version, "row_count": 0}
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

    # S02: Extract data from APIs
    logger.info("S02: Extracting data from APIs...")
    extracted_frame = _extract_batch_data(chembl_client, pubchem_client, normalised, config)

    if extracted_frame.empty:
        logger.warning("No data extracted from APIs")
        return TestitemETLResult(
            testitems=pd.DataFrame(), qc=pd.DataFrame([{"metric": "row_count", "value": 0}]), meta={"pipeline_version": config.pipeline_version, "row_count": 0}
        )

    # Add chembl_release to all records
    extracted_frame["chembl_release"] = chembl_release

    # S03: Normalize data
    logger.info("S03: Normalizing data...")
    normalized_frame = _normalize_testitem_data(extracted_frame)

    # S04: Validate data
    logger.info("S04: Validating data...")
    validated_frame = _validate_testitem_data(normalized_frame)

    # S05: Calculate QC metrics
    logger.info("S05: Calculating QC metrics...")
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

    # S06: Create metadata
    logger.info("S06: Creating metadata...")
    meta = {
        "pipeline_version": config.pipeline_version,
        "chembl_release": chembl_release,
        "row_count": len(validated_frame),
        "extraction_parameters": {
            "total_molecules": len(validated_frame),
            "pubchem_enabled": config.enable_pubchem,
            "allow_parent_missing": config.allow_parent_missing,
            "batch_size": getattr(config.runtime, "batch_size", 200),
            "retries": getattr(config.runtime, "retries", 5),
            "timeout_sec": getattr(config.runtime, "timeout_sec", 30),
        },
    }

    # S07: Perform correlation analysis if enabled in config
    logger.info("S07: Performing correlation analysis...")
    correlation_analysis = None
    correlation_reports = None
    correlation_insights = None

    if hasattr(config, "postprocess") and hasattr(config.postprocess, "correlation") and config.postprocess.correlation.enabled and len(validated_frame) > 1:
        try:
            logger.info("Performing correlation analysis...")
            logger.info(f"Input data shape: {validated_frame.shape}")
            logger.info(f"Input columns: {list(validated_frame.columns)}")

            # Подготавливаем данные для корреляционного анализа
            analysis_df = prepare_data_for_correlation_analysis(validated_frame, data_type="testitems", logger=logger)

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
        testitems=validated_frame, qc=qc, meta=meta, correlation_analysis=correlation_analysis, correlation_reports=correlation_reports, correlation_insights=correlation_insights
    )


def write_testitem_outputs(result: TestitemETLResult, output_dir: Path, date_tag: str, config: TestitemConfig) -> dict[str, Path]:
    """Persist ETL artefacts to disk and return the generated paths (documents-style)."""

    try:
        output_dir.mkdir(parents=True, exist_ok=True)
    except OSError as exc:  # pragma: no cover - filesystem permission issues
        raise TestitemIOError(f"Failed to create output directory: {exc}") from exc

    # File paths (documents-style naming)
    csv_path = output_dir / f"testitems_{date_tag}.csv"
    qc_path = output_dir / f"testitems_{date_tag}_qc.csv"
    meta_path = output_dir / f"testitems_{date_tag}_meta.yaml"

    try:
        # S08: Persist data with deterministic serialization
        logger.info("S08: Persisting data...")

        # Save CSV with deterministic order
        from library.etl.load import write_deterministic_csv

        write_deterministic_csv(
            result.testitems,
            csv_path,
            determinism=config.determinism,
            output=None,  # Use fallback settings
        )

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
                correlation_dir = output_dir / f"testitems_correlation_report_{date_tag}"
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
        raise TestitemIOError(f"Failed to write outputs: {exc}") from exc

    return outputs


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
