"""Data normalization stage for testitem ETL pipeline."""

from __future__ import annotations

import hashlib
import logging
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)


def normalize_string_field(value: Any) -> str | None:
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


def normalize_numeric_field(value: Any) -> float | None:
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


def normalize_boolean_field(value: Any) -> bool | None:
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


def normalize_list_field(value: Any) -> list[str] | None:
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
                return normalize_list_field(parsed)
        except (json.JSONDecodeError, TypeError):
            pass
        
        # Split by common delimiters
        items = [item.strip() for item in value.replace(";", ",").split(",") if item.strip()]
        return items if items else None
    
    return None


def normalize_molecule_data(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize molecule data after extraction."""
    
    logger.info("Normalizing molecule data...")
    
    normalized_df = df.copy()
    
    # String fields
    string_fields = [
        "molecule_chembl_id", "pref_name", "structure_type", "molecule_type",
        "parent_chembl_id", "mechanism_of_action", "mechanism_comment",
        "target_chembl_id", "drug_chembl_id", "drug_name", "drug_type",
        "usan_stem", "usan_substem", "usan_stem_definition", "indication_class",
        "withdrawn_country", "withdrawn_reason",
        "pubchem_molecular_formula", "pubchem_canonical_smiles", 
        "pubchem_isomeric_smiles", "pubchem_inchi", "pubchem_inchi_key",
        "pubchem_registry_id", "pubchem_rn",
        "standardized_inchi", "standardized_inchi_key", "standardized_smiles"
    ]
    
    for field in string_fields:
        if field in normalized_df.columns:
            normalized_df[field] = normalized_df[field].apply(normalize_string_field)
    
    # Numeric fields
    numeric_fields = [
        "molregno", "parent_molregno", "max_phase", "mw_freebase", "alogp",
        "hba", "hbd", "psa", "rtb", "ro3_pass", "num_ro5_violations",
        "acd_most_apka", "acd_most_bpka", "acd_logp", "acd_logd",
        "full_mwt", "aromatic_rings", "heavy_atoms", "qed_weighted",
        "mw_monoisotopic", "hba_lipinski", "hbd_lipinski", 
        "num_lipinski_ro5_violations", "first_approval", "usan_year",
        "withdrawn_year", "pubchem_molecular_weight", "pubchem_cid"
    ]
    
    for field in numeric_fields:
        if field in normalized_df.columns:
            normalized_df[field] = normalized_df[field].apply(normalize_numeric_field)
    
    # Boolean fields
    boolean_fields = [
        "therapeutic_flag", "dosed_ingredient", "oral", "parenteral", "topical",
        "black_box_warning", "natural_product", "first_in_class", "prodrug",
        "inorganic_flag", "polymer_flag", "withdrawn_flag", "direct_interaction",
        "molecular_mechanism", "drug_substance_flag", "drug_indication_flag",
        "drug_antibacterial_flag", "drug_antiviral_flag", "drug_antifungal_flag",
        "drug_antiparasitic_flag", "drug_antineoplastic_flag",
        "drug_immunosuppressant_flag", "drug_antiinflammatory_flag"
    ]
    
    for field in boolean_fields:
        if field in normalized_df.columns:
            normalized_df[field] = normalized_df[field].apply(normalize_boolean_field)
    
    # List fields
    list_fields = [
        "synonyms", "pubchem_synonyms", "drug_warnings", "xref_sources"
    ]
    
    for field in list_fields:
        if field in normalized_df.columns:
            normalized_df[field] = normalized_df[field].apply(normalize_list_field)
    
    # Special handling for chirality
    if "chirality" in normalized_df.columns:
        normalized_df["chirality"] = normalized_df["chirality"].apply(
            lambda x: normalize_string_field(x) if x is not None else None
        )
    
    # Special handling for availability_type
    if "availability_type" in normalized_df.columns:
        normalized_df["availability_type"] = normalized_df["availability_type"].apply(
            lambda x: normalize_numeric_field(x) if x is not None else None
        )
    
    # Create pref_name_key for sorting
    if "pref_name" in normalized_df.columns:
        normalized_df["pref_name_key"] = normalized_df["pref_name"].apply(
            lambda x: str(x).lower().strip() if x is not None else ""
        )
    
    logger.info("Molecule data normalization completed")
    
    return normalized_df


def calculate_business_key_hash(row: pd.Series) -> str:
    """Calculate hash for business key (molecule_chembl_id)."""
    business_key = row.get("molecule_chembl_id")
    if business_key is None or pd.isna(business_key):
        return "unknown"
    
    return hashlib.sha256(str(business_key).encode()).hexdigest()


def calculate_row_hash(row: pd.Series) -> str:
    """Calculate hash for entire row."""
    # Convert row to dictionary and sort keys for deterministic hashing
    row_dict = row.to_dict()
    
    # Remove hash fields from calculation to avoid circular dependency
    row_dict.pop("hash_row", None)
    row_dict.pop("hash_business_key", None)
    
    # Sort keys for deterministic ordering
    sorted_items = sorted(row_dict.items())
    
    # Create string representation
    row_str = str(sorted_items)
    
    return hashlib.sha256(row_str.encode()).hexdigest()


def add_hash_fields(df: pd.DataFrame) -> pd.DataFrame:
    """Add hash fields for deduplication and integrity checking."""
    
    logger.info("Adding hash fields...")
    
    df_with_hashes = df.copy()
    
    # Calculate business key hash
    df_with_hashes["hash_business_key"] = df_with_hashes.apply(calculate_business_key_hash, axis=1)
    
    # Calculate row hash
    df_with_hashes["hash_row"] = df_with_hashes.apply(calculate_row_hash, axis=1)
    
    logger.info("Hash fields added successfully")
    
    return df_with_hashes


def standardize_structures(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize molecular structures (InChI, SMILES)."""
    
    logger.info("Standardizing molecular structures...")
    
    df_standardized = df.copy()
    
    # For now, we'll use PubChem data as standardized if available
    # In a real implementation, you might use RDKit or other tools for standardization
    
    if "pubchem_inchi" in df_standardized.columns:
        df_standardized["standardized_inchi"] = df_standardized["pubchem_inchi"]
    
    if "pubchem_inchi_key" in df_standardized.columns:
        df_standardized["standardized_inchi_key"] = df_standardized["pubchem_inchi_key"]
    
    if "pubchem_canonical_smiles" in df_standardized.columns:
        df_standardized["standardized_smiles"] = df_standardized["pubchem_canonical_smiles"]
    
    logger.info("Molecular structure standardization completed")
    
    return df_standardized


def normalize_testitem_data(df: pd.DataFrame) -> pd.DataFrame:
    """Main normalization function for testitem data."""
    
    logger.info("Starting testitem data normalization...")
    
    # Step 1: Normalize field types and values
    normalized_df = normalize_molecule_data(df)
    
    # Step 2: Standardize molecular structures
    standardized_df = standardize_structures(normalized_df)
    
    # Step 3: Add hash fields
    final_df = add_hash_fields(standardized_df)
    
    logger.info("Testitem data normalization completed")
    
    return final_df
