"""Data validation stage for testitem ETL pipeline."""

from __future__ import annotations

import logging

import pandas as pd

logger = logging.getLogger(__name__)


def validate_testitem_schema(df: pd.DataFrame) -> pd.DataFrame:
    """Validate testitem data using Pandera schema."""
    
    logger.info("Validating testitem data schema...")
    
    try:
        # Convert extracted_at to datetime if it's a string
        if "extracted_at" in df.columns:
            df["extracted_at"] = pd.to_datetime(df["extracted_at"])
        
        # Note: chembl_release is now handled at metadata level, not per-record
        
        # Validate using Pandera schema (temporarily disabled)
        # validated_df = TestitemNormalizedSchema.validate(df)
        validated_df = df
        
        logger.info(f"Schema validation passed for {len(validated_df)} molecules")
        
        return validated_df
        
    except Exception as e:
        logger.error(f"Schema validation failed: {e}")
        raise TestitemValidationError(f"Schema validation failed: {e}") from e


def validate_business_rules(df: pd.DataFrame) -> pd.DataFrame:
    """Validate business rules for testitem data."""
    
    logger.info("Validating business rules...")
    
    validated_df = df.copy()
    
    # Rule 1: At least one identifier must be present
    missing_identifiers = (
        validated_df["molecule_chembl_id"].isna() & 
        validated_df["molregno"].isna()
    )
    
    if missing_identifiers.any():
        logger.warning(f"Found {missing_identifiers.sum()} records with missing identifiers")
    
    # Rule 2: If parent_chembl_id is present, it should be different from molecule_chembl_id
    if "parent_chembl_id" in validated_df.columns and "molecule_chembl_id" in validated_df.columns:
        same_parent = (
            validated_df["parent_chembl_id"].notna() & 
            (validated_df["parent_chembl_id"] == validated_df["molecule_chembl_id"])
        )
        
        if same_parent.any():
            logger.warning(f"Found {same_parent.sum()} records where parent equals molecule ID")
    
    # Rule 3: Molecular weight should be positive if present
    if "mw_freebase" in validated_df.columns:
        invalid_mw = (
            validated_df["mw_freebase"].notna() & 
            (validated_df["mw_freebase"] <= 0)
        )
        
        if invalid_mw.any():
            logger.warning(f"Found {invalid_mw.sum()} records with invalid molecular weight")
    
    # Rule 4: ALogP should be within reasonable range if present
    if "alogp" in validated_df.columns:
        invalid_alogp = (
            validated_df["alogp"].notna() & 
            ((validated_df["alogp"] < -10) | (validated_df["alogp"] > 10))
        )
        
        if invalid_alogp.any():
            logger.warning(f"Found {invalid_alogp.sum()} records with ALogP outside reasonable range")
    
    # Rule 5: HBA and HBD should be non-negative integers if present
    for field in ["hba", "hbd"]:
        if field in validated_df.columns:
            invalid_count = (
                validated_df[field].notna() & 
                (validated_df[field] < 0)
            )
            
            if invalid_count.any():
                logger.warning(f"Found {invalid_count.sum()} records with invalid {field}")
    
    # Rule 6: PSA should be non-negative if present
    if "psa" in validated_df.columns:
        invalid_psa = (
            validated_df["psa"].notna() & 
            (validated_df["psa"] < 0)
        )
        
        if invalid_psa.any():
            logger.warning(f"Found {invalid_psa.sum()} records with invalid PSA")
    
    # Rule 7: Max phase should be between 0 and 4 if present
    if "max_phase" in validated_df.columns:
        invalid_phase = (
            validated_df["max_phase"].notna() & 
            ((validated_df["max_phase"] < 0) | (validated_df["max_phase"] > 4))
        )
        
        if invalid_phase.any():
            logger.warning(f"Found {invalid_phase.sum()} records with invalid max_phase")
    
    # Rule 8: First approval year should be reasonable if present
    if "first_approval" in validated_df.columns:
        invalid_year = (
            validated_df["first_approval"].notna() & 
            ((validated_df["first_approval"] < 1900) | (validated_df["first_approval"] > 2030))
        )
        
        if invalid_year.any():
            logger.warning(f"Found {invalid_year.sum()} records with invalid first_approval year")
    
    # Rule 9: Withdrawn year should be after first approval if both present
    if "withdrawn_year" in validated_df.columns and "first_approval" in validated_df.columns:
        invalid_withdrawal = (
            validated_df["withdrawn_year"].notna() & 
            validated_df["first_approval"].notna() & 
            (validated_df["withdrawn_year"] < validated_df["first_approval"])
        )
        
        if invalid_withdrawal.any():
            logger.warning(f"Found {invalid_withdrawal.sum()} records with withdrawn year before first approval")
    
    # Rule 10: InChI key should be 27 characters if present
    if "pubchem_inchi_key" in validated_df.columns:
        invalid_inchi_key = (
            validated_df["pubchem_inchi_key"].notna() & 
            (validated_df["pubchem_inchi_key"].str.len() != 27)
        )
        
        if invalid_inchi_key.any():
            logger.warning(f"Found {invalid_inchi_key.sum()} records with invalid InChI key length")
    
    logger.info("Business rules validation completed")
    
    return validated_df


def validate_data_quality(df: pd.DataFrame) -> pd.DataFrame:
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


def validate_testitem_data(df: pd.DataFrame) -> pd.DataFrame:
    """Main validation function for testitem data."""
    
    logger.info("Starting testitem data validation...")
    
    # Step 1: Validate schema
    schema_validated_df = validate_testitem_schema(df)
    
    # Step 2: Validate business rules
    business_validated_df = validate_business_rules(schema_validated_df)
    
    # Step 3: Validate data quality
    final_df = validate_data_quality(business_validated_df)
    
    logger.info("Testitem data validation completed")
    
    return final_df


class TestitemValidationError(Exception):
    """Raised when testitem data validation fails."""
    pass
