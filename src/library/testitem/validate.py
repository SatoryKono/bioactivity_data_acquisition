"""Data validation for testitem records using Pandera schemas."""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd
import pandera.pandas as pa
from pandera import Check, Column, DataFrameSchema

from library.schemas.testitem_schema import (
    TestitemInputSchema,
    TestitemNormalizedSchema,
    TestitemRawSchema,
)

logger = logging.getLogger(__name__)


class TestitemValidator:
    """Validates testitem data using Pandera schemas."""

    def __init__(self, config: dict[str, Any] | None = None):
        """Initialize validator with configuration."""
        self.config = config or {}
        self.strict_mode = self.config.get('validation', {}).get('strict', True)

    def get_raw_schema(self) -> DataFrameSchema:
        """Schema for raw testitem data from APIs."""
        return DataFrameSchema({
            # Required fields
            "source_system": Column(
                pa.String,
                nullable=False,
                description="Data source identifier"
            ),
            "extracted_at": Column(
                pa.String,
                nullable=False,
                description="Timestamp when data was retrieved"
            ),
            
            # Core molecule fields
            "molecule_chembl_id": Column(
                pa.String,
                nullable=True,
                checks=[
                    Check.str_matches(r"^CHEMBL\d+$", name="chembl_id_format")
                ],
                description="ChEMBL molecule identifier"
            ),
            "molregno": Column(
                pa.Int64,
                nullable=True,
                checks=[
                    Check.greater_than(0, name="molregno_positive")
                ],
                description="ChEMBL molecule registry number"
            ),
            "pref_name": Column(
                pa.String,
                nullable=True,
                description="Preferred name"
            ),
            
            # Parent/child relationship fields
            "parent_chembl_id": Column(
                pa.String,
                nullable=True,
                checks=[
                    Check.str_matches(r"^CHEMBL\d+$", name="chembl_id_format")
                ],
                description="Parent ChEMBL molecule identifier"
            ),
            "parent_molregno": Column(
                pa.Int64,
                nullable=True,
                checks=[
                    Check.greater_than(0, name="parent_molregno_positive")
                ],
                description="Parent ChEMBL molecule registry number"
            ),
            
            # Drug development fields
            "max_phase": Column(
                pa.Int64,
                nullable=True,
                checks=[
                    Check.greater_than_or_equal_to(0, name="max_phase_min"),
                    Check.less_than_or_equal_to(4, name="max_phase_max")
                ],
                description="Maximum development phase"
            ),
            "therapeutic_flag": Column(
                pa.Boolean,
                nullable=True,
                description="Therapeutic flag"
            ),
            "dosed_ingredient": Column(
                pa.Boolean,
                nullable=True,
                description="Dosed ingredient flag"
            ),
            "first_approval": Column(
                pa.Int64,
                nullable=True,
                checks=[
                    Check.greater_than_or_equal_to(1900, name="first_approval_min"),
                    Check.less_than_or_equal_to(2030, name="first_approval_max")
                ],
                description="First approval year"
            ),
            
            # Structure and properties fields
            "structure_type": Column(
                pa.String,
                nullable=True,
                description="Structure type"
            ),
            "molecule_type": Column(
                pa.String,
                nullable=True,
                description="Molecule type"
            ),
            "mw_freebase": Column(
                pa.Float64,
                nullable=True,
                checks=[
                    Check.greater_than(0, name="mw_positive")
                ],
                description="Molecular weight (freebase)"
            ),
            "alogp": Column(
                pa.Float64,
                nullable=True,
                description="ALogP"
            ),
            "hba": Column(
                pa.Int64,
                nullable=True,
                checks=[
                    Check.greater_than_or_equal_to(0, name="hba_non_negative")
                ],
                description="Hydrogen bond acceptors"
            ),
            "hbd": Column(
                pa.Int64,
                nullable=True,
                checks=[
                    Check.greater_than_or_equal_to(0, name="hbd_non_negative")
                ],
                description="Hydrogen bond donors"
            ),
            "psa": Column(
                pa.Float64,
                nullable=True,
                checks=[
                    Check.greater_than_or_equal_to(0, name="psa_non_negative")
                ],
                description="Polar surface area"
            ),
            "rtb": Column(
                pa.Int64,
                nullable=True,
                checks=[
                    Check.greater_than_or_equal_to(0, name="rtb_non_negative")
                ],
                description="Rotatable bonds"
            ),
            
            # PubChem fields
            "pubchem_cid": Column(
                pa.Int64,
                nullable=True,
                checks=[
                    Check.greater_than(0, name="pubchem_cid_positive")
                ],
                description="PubChem compound identifier"
            ),
            "pubchem_molecular_formula": Column(
                pa.String,
                nullable=True,
                description="PubChem molecular formula"
            ),
            "pubchem_molecular_weight": Column(
                pa.Float64,
                nullable=True,
                checks=[
                    Check.greater_than(0, name="pubchem_mw_positive")
                ],
                description="PubChem molecular weight"
            ),
            "pubchem_canonical_smiles": Column(
                pa.String,
                nullable=True,
                description="PubChem canonical SMILES"
            ),
            "pubchem_inchi": Column(
                pa.String,
                nullable=True,
                description="PubChem InChI"
            ),
            "pubchem_inchi_key": Column(
                pa.String,
                nullable=True,
                description="PubChem InChI key"
            ),
        })

    def get_normalized_schema(self) -> DataFrameSchema:
        """Schema for normalized testitem data after processing."""
        return DataFrameSchema({
            # Core fields
            "molecule_chembl_id": Column(
                pa.String,
                nullable=True,
                checks=[
                    Check.str_matches(r"^CHEMBL\d+$", name="chembl_id_format")
                ],
                description="ChEMBL molecule identifier"
            ),
            "molregno": Column(
                pa.Int64,
                nullable=True,
                description="ChEMBL molecule registry number"
            ),
            "pref_name": Column(
                pa.String,
                nullable=True,
                description="Preferred name"
            ),
            "pref_name_key": Column(
                pa.String,
                nullable=True,
                description="Preferred name key for sorting"
            ),
            
            # Computed fields
            "hash_business_key": Column(
                pa.String,
                nullable=True,
                description="Business key hash"
            ),
            "hash_row": Column(
                pa.String,
                nullable=True,
                description="Row hash for deduplication"
            ),
            
            # Standardized structure fields
            "standardized_inchi": Column(
                pa.String,
                nullable=True,
                description="Standardized InChI"
            ),
            "standardized_inchi_key": Column(
                pa.String,
                nullable=True,
                description="Standardized InChI key"
            ),
            "standardized_smiles": Column(
                pa.String,
                nullable=True,
                description="Standardized SMILES"
            ),
            
            # Molecular properties
            "mw_freebase": Column(
                pa.Float64,
                nullable=True,
                description="Molecular weight (freebase)"
            ),
            "alogp": Column(
                pa.Float64,
                nullable=True,
                description="ALogP"
            ),
            "hba": Column(
                pa.Int64,
                nullable=True,
                description="Hydrogen bond acceptors"
            ),
            "hbd": Column(
                pa.Int64,
                nullable=True,
                description="Hydrogen bond donors"
            ),
            "psa": Column(
                pa.Float64,
                nullable=True,
                description="Polar surface area"
            ),
            "rtb": Column(
                pa.Int64,
                nullable=True,
                description="Rotatable bonds"
            ),
        })

    def validate_input(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate input CSV data using input schema."""
        logger.info(f"Validating input testitem data: {len(df)} records")
        
        try:
            # Use input schema for CSV data
            schema = TestitemInputSchema.get_schema()
            validated_df = schema.validate(df, lazy=True)
            logger.info("Input data validation passed")
            return validated_df
        except Exception as exc:
            logger.error("Input data validation failed: %s", exc)
            raise TestitemValidationError(f"Input data validation failed: {exc}") from exc

    def validate_raw(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate raw input data using Pandera schema."""
        logger.info(f"Validating raw testitem data: {len(df)} records")
        
        try:
            # Use existing schema if available
            schema = TestitemRawSchema.get_schema()
            validated_df = schema.validate(df, lazy=True)
            logger.info("Raw data validation passed")
            return validated_df
        except Exception as exc:
            logger.error("Raw data validation failed: %s", exc)
            raise TestitemValidationError(f"Raw data validation failed: {exc}") from exc

    def validate_normalized(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate normalized data using Pandera schema."""
        logger.info(f"Validating normalized testitem data: {len(df)} records")
        
        try:
            # Use existing schema if available
            schema = TestitemNormalizedSchema.get_schema()
            validated_df = schema.validate(df, lazy=True)
            logger.info("Normalized data validation passed")
            return validated_df
        except Exception as exc:
            # Экранируем символы % в сообщениях об ошибках для безопасного логирования
            safe_exc = str(exc).replace('%', '%%')
            logger.error("Normalized data validation failed: %s", safe_exc)
            raise TestitemValidationError(f"Normalized data validation failed: {exc}") from exc

    def validate_business_rules(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate business rules for testitem data."""
        logger.info("Validating business rules")
        
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
        
        # Rule 7: RTB should be non-negative if present
        if "rtb" in validated_df.columns:
            invalid_rtb = (
                validated_df["rtb"].notna() & 
                (validated_df["rtb"] < 0)
            )
            
            if invalid_rtb.any():
                logger.warning(f"Found {invalid_rtb.sum()} records with invalid RTB")
        
        # Rule 8: Max phase should be between 0 and 4 if present
        if "max_phase" in validated_df.columns:
            invalid_phase = (
                validated_df["max_phase"].notna() & 
                ((validated_df["max_phase"] < 0) | (validated_df["max_phase"] > 4))
            )
            
            if invalid_phase.any():
                logger.warning(f"Found {invalid_phase.sum()} records with invalid max_phase")
        
        # Rule 9: First approval year should be reasonable if present
        if "first_approval" in validated_df.columns:
            invalid_approval = (
                validated_df["first_approval"].notna() & 
                ((validated_df["first_approval"] < 1900) | (validated_df["first_approval"] > 2030))
            )
            
            if invalid_approval.any():
                logger.warning(f"Found {invalid_approval.sum()} records with invalid first_approval year")
        
        # Rule 10: PubChem CID should be positive if present
        if "pubchem_cid" in validated_df.columns:
            invalid_cid = (
                validated_df["pubchem_cid"].notna() & 
                (validated_df["pubchem_cid"] <= 0)
            )
            
            if invalid_cid.any():
                logger.warning(f"Found {invalid_cid.sum()} records with invalid PubChem CID")
        
        return validated_df

    def validate_chembl_id_format(self, chembl_id: str | None) -> bool:
        """Validate ChEMBL ID format."""
        if not chembl_id or pd.isna(chembl_id):
            return True  # Empty ChEMBL ID is valid (nullable)
        
        # ChEMBL ID format: CHEMBL followed by digits
        chembl_pattern = r'^CHEMBL\d+$'
        return bool(pd.Series([chembl_id]).str.match(chembl_pattern).iloc[0])

    def validate_molecular_weight(self, mw: float | None) -> bool:
        """Validate molecular weight value."""
        if mw is None or pd.isna(mw):
            return True  # Empty MW is valid
        
        # Molecular weight should be positive and reasonable
        return 0 < mw < 10000  # Reasonable range for drug-like molecules

    def validate_alogp(self, alogp: float | None) -> bool:
        """Validate ALogP value."""
        if alogp is None or pd.isna(alogp):
            return True  # Empty ALogP is valid
        
        # ALogP should be within reasonable range
        return -10 <= alogp <= 10

    def validate_structure_format(self, structure: str | None, structure_type: str) -> bool:
        """Validate molecular structure format."""
        if not structure or pd.isna(structure):
            return True  # Empty structure is valid
        
        structure = str(structure).strip()
        
        if structure_type == "SMILES":
            # Basic SMILES validation - should contain common atoms and bonds
            valid_chars = set("CNOSPFClBrI()[]=#+-\\/")
            return all(c in valid_chars or c.isdigit() or c.islower() for c in structure)
        
        elif structure_type == "InChI":
            # Basic InChI validation - should start with "InChI="
            return structure.startswith("InChI=")
        
        elif structure_type == "InChIKey":
            # Basic InChIKey validation - should be 27 characters with hyphens
            return len(structure) == 27 and structure.count('-') == 2
        
        return True  # Unknown structure type, assume valid


class TestitemValidationError(Exception):
    """Raised when testitem validation fails."""
    pass
