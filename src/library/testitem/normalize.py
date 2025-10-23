"""Data normalization for testitem records."""

from __future__ import annotations

import hashlib
import logging
from typing import Any

import pandas as pd

from library.normalizers import get_normalizer
from library.schemas.testitem_schema_normalized import TestitemNormalizedSchema
from library.utils.empty_value_handler import normalize_boolean_field
from library.utils.empty_value_handler import normalize_list_field
from library.utils.empty_value_handler import normalize_numeric_field
from library.utils.empty_value_handler import normalize_string_field

logger = logging.getLogger(__name__)


class TestitemNormalizer:
    """Normalizes testitem data according to business rules."""

    def __init__(self, config: dict[str, Any] | None = None):
        """Initialize normalizer with configuration."""
        self.config = config or {}

    def normalize_testitems(self, df: pd.DataFrame) -> pd.DataFrame:
        """Нормализация данных testitem согласно бизнес-правилам.
        
        Преобразует сырые данные testitem в нормализованный формат,
        добавляя вычисляемые поля и нормализуя значения.
        
        Args:
            df: DataFrame с валидированными сырыми данными testitem
            
        Returns:
            pd.DataFrame: Нормализованный DataFrame с дополнительными полями:
                - hash_business_key, hash_row: хеши для дедупликации
                - pref_name_key: поле для сортировки
                - нормализованные молекулярные свойства
                
        Example:
            >>> normalizer = TestitemNormalizer(config)
            >>> normalized_df = normalizer.normalize_testitems(validated_df)
        """
        logger.info(f"Normalizing {len(df)} testitem records")
        
        # Create a copy to avoid modifying original
        normalized_df = df.copy()
        
        # Apply schema-based normalization first
        normalized_df = self._apply_schema_normalizations(normalized_df)
        
        # Step 1: Add missing columns
        normalized_df = self._add_missing_columns(normalized_df)
        
        # Step 2: Normalize field types and values
        normalized_df = self._normalize_molecule_data(normalized_df)
        
        # Step 3: Standardize molecular structures
        normalized_df = self._standardize_structures(normalized_df)
        
        # Step 4: Add system metadata columns
        normalized_df = self._add_system_metadata(normalized_df)
        
        # Step 5: Add hash fields
        normalized_df = self._add_hash_fields(normalized_df)
        
        logger.info(f"Normalization completed. Output: {len(normalized_df)} records")
        return normalized_df

    def _add_missing_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add missing columns that are expected by the schema but not present in the data."""
        
        # Define all columns that should be present in the normalized schema
        required_columns = {
            "chembl_release", "parent_chembl_id", "parent_molregno", 
            "mechanism_of_action", "direct_interaction", "molecular_mechanism",
            "pubchem_registry_id", "pubchem_rn",
            # Распакованные поля из вложенных ChEMBL структур
            "atc_classifications", "biotherapeutic", "chemical_probe", 
            "chirality_chembl", "cross_references", "helm_notation",
            "molecule_type_chembl", "orphan", "veterinary"
        }
        
        # Add missing columns with default values
        for column in required_columns:
            if column not in df.columns:
                df[column] = pd.NA
        
        return df

    def _normalize_molecule_data(self, df: pd.DataFrame) -> pd.DataFrame:
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
            "standardized_inchi", "standardized_inchi_key", "standardized_smiles",
            # Распакованные поля из вложенных ChEMBL структур
            "atc_classifications", "biotherapeutic", "chirality_chembl", 
            "cross_references", "helm_notation", "molecule_type_chembl"
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
            "drug_immunosuppressant_flag", "drug_antiinflammatory_flag",
            # Распакованные булевы поля из вложенных ChEMBL структур
            "chemical_probe", "orphan", "veterinary"
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

    def _standardize_structures(self, df: pd.DataFrame) -> pd.DataFrame:
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

    def _add_system_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add system metadata columns."""
        from datetime import datetime
        
        logger.info("Adding system metadata columns...")
        
        df_with_metadata = df.copy()
        
        # Index - порядковый номер записи
        df_with_metadata['index'] = range(len(df))
        
        # Pipeline version
        df_with_metadata['pipeline_version'] = self.config.get('pipeline', {}).get('version', '2.0.0')
        
        # Source system
        df_with_metadata['source_system'] = 'ChEMBL'
        
        # ChEMBL release - из конфига или None
        chembl_release = self.config.get('sources', {}).get('chembl', {}).get('release', None)
        df_with_metadata['chembl_release'] = chembl_release
        
        # Extracted at - current UTC time
        df_with_metadata['extracted_at'] = datetime.utcnow().isoformat() + 'Z'
        
        logger.info("System metadata columns added successfully")
        
        return df_with_metadata

    def _add_hash_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add hash fields for deduplication and integrity checking."""
        
        logger.info("Adding hash fields...")
        
        df_with_hashes = df.copy()
        
        # Calculate business key hash
        df_with_hashes["hash_business_key"] = df_with_hashes.apply(self._calculate_business_key_hash, axis=1)
        
        # Calculate row hash
        df_with_hashes["hash_row"] = df_with_hashes.apply(self._calculate_row_hash, axis=1)
        
        logger.info("Hash fields added successfully")
        
        return df_with_hashes

    def _calculate_business_key_hash(self, row: pd.Series) -> str:
        """Calculate hash for business key (molecule_chembl_id)."""
        business_key = row.get("molecule_chembl_id")
        if business_key is None or pd.isna(business_key):
            return "unknown"
        
        return hashlib.sha256(str(business_key).encode()).hexdigest()

    def _calculate_row_hash(self, row: pd.Series) -> str:
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
    
    def _apply_schema_normalizations(self, df: pd.DataFrame) -> pd.DataFrame:
        """Применяет функции нормализации из схемы к DataFrame.
        
        Args:
            df: DataFrame для нормализации
            
        Returns:
            DataFrame с примененными нормализациями
        """
        logger.info("Applying schema-based normalizations")
        
        # Получаем схему
        schema = TestitemNormalizedSchema.get_schema()
        
        # Применяем нормализацию к каждой колонке
        for column_name, column_schema in schema.columns.items():
            if column_name in df.columns:
                norm_funcs = column_schema.metadata.get("normalization_functions", [])
                if norm_funcs:
                    logger.debug(f"Normalizing column '{column_name}' with functions: {norm_funcs}")
                    
                    # Применяем функции нормализации в порядке
                    for func_name in norm_funcs:
                        try:
                            func = get_normalizer(func_name)
                            df[column_name] = df[column_name].apply(func)
                        except Exception as e:
                            logger.warning(f"Failed to apply normalizer '{func_name}' to column '{column_name}': {e}")
        
        return df
