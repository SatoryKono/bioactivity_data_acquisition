"""Data normalization stage for testitem ETL pipeline."""

from __future__ import annotations

import hashlib
import logging
from typing import Any

import pandas as pd

<<<<<<< Updated upstream
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
=======
from library.normalizers import get_normalizer
from library.schemas.testitem_schema import TestitemNormalizedSchema
from library.utils.empty_value_handler import (
    normalize_boolean_field,
    normalize_list_field,
    normalize_numeric_field,
    normalize_string_field,
)

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
        logger.info("Normalizing %d testitem records", len(df))

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

        logger.info("Normalization completed. Output: %d records", len(normalized_df))
        return normalized_df

    def _add_missing_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add missing columns that are expected by the schema but not present in the data."""

        # Define all columns that should be present in the normalized schema
        # Based on column_order from config_testitem.yaml
        required_columns = {
            # Основные идентификаторы и метаданные
            "molregno",
            "pref_name",
            "pref_name_key",
            "parent_chembl_id",
            "parent_molregno",
            "max_phase",
            "therapeutic_flag",
            "dosed_ingredient",
            "first_approval",
            "structure_type",
            "molecule_type",
            # Физико-химические свойства ChEMBL
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
            "molecular_species",
            "full_mwt",
            "aromatic_rings",
            "heavy_atoms",
            "qed_weighted",
            "mw_monoisotopic",
            "full_molformula",
            "hba_lipinski",
            "hbd_lipinski",
            "num_lipinski_ro5_violations",
            # Пути введения и флаги
            "oral",
            "parenteral",
            "topical",
            "black_box_warning",
            "natural_product",
            "first_in_class",
            "chirality",
            "prodrug",
            "inorganic_flag",
            "polymer_flag",
            # Регистрация и отзыв
            "usan_year",
            "availability_type",
            "usan_stem",
            "usan_substem",
            "usan_stem_definition",
            "indication_class",
            "withdrawn_flag",
            "withdrawn_year",
            "withdrawn_country",
            "withdrawn_reason",
            # Механизм действия
            "mechanism_of_action",
            "direct_interaction",
            "molecular_mechanism",
            # Drug данные
            "drug_chembl_id",
            "drug_name",
            "drug_type",
            "drug_substance_flag",
            "drug_indication_flag",
            "drug_antibacterial_flag",
            "drug_antiviral_flag",
            "drug_antifungal_flag",
            "drug_antiparasitic_flag",
            "drug_antineoplastic_flag",
            "drug_immunosuppressant_flag",
            "drug_antiinflammatory_flag",
            # PubChem данные
            "pubchem_cid",
            "pubchem_molecular_formula",
            "pubchem_molecular_weight",
            "pubchem_canonical_smiles",
            "pubchem_isomeric_smiles",
            "pubchem_inchi",
            "pubchem_inchi_key",
            "pubchem_registry_id",
            "pubchem_rn",
            # Стандартизированные структуры
            "standardized_inchi",
            "standardized_inchi_key",
            "standardized_smiles",
            # Вложенные структуры ChEMBL (JSON/распакованные)
            "atc_classifications",
            "biotherapeutic",
            "chemical_probe",
            "cross_references",
            "helm_notation",
            "molecule_hierarchy",
            "molecule_properties",
            "molecule_structures",
            "molecule_synonyms",
            "orphan",
            "veterinary",
            "standard_inchi",
            "chirality_chembl",
            "molecule_type_chembl",
            # Входные данные из input файла
            "nstereo",
            "salt_chembl_id",
            # Метаданные (будут добавлены в _add_system_metadata)
            "index",
            "pipeline_version",
            "source_system",
            "chembl_release",
            "extracted_at",
            "hash_row",
            "hash_business_key",
        }

        # Add missing columns with default values
        for column in required_columns:
            if column not in df.columns:
                # Для булевых полей используем False с правильным типом
                if column in [
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
                    "drug_substance_flag",
                    "drug_indication_flag",
                    "drug_antibacterial_flag",
                    "drug_antiviral_flag",
                    "drug_antifungal_flag",
                    "drug_antiparasitic_flag",
                    "drug_antineoplastic_flag",
                    "drug_immunosuppressant_flag",
                    "drug_antiinflammatory_flag",
                    "chemical_probe",
                    "orphan",
                    "veterinary",
                    "ro3_pass",
                ]:
                    df[column] = pd.Series(dtype="bool")
                # Для целочисленных полей используем Int64
                elif column in [
                    "molregno",
                    "parent_molregno",
                    "hba",
                    "hbd",
                    "rtb",
                    "num_ro5_violations",
                    "hba_lipinski",
                    "hbd_lipinski",
                    "num_lipinski_ro5_violations",
                    "chirality",
                    "usan_year",
                    "withdrawn_year",
                    "pubchem_cid",
                    "nstereo",
                ]:
                    df[column] = pd.Series(dtype="Int64")
                # Для float полей используем Float64
                elif column in [
                    "max_phase",
                    "mw_freebase",
                    "alogp",
                    "psa",
                    "acd_most_apka",
                    "acd_most_bpka",
                    "acd_logp",
                    "acd_logd",
                    "full_mwt",
                    "aromatic_rings",
                    "heavy_atoms",
                    "qed_weighted",
                    "mw_monoisotopic",
                    "pubchem_molecular_weight",
                ]:
                    df[column] = pd.Series(dtype="Float64")
                # Для строковых полей используем pd.NA с правильным типом
                else:
                    df[column] = pd.Series(dtype="string")

        return df

    def _normalize_molecule_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize molecule data after extraction."""

        logger.info("Normalizing molecule data...")

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
            "molecular_mechanism",
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
            # Распакованные поля из вложенных ChEMBL структур
            "atc_classifications",
            "biotherapeutic",
            "chirality_chembl",
            "cross_references",
            "helm_notation",
            "molecule_type_chembl",
        ]

        for field in string_fields:
            if field in normalized_df.columns:
                normalized_df[field] = normalized_df[field].apply(normalize_string_field)

        # Integer fields (should be Int64)
        integer_fields = [
            "molregno",
            "parent_molregno",
            "hba",
            "hbd",
            "rtb",
            "num_ro5_violations",
            "hba_lipinski",
            "hbd_lipinski",
            "num_lipinski_ro5_violations",
            "first_approval",
            "usan_year",
            "withdrawn_year",
            "pubchem_cid",
            "chirality",
        ]

        for field in integer_fields:
            if field in normalized_df.columns:
                normalized_df[field] = normalized_df[field].apply(normalize_numeric_field)
                # Конвертируем в Int64, сохраняя NaN значения
                normalized_df[field] = normalized_df[field].astype("Int64")

        # Float fields that should be float64 (not Int64)
        float_fields_special = ["max_phase", "aromatic_rings", "heavy_atoms"]

        for field in float_fields_special:
            if field in normalized_df.columns:
                normalized_df[field] = normalized_df[field].apply(normalize_numeric_field)
                # Конвертируем в float64
                normalized_df[field] = normalized_df[field].astype("float64")

        # Float fields (should be Float64)
        float_fields = [
            "mw_freebase",
            "alogp",
            "psa",
            "acd_most_apka",
            "acd_most_bpka",
            "acd_logp",
            "acd_logd",
            "full_mwt",
            "qed_weighted",
            "mw_monoisotopic",
            "pubchem_molecular_weight",
        ]

        for field in float_fields:
            if field in normalized_df.columns:
                normalized_df[field] = normalized_df[field].apply(normalize_numeric_field)
                # Конвертируем в Float64
                normalized_df[field] = normalized_df[field].astype("Float64")

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
            "drug_substance_flag",
            "drug_indication_flag",
            "drug_antibacterial_flag",
            "drug_antiviral_flag",
            "drug_antifungal_flag",
            "drug_antiparasitic_flag",
            "drug_antineoplastic_flag",
            "drug_immunosuppressant_flag",
            "drug_antiinflammatory_flag",
            "ro3_pass",
            # Распакованные булевы поля из вложенных ChEMBL структур
            "chemical_probe",
            "orphan",
            "veterinary",
        ]

        for field in boolean_fields:
            if field in normalized_df.columns:
                normalized_df[field] = normalized_df[field].apply(normalize_boolean_field)
                # Конвертируем в bool тип (не boolean) - заменяем NaN на False
                normalized_df[field] = normalized_df[field].fillna(False).astype("bool")

        # List fields
        list_fields = ["synonyms", "pubchem_synonyms", "drug_warnings", "xref_sources"]

        for field in list_fields:
            if field in normalized_df.columns:
                normalized_df[field] = normalized_df[field].apply(normalize_list_field)

        # chirality уже обработано в integer_fields выше

        # Special handling for availability_type - должно быть строкой
        if "availability_type" in normalized_df.columns:
            normalized_df["availability_type"] = normalized_df["availability_type"].apply(lambda x: str(x) if x is not None and not pd.isna(x) else None)

        # Create pref_name_key for sorting
        if "pref_name" in normalized_df.columns:
            normalized_df["pref_name_key"] = normalized_df["pref_name"].apply(lambda x: str(x).lower().strip() if x is not None else "")

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
        """Add system metadata columns using unified utility."""
        from library.common.metadata_fields import (
            add_system_metadata_fields,
            create_chembl_client_from_config,
        )

        logger.info("Adding system metadata columns...")

        # Создаем ChEMBL клиент для получения версии
        chembl_client = create_chembl_client_from_config(self.config)

        # Используем унифицированную утилиту
        return add_system_metadata_fields(df, self.config, chembl_client)

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
                    logger.debug("Normalizing column '%s' with functions: %s", column_name, norm_funcs)

                    # Применяем функции нормализации в порядке
                    for func_name in norm_funcs:
                        try:
                            func = get_normalizer(func_name)
                            df[column_name] = df[column_name].apply(func)
                        except Exception as e:
                            logger.warning("Failed to apply normalizer '%s' to column '%s': %s", func_name, column_name, e)

        return df
>>>>>>> Stashed changes
