"""Pandera schemas for testitem data validation."""

from __future__ import annotations

import importlib.util

from pandera.typing import Series

_PANDERA_PANDAS_SPEC = importlib.util.find_spec("pandera.pandas")
if _PANDERA_PANDAS_SPEC is not None:  # pragma: no cover - import side effect
    import pandera.pandas as pa  # type: ignore[no-redef]
else:  # pragma: no cover - import side effect
    import pandera as pa


class TestitemInputSchema(pa.DataFrameModel):
    """Schema for input testitem data from CSV files."""

    # Required fields - at least one must be present
    molecule_chembl_id: Series[str] = pa.Field(nullable=True, description="ChEMBL molecule identifier")
    molregno: Series[object] = pa.Field(nullable=True, description="ChEMBL molecule registry number")
    
    # Optional fields
    parent_chembl_id: Series[str] = pa.Field(nullable=True, description="Parent ChEMBL molecule identifier")
    parent_molregno: Series[object] = pa.Field(nullable=True, description="Parent ChEMBL molecule registry number")
    pubchem_cid: Series[object] = pa.Field(nullable=True, description="PubChem compound identifier")

    class Config:
        strict = True
        coerce = True


class TestitemRawSchema(pa.DataFrameModel):
    """Schema for raw testitem records fetched from APIs.
    
    This schema validates raw data from ChEMBL and PubChem APIs before normalization.
    """

    # Required fields - must be present in all records
    source_system: Series[str] = pa.Field(
        description="Data source identifier (e.g., 'ChEMBL', 'PubChem')",
        nullable=False
    )
    extracted_at: Series[str] = pa.Field(
        description="Timestamp when data was retrieved from API",
        nullable=False
    )
    
    # Core molecule fields - nullable=True because API may not always provide them
    molecule_chembl_id: Series[str] = pa.Field(
        description="ChEMBL molecule identifier",
        nullable=True
    )
    molregno: Series[object] = pa.Field(
        description="ChEMBL molecule registry number",
        nullable=True
    )
    pref_name: Series[str] = pa.Field(
        description="Preferred name",
        nullable=True
    )
    
    # Parent/child relationship fields
    parent_chembl_id: Series[str] = pa.Field(
        description="Parent ChEMBL molecule identifier",
        nullable=True
    )
    parent_molregno: Series[object] = pa.Field(
        description="Parent ChEMBL molecule registry number",
        nullable=True
    )
    
    # Drug development fields
    max_phase: Series[object] = pa.Field(
        description="Maximum development phase",
        nullable=True
    )
    therapeutic_flag: Series[object] = pa.Field(
        description="Therapeutic flag",
        nullable=True
    )
    dosed_ingredient: Series[object] = pa.Field(
        description="Dosed ingredient flag",
        nullable=True
    )
    first_approval: Series[object] = pa.Field(
        description="First approval year",
        nullable=True
    )
    
    # Structure and properties fields
    structure_type: Series[str] = pa.Field(
        description="Structure type",
        nullable=True
    )
    molecule_type: Series[str] = pa.Field(
        description="Molecule type",
        nullable=True
    )
    mw_freebase: Series[object] = pa.Field(
        description="Molecular weight (freebase)",
        nullable=True
    )
    alogp: Series[object] = pa.Field(
        description="ALogP",
        nullable=True
    )
    hba: Series[object] = pa.Field(
        description="Hydrogen bond acceptors",
        nullable=True
    )
    hbd: Series[object] = pa.Field(
        description="Hydrogen bond donors",
        nullable=True
    )
    psa: Series[object] = pa.Field(
        description="Polar surface area",
        nullable=True
    )
    rtb: Series[object] = pa.Field(
        description="Rotatable bonds",
        nullable=True
    )
    
    # Drug properties
    oral: Series[object] = pa.Field(
        description="Oral administration flag",
        nullable=True
    )
    parenteral: Series[object] = pa.Field(
        description="Parenteral administration flag",
        nullable=True
    )
    topical: Series[object] = pa.Field(
        description="Topical administration flag",
        nullable=True
    )
    black_box_warning: Series[object] = pa.Field(
        description="Black box warning flag",
        nullable=True
    )
    
    # Classification fields
    natural_product: Series[object] = pa.Field(
        description="Natural product flag",
        nullable=True
    )
    first_in_class: Series[object] = pa.Field(
        description="First in class flag",
        nullable=True
    )
    chirality: Series[object] = pa.Field(
        description="Chirality",
        nullable=True
    )
    prodrug: Series[object] = pa.Field(
        description="Prodrug flag",
        nullable=True
    )
    inorganic_flag: Series[object] = pa.Field(
        description="Inorganic flag",
        nullable=True
    )
    polymer_flag: Series[object] = pa.Field(
        description="Polymer flag",
        nullable=True
    )
    
    # USAN fields
    usan_year: Series[object] = pa.Field(
        description="USAN year",
        nullable=True
    )
    availability_type: Series[object] = pa.Field(
        description="Availability type",
        nullable=True
    )
    usan_stem: Series[str] = pa.Field(
        description="USAN stem",
        nullable=True
    )
    usan_substem: Series[str] = pa.Field(
        description="USAN substem",
        nullable=True
    )
    usan_stem_definition: Series[str] = pa.Field(
        description="USAN stem definition",
        nullable=True
    )
    indication_class: Series[str] = pa.Field(
        description="Indication class",
        nullable=True
    )
    
    # Withdrawal fields
    withdrawn_flag: Series[object] = pa.Field(
        description="Withdrawn flag",
        nullable=True
    )
    withdrawn_year: Series[object] = pa.Field(
        description="Withdrawn year",
        nullable=True
    )
    withdrawn_country: Series[str] = pa.Field(
        description="Withdrawn country",
        nullable=True
    )
    withdrawn_reason: Series[str] = pa.Field(
        description="Withdrawn reason",
        nullable=True
    )
    
    # PubChem fields
    pubchem_cid: Series[object] = pa.Field(
        description="PubChem compound identifier",
        nullable=True
    )
    pubchem_molecular_formula: Series[str] = pa.Field(
        description="PubChem molecular formula",
        nullable=True
    )
    pubchem_molecular_weight: Series[object] = pa.Field(
        description="PubChem molecular weight",
        nullable=True
    )
    pubchem_canonical_smiles: Series[str] = pa.Field(
        description="PubChem canonical SMILES",
        nullable=True
    )
    pubchem_isomeric_smiles: Series[str] = pa.Field(
        description="PubChem isomeric SMILES",
        nullable=True
    )
    pubchem_inchi: Series[str] = pa.Field(
        description="PubChem InChI",
        nullable=True
    )
    pubchem_inchi_key: Series[str] = pa.Field(
        description="PubChem InChI key",
        nullable=True
    )
    
    # Hash fields for deduplication
    hash_row: Series[str] = pa.Field(
        description="Row hash for deduplication",
        nullable=True
    )
    hash_business_key: Series[str] = pa.Field(
        description="Business key hash",
        nullable=True
    )

    class Config:
        strict = False  # Allow additional columns not defined in schema
        coerce = True  # Allow type coercion for data cleaning


class TestitemNormalizedSchema(pa.DataFrameModel):
    """Schema for normalized testitem data ready for export.
    
    This schema validates normalized data after ETL processing.
    All data should be clean and consistent for downstream analysis.
    """

    # Required fields - must be present in all normalized records
    source_system: Series[str] = pa.Field(
        description="Data source identifier (normalized)",
        nullable=True
    )
    extracted_at: Series[object] = pa.Field(
        description="Timestamp when data was retrieved from API",
        nullable=True
    )
    chembl_release: Series[str] = pa.Field(
        description="ChEMBL release version",
        nullable=True
    )
    
    # Core molecule fields - nullable=True for missing data
    molecule_chembl_id: Series[str] = pa.Field(
        description="ChEMBL molecule identifier (business key)",
        nullable=True
    )
    molregno: Series[object] = pa.Field(
        description="ChEMBL molecule registry number",
        nullable=True
    )
    pref_name: Series[str] = pa.Field(
        description="Preferred name",
        nullable=True
    )
    pref_name_key: Series[str] = pa.Field(
        description="Preferred name key for sorting",
        nullable=True
    )
    
    # Parent/child relationship fields
    parent_chembl_id: Series[str] = pa.Field(
        description="Parent ChEMBL molecule identifier",
        nullable=True
    )
    parent_molregno: Series[object] = pa.Field(
        description="Parent ChEMBL molecule registry number",
        nullable=True
    )
    
    # Drug development fields
    max_phase: Series[object] = pa.Field(
        description="Maximum development phase",
        nullable=True
    )
    therapeutic_flag: Series[object] = pa.Field(
        description="Therapeutic flag",
        nullable=True
    )
    dosed_ingredient: Series[object] = pa.Field(
        description="Dosed ingredient flag",
        nullable=True
    )
    first_approval: Series[object] = pa.Field(
        description="First approval year",
        nullable=True
    )
    
    # Structure and properties fields
    structure_type: Series[str] = pa.Field(
        description="Structure type",
        nullable=True
    )
    molecule_type: Series[str] = pa.Field(
        description="Molecule type",
        nullable=True
    )
    mw_freebase: Series[object] = pa.Field(
        description="Molecular weight (freebase)",
        nullable=True
    )
    alogp: Series[object] = pa.Field(
        description="ALogP",
        nullable=True
    )
    hba: Series[object] = pa.Field(
        description="Hydrogen bond acceptors",
        nullable=True
    )
    hbd: Series[object] = pa.Field(
        description="Hydrogen bond donors",
        nullable=True
    )
    psa: Series[object] = pa.Field(
        description="Polar surface area",
        nullable=True
    )
    rtb: Series[object] = pa.Field(
        description="Rotatable bonds",
        nullable=True
    )
    ro3_pass: Series[object] = pa.Field(
        description="Rule of 3 pass",
        nullable=True
    )
    num_ro5_violations: Series[object] = pa.Field(
        description="Number of Rule of 5 violations",
        nullable=True
    )
    acd_most_apka: Series[object] = pa.Field(
        description="ACD most acidic pKa",
        nullable=True
    )
    acd_most_bpka: Series[object] = pa.Field(
        description="ACD most basic pKa",
        nullable=True
    )
    acd_logp: Series[object] = pa.Field(
        description="ACD LogP",
        nullable=True
    )
    acd_logd: Series[object] = pa.Field(
        description="ACD LogD",
        nullable=True
    )
    molecular_species: Series[str] = pa.Field(
        description="Molecular species",
        nullable=True
    )
    full_mwt: Series[object] = pa.Field(
        description="Full molecular weight",
        nullable=True
    )
    aromatic_rings: Series[object] = pa.Field(
        description="Number of aromatic rings",
        nullable=True
    )
    heavy_atoms: Series[object] = pa.Field(
        description="Number of heavy atoms",
        nullable=True
    )
    qed_weighted: Series[object] = pa.Field(
        description="QED weighted score",
        nullable=True
    )
    mw_monoisotopic: Series[object] = pa.Field(
        description="Monoisotopic molecular weight",
        nullable=True
    )
    full_molformula: Series[str] = pa.Field(
        description="Full molecular formula",
        nullable=True
    )
    hba_lipinski: Series[object] = pa.Field(
        description="Lipinski HBA",
        nullable=True
    )
    hbd_lipinski: Series[object] = pa.Field(
        description="Lipinski HBD",
        nullable=True
    )
    num_lipinski_ro5_violations: Series[object] = pa.Field(
        description="Number of Lipinski Rule of 5 violations",
        nullable=True
    )
    
    # Drug properties
    oral: Series[object] = pa.Field(
        description="Oral administration flag",
        nullable=True
    )
    parenteral: Series[object] = pa.Field(
        description="Parenteral administration flag",
        nullable=True
    )
    topical: Series[object] = pa.Field(
        description="Topical administration flag",
        nullable=True
    )
    black_box_warning: Series[object] = pa.Field(
        description="Black box warning flag",
        nullable=True
    )
    
    # Classification fields
    natural_product: Series[object] = pa.Field(
        description="Natural product flag",
        nullable=True
    )
    first_in_class: Series[object] = pa.Field(
        description="First in class flag",
        nullable=True
    )
    chirality: Series[object] = pa.Field(
        description="Chirality",
        nullable=True
    )
    prodrug: Series[object] = pa.Field(
        description="Prodrug flag",
        nullable=True
    )
    inorganic_flag: Series[object] = pa.Field(
        description="Inorganic flag",
        nullable=True
    )
    polymer_flag: Series[object] = pa.Field(
        description="Polymer flag",
        nullable=True
    )
    
    # USAN fields
    usan_year: Series[object] = pa.Field(
        description="USAN year",
        nullable=True
    )
    availability_type: Series[object] = pa.Field(
        description="Availability type",
        nullable=True
    )
    usan_stem: Series[str] = pa.Field(
        description="USAN stem",
        nullable=True
    )
    usan_substem: Series[str] = pa.Field(
        description="USAN substem",
        nullable=True
    )
    usan_stem_definition: Series[str] = pa.Field(
        description="USAN stem definition",
        nullable=True
    )
    indication_class: Series[str] = pa.Field(
        description="Indication class",
        nullable=True
    )
    
    # Withdrawal fields
    withdrawn_flag: Series[object] = pa.Field(
        description="Withdrawn flag",
        nullable=True
    )
    withdrawn_year: Series[object] = pa.Field(
        description="Withdrawn year",
        nullable=True
    )
    withdrawn_country: Series[str] = pa.Field(
        description="Withdrawn country",
        nullable=True
    )
    withdrawn_reason: Series[str] = pa.Field(
        description="Withdrawn reason",
        nullable=True
    )
    
    # Mechanism fields
    mechanism_of_action: Series[str] = pa.Field(
        description="Mechanism of action",
        nullable=True
    )
    direct_interaction: Series[object] = pa.Field(
        description="Direct interaction flag",
        nullable=True
    )
    molecular_mechanism: Series[object] = pa.Field(
        description="Molecular mechanism flag",
        nullable=True
    )
    mechanism_comment: Series[str] = pa.Field(
        description="Mechanism comment",
        nullable=True
    )
    target_chembl_id: Series[str] = pa.Field(
        description="Target ChEMBL identifier",
        nullable=True
    )
    
    # ATC classification fields
    atc_level1: Series[str] = pa.Field(
        description="ATC level 1",
        nullable=True
    )
    atc_level1_description: Series[str] = pa.Field(
        description="ATC level 1 description",
        nullable=True
    )
    atc_level2: Series[str] = pa.Field(
        description="ATC level 2",
        nullable=True
    )
    atc_level2_description: Series[str] = pa.Field(
        description="ATC level 2 description",
        nullable=True
    )
    atc_level3: Series[str] = pa.Field(
        description="ATC level 3",
        nullable=True
    )
    atc_level3_description: Series[str] = pa.Field(
        description="ATC level 3 description",
        nullable=True
    )
    atc_level4: Series[str] = pa.Field(
        description="ATC level 4",
        nullable=True
    )
    atc_level4_description: Series[str] = pa.Field(
        description="ATC level 4 description",
        nullable=True
    )
    atc_level5: Series[str] = pa.Field(
        description="ATC level 5",
        nullable=True
    )
    atc_level5_description: Series[str] = pa.Field(
        description="ATC level 5 description",
        nullable=True
    )
    
    # Drug fields
    drug_chembl_id: Series[str] = pa.Field(
        description="Drug ChEMBL identifier",
        nullable=True
    )
    drug_name: Series[str] = pa.Field(
        description="Drug name",
        nullable=True
    )
    drug_type: Series[str] = pa.Field(
        description="Drug type",
        nullable=True
    )
    drug_substance_flag: Series[object] = pa.Field(
        description="Drug substance flag",
        nullable=True
    )
    drug_indication_flag: Series[object] = pa.Field(
        description="Drug indication flag",
        nullable=True
    )
    drug_antibacterial_flag: Series[object] = pa.Field(
        description="Drug antibacterial flag",
        nullable=True
    )
    drug_antiviral_flag: Series[object] = pa.Field(
        description="Drug antiviral flag",
        nullable=True
    )
    drug_antifungal_flag: Series[object] = pa.Field(
        description="Drug antifungal flag",
        nullable=True
    )
    drug_antiparasitic_flag: Series[object] = pa.Field(
        description="Drug antiparasitic flag",
        nullable=True
    )
    drug_antineoplastic_flag: Series[object] = pa.Field(
        description="Drug antineoplastic flag",
        nullable=True
    )
    drug_immunosuppressant_flag: Series[object] = pa.Field(
        description="Drug immunosuppressant flag",
        nullable=True
    )
    drug_antiinflammatory_flag: Series[object] = pa.Field(
        description="Drug antiinflammatory flag",
        nullable=True
    )
    
    # PubChem fields
    pubchem_cid: Series[object] = pa.Field(
        description="PubChem compound identifier",
        nullable=True
    )
    pubchem_molecular_formula: Series[str] = pa.Field(
        description="PubChem molecular formula",
        nullable=True
    )
    pubchem_molecular_weight: Series[object] = pa.Field(
        description="PubChem molecular weight",
        nullable=True
    )
    pubchem_canonical_smiles: Series[str] = pa.Field(
        description="PubChem canonical SMILES",
        nullable=True
    )
    pubchem_isomeric_smiles: Series[str] = pa.Field(
        description="PubChem isomeric SMILES",
        nullable=True
    )
    pubchem_inchi: Series[str] = pa.Field(
        description="PubChem InChI",
        nullable=True
    )
    pubchem_inchi_key: Series[str] = pa.Field(
        description="PubChem InChI key",
        nullable=True
    )
    pubchem_registry_id: Series[str] = pa.Field(
        description="PubChem registry ID",
        nullable=True
    )
    pubchem_rn: Series[str] = pa.Field(
        description="PubChem RN",
        nullable=True
    )
    pubchem_synonyms: Series[object] = pa.Field(
        description="PubChem synonyms (list)",
        nullable=True
    )
    synonyms: Series[object] = pa.Field(
        description="ChEMBL molecule synonyms (list)",
        nullable=True
    )
    
    # Standardized structure fields
    standardized_inchi: Series[str] = pa.Field(
        description="Standardized InChI",
        nullable=True
    )
    standardized_inchi_key: Series[str] = pa.Field(
        description="Standardized InChI key",
        nullable=True
    )
    standardized_smiles: Series[str] = pa.Field(
        description="Standardized SMILES",
        nullable=True
    )
    
    # Hash fields for deduplication
    hash_row: Series[str] = pa.Field(
        description="Row hash for deduplication",
        nullable=True
    )
    hash_business_key: Series[str] = pa.Field(
        description="Business key hash",
        nullable=True
    )
    molecule_form_chembl_id: Series[str] = pa.Field(
        description="ChEMBL molecule form identifier",
        nullable=True
    )
    xref_sources: Series[object] = pa.Field(
        description="Cross-reference sources",
        nullable=True
    )

    class Config:
        strict = False  # Allow additional columns not defined in schema
        coerce = True  # Allow type coercion for data cleaning


__all__ = ["TestitemInputSchema", "TestitemRawSchema", "TestitemNormalizedSchema"]
# import pandas as pd  # Unused after removing legacy validators
import pandera.pandas as pa
from pandera import Check, Column, DataFrameSchema


class TestitemInputSchema:
    """Схемы для входных данных теститемов."""

    @staticmethod
    def get_schema() -> DataFrameSchema:
        """Схема для входных данных теститемов."""
        return DataFrameSchema(
            {
                "molecule_chembl_id": Column(
                    pa.String,
                    checks=[Check.str_matches(r"^CHEMBL\d+$", error="Invalid ChEMBL molecule ID format"), Check(lambda x: x.notna())],
                    nullable=False,
                    description="ChEMBL ID молекулы",
                ),
                "nstereo": Column(pa.Int, nullable=True, description="Количество стереоизомеров"),
            },
            strict=False,
        )  # strict=False позволяет дополнительные колонки


class TestitemRawSchema:
    """Схемы для сырых данных теститемов из API."""

    @staticmethod
    def get_schema() -> DataFrameSchema:
        """Схема для сырых данных теститемов."""
        return DataFrameSchema(
            {
                # Основные поля ChEMBL
                "molecule_chembl_id": Column(
                    pa.String,
                    checks=[Check.str_matches(r"^CHEMBL\d+$", error="Invalid ChEMBL molecule ID format"), Check(lambda x: x.notna())],
                    nullable=False,
                    description="ChEMBL ID молекулы",
                ),
                "molregno": Column(pa.Int, nullable=True, description="Номер регистрации молекулы"),
                "pref_name": Column(pa.String, nullable=True, description="Предпочтительное название молекулы"),
                "parent_chembl_id": Column(pa.String, nullable=True, description="ID родительской молекулы"),
                "max_phase": Column(pa.Float, nullable=True, description="Максимальная фаза разработки"),
                "therapeutic_flag": Column(pa.Bool, nullable=True, description="Флаг терапевтического применения"),
                "structure_type": Column(pa.String, nullable=True, description="Тип структуры"),
                "molecule_type": Column(pa.String, nullable=True, description="Тип молекулы"),
                "mw_freebase": Column(pa.Float, nullable=True, description="Молекулярная масса freebase"),
                "alogp": Column(pa.Float, nullable=True, description="ALogP значение"),
                "hba": Column(pa.Int, nullable=True, description="Количество акцепторов водорода"),
                "hbd": Column(pa.Int, nullable=True, description="Количество доноров водорода"),
                "psa": Column(pa.Float, nullable=True, description="Полярная площадь поверхности"),
                "rtb": Column(pa.Int, nullable=True, description="Количество вращающихся связей"),
                "ro3_pass": Column(pa.Bool, nullable=True, description="Проходит ли Rule of 3"),
                "num_ro5_violations": Column(pa.Int, nullable=True, description="Нарушений Rule of 5"),
                "qed_weighted": Column(pa.Float, nullable=True, description="Weighted QED значение"),
                "oral": Column(pa.Bool, nullable=True, description="Оральный путь введения"),
                "parenteral": Column(pa.Bool, nullable=True, description="Парентеральный путь введения"),
                "topical": Column(pa.Bool, nullable=True, description="Топический путь введения"),
                "withdrawn_flag": Column(pa.Bool, nullable=True, description="Отозванное лекарство"),
                "retrieved_at": Column(pa.DateTime, checks=[Check(lambda x: x.notna())], nullable=False, description="Время получения данных"),
                # PubChem поля
                "pubchem_cid": Column(pa.Int, nullable=True, description="PubChem CID"),
                "pubchem_molecular_formula": Column(pa.String, nullable=True, description="Молекулярная формула PubChem"),
                "pubchem_molecular_weight": Column(pa.Float, nullable=True, description="Молекулярная масса PubChem"),
                "pubchem_canonical_smiles": Column(pa.String, nullable=True, description="Канонические SMILES PubChem"),
                "pubchem_inchi": Column(pa.String, nullable=True, description="InChI PubChem"),
                "pubchem_inchi_key": Column(pa.String, nullable=True, description="InChI Key PubChem"),
            }
        )


class TestitemNormalizedSchema:
    """Схемы для нормализованных данных теститемов."""

    @staticmethod
    def get_schema() -> DataFrameSchema:
        """Схема для нормализованных данных теститемов."""
        return DataFrameSchema(
            {
                # Основные идентификаторы и метаданные
                "molecule_chembl_id": Column(
                    pa.String,
                    checks=[Check.str_matches(r"^CHEMBL\d+$", error="Invalid ChEMBL molecule ID format"), Check(lambda x: x.notna())],
                    nullable=False,
                    description="ChEMBL ID молекулы",
                ),
                "molregno": Column(pa.Int, nullable=True, description="Номер регистрации молекулы"),
                "pref_name": Column(pa.String, nullable=True, description="Предпочтительное название молекулы"),
                "pref_name_key": Column(pa.String, nullable=True, description="Ключ предпочтительного названия"),
                "parent_chembl_id": Column(pa.String, nullable=True, description="ID родительской молекулы"),
                "parent_molregno": Column(pa.Int, nullable=True, description="Номер регистрации родительской молекулы"),
                "max_phase": Column(pa.Float, nullable=True, description="Максимальная фаза разработки"),
                "therapeutic_flag": Column(pa.Bool, nullable=True, description="Флаг терапевтического применения"),
                "dosed_ingredient": Column(pa.Bool, nullable=True, description="Флаг дозируемого ингредиента"),
                "first_approval": Column(pa.String, nullable=True, description="Дата первого одобрения"),
                "structure_type": Column(pa.String, nullable=True, description="Тип структуры"),
                "molecule_type": Column(pa.String, nullable=True, description="Тип молекулы"),
                # Физико-химические свойства ChEMBL
                "mw_freebase": Column(pa.Float, nullable=True, description="Молекулярная масса freebase"),
                "alogp": Column(pa.Float, nullable=True, description="ALogP значение"),
                "hba": Column(pa.Int, nullable=True, description="Количество акцепторов водорода"),
                "hbd": Column(pa.Int, nullable=True, description="Количество доноров водорода"),
                "psa": Column(pa.Float, nullable=True, description="Полярная площадь поверхности"),
                "rtb": Column(pa.Int, nullable=True, description="Количество вращающихся связей"),
                "ro3_pass": Column(pa.Bool, nullable=True, description="Проходит ли Rule of 3"),
                "num_ro5_violations": Column(pa.Int, nullable=True, description="Количество нарушений Rule of 5"),
                "acd_most_apka": Column(pa.Float, nullable=True, description="Наиболее кислотный pKa"),
                "acd_most_bpka": Column(pa.Float, nullable=True, description="Наиболее основной pKa"),
                "acd_logp": Column(pa.Float, nullable=True, description="ACD LogP"),
                "acd_logd": Column(pa.Float, nullable=True, description="ACD LogD"),
                "molecular_species": Column(pa.String, nullable=True, description="Молекулярный вид"),
                "full_mwt": Column(pa.Float, nullable=True, description="Полная молекулярная масса"),
                "aromatic_rings": Column(pa.Float, nullable=True, description="Количество ароматических колец"),
                "heavy_atoms": Column(pa.Float, nullable=True, description="Количество тяжелых атомов"),
                "qed_weighted": Column(pa.Float, nullable=True, description="Weighted QED значение"),
                "mw_monoisotopic": Column(pa.Float, nullable=True, description="Моноизотопная молекулярная масса"),
                "full_molformula": Column(pa.String, nullable=True, description="Полная молекулярная формула"),
                "hba_lipinski": Column(pa.Int, nullable=True, description="HBA по Lipinski"),
                "hbd_lipinski": Column(pa.Int, nullable=True, description="HBD по Lipinski"),
                "num_lipinski_ro5_violations": Column(pa.Int, nullable=True, description="Количество нарушений Lipinski Ro5"),
                # Пути введения и флаги
                "oral": Column(pa.Bool, nullable=True, description="Оральный путь введения"),
                "parenteral": Column(pa.Bool, nullable=True, description="Парентеральный путь введения"),
                "topical": Column(pa.Bool, nullable=True, description="Топический путь введения"),
                "black_box_warning": Column(pa.Bool, nullable=True, description="Предупреждение черного ящика"),
                "natural_product": Column(pa.Bool, nullable=True, description="Природный продукт"),
                "first_in_class": Column(pa.Bool, nullable=True, description="Первый в классе"),
                "chirality": Column(pa.Int, nullable=True, description="Хиральность"),
                "prodrug": Column(pa.Bool, nullable=True, description="Пролекарство"),
                "inorganic_flag": Column(pa.Bool, nullable=True, description="Неорганическое соединение"),
                "polymer_flag": Column(pa.Bool, nullable=True, description="Полимер"),
                # Регистрация и отзыв
                "usan_year": Column(pa.Int, nullable=True, description="Год USAN регистрации"),
                "availability_type": Column(pa.String, nullable=True, description="Тип доступности"),
                "usan_stem": Column(pa.String, nullable=True, description="USAN stem"),
                "usan_substem": Column(pa.String, nullable=True, description="USAN substem"),
                "usan_stem_definition": Column(pa.String, nullable=True, description="Определение USAN stem"),
                "indication_class": Column(pa.String, nullable=True, description="Класс показаний"),
                "withdrawn_flag": Column(pa.Bool, nullable=True, description="Отозванное лекарство"),
                "withdrawn_year": Column(pa.Int, nullable=True, description="Год отзыва"),
                "withdrawn_country": Column(pa.String, nullable=True, description="Страна отзыва"),
                "withdrawn_reason": Column(pa.String, nullable=True, description="Причина отзыва"),
                # Механизм действия
                "mechanism_of_action": Column(pa.String, nullable=True, description="Механизм действия"),
                "direct_interaction": Column(pa.Bool, nullable=True, description="Прямое взаимодействие"),
                "molecular_mechanism": Column(pa.String, nullable=True, description="Молекулярный механизм"),
                # Drug данные
                "drug_chembl_id": Column(pa.String, nullable=True, description="ID лекарства в ChEMBL"),
                "drug_name": Column(pa.String, nullable=True, description="Название лекарства"),
                "drug_type": Column(pa.String, nullable=True, description="Тип лекарства"),
                "drug_substance_flag": Column(pa.Bool, nullable=True, description="Флаг лекарственного вещества"),
                "drug_indication_flag": Column(pa.Bool, nullable=True, description="Флаг показаний"),
                "drug_antibacterial_flag": Column(pa.Bool, nullable=True, description="Флаг антибактериального действия"),
                "drug_antiviral_flag": Column(pa.Bool, nullable=True, description="Флаг противовирусного действия"),
                "drug_antifungal_flag": Column(pa.Bool, nullable=True, description="Флаг противогрибкового действия"),
                "drug_antiparasitic_flag": Column(pa.Bool, nullable=True, description="Флаг противопаразитарного действия"),
                "drug_antineoplastic_flag": Column(pa.Bool, nullable=True, description="Флаг противоопухолевого действия"),
                "drug_immunosuppressant_flag": Column(pa.Bool, nullable=True, description="Флаг иммуносупрессивного действия"),
                "drug_antiinflammatory_flag": Column(pa.Bool, nullable=True, description="Флаг противовоспалительного действия"),
                # PubChem данные
                "pubchem_cid": Column(pa.Int, nullable=True, description="PubChem CID"),
                "pubchem_molecular_formula": Column(pa.String, nullable=True, description="Молекулярная формула PubChem"),
                "pubchem_molecular_weight": Column(pa.Float, nullable=True, description="Молекулярная масса PubChem"),
                "pubchem_canonical_smiles": Column(pa.String, nullable=True, description="Канонические SMILES PubChem"),
                "pubchem_isomeric_smiles": Column(pa.String, nullable=True, description="Изомерные SMILES PubChem"),
                "pubchem_inchi": Column(pa.String, nullable=True, description="InChI PubChem"),
                "pubchem_inchi_key": Column(pa.String, nullable=True, description="InChI Key PubChem"),
                "pubchem_registry_id": Column(pa.String, nullable=True, description="PubChem Registry ID"),
                "pubchem_rn": Column(pa.String, nullable=True, description="PubChem RN"),
                # Стандартизированные структуры
                "standardized_inchi": Column(pa.String, nullable=True, description="Стандартизированный InChI"),
                "standardized_inchi_key": Column(pa.String, nullable=True, description="Стандартизированный InChI Key"),
                "standardized_smiles": Column(pa.String, nullable=True, description="Стандартизированные SMILES"),
                # Вложенные структуры ChEMBL (JSON/распакованные)
                "atc_classifications": Column(pa.String, nullable=True, description="ATC классификации (JSON)"),
                "biotherapeutic": Column(pa.String, nullable=True, description="Биотерапевтическое соединение (JSON)"),
                "chemical_probe": Column(pa.Bool, nullable=True, description="Химический зонд"),
                "cross_references": Column(pa.String, nullable=True, description="Перекрестные ссылки (JSON)"),
                "helm_notation": Column(pa.String, nullable=True, description="HELM нотация"),
                "molecule_hierarchy": Column(pa.String, nullable=True, description="Иерархия молекулы (JSON)"),
                "molecule_properties": Column(pa.String, nullable=True, description="Свойства молекулы (JSON)"),
                "molecule_structures": Column(pa.String, nullable=True, description="Структуры молекулы (JSON)"),
                "molecule_synonyms": Column(pa.String, nullable=True, description="Синонимы молекулы (JSON)"),
                "orphan": Column(pa.Bool, nullable=True, description="Орфанное лекарство"),
                "veterinary": Column(pa.Bool, nullable=True, description="Ветеринарное лекарство"),
                "standard_inchi": Column(pa.String, nullable=True, description="Стандартный InChI"),
                "chirality_chembl": Column(pa.String, nullable=True, description="Хиральность из ChEMBL"),
                "molecule_type_chembl": Column(pa.String, nullable=True, description="Тип молекулы из ChEMBL"),
                # Входные данные из input файла
                "nstereo": Column(pa.Int, nullable=True, description="Количество стереоизомеров"),
                "salt_chembl_id": Column(pa.String, nullable=True, description="ChEMBL ID соли"),
                # Метаданные
                "index": Column(pa.Int, nullable=False, description="Порядковый номер записи"),
                "pipeline_version": Column(pa.String, nullable=False, description="Версия пайплайна"),
                "source_system": Column(pa.String, nullable=False, description="Система-источник"),
                "chembl_release": Column(pa.String, nullable=True, description="Версия ChEMBL"),
                "extracted_at": Column(pa.String, nullable=False, description="Время извлечения данных"),
                "hash_row": Column(pa.String, nullable=False, description="Хеш строки SHA256"),
                "hash_business_key": Column(pa.String, nullable=False, description="Хеш бизнес-ключа SHA256"),
            },
            strict=False,
        )  # strict=False позволяет дополнительные колонки


# DEPRECATED: Legacy validation functions removed.
# Use library.common.validation.validate_entity_data() instead.
#
# Example:
# from library.common.validation import validate_entity_data, ValidationStage
# result = validate_entity_data("testitem", df, ValidationStage.NORMALIZED)
