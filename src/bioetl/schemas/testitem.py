"""Pandera schema for TestItem pipeline output."""

from __future__ import annotations

import pandera as pa
from pandera.typing import Series

from bioetl.schemas.base import BaseSchema


TESTITEM_COLUMN_ORDER: list[str] = [
    # Identifiers
    "molecule_chembl_id",
    "molregno",
    "pref_name",
    "pref_name_key",
    # Hierarchy
    "parent_chembl_id",
    "parent_molregno",
    # Development and registration
    "max_phase",
    "therapeutic_flag",
    "dosed_ingredient",
    "first_approval",
    # Types
    "structure_type",
    "molecule_type",
    # Physico-chemical properties (22)
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
    # Structures
    "canonical_smiles",
    "standard_inchi",
    "standard_inchi_key",
    # Administration route flags
    "oral",
    "parenteral",
    "topical",
    # Additional flags
    "black_box_warning",
    "natural_product",
    "first_in_class",
    "chirality",
    "prodrug",
    "inorganic_flag",
    "polymer_flag",
    # USAN registration
    "usan_year",
    "availability_type",
    "usan_stem",
    "usan_substem",
    "usan_stem_definition",
    # Withdrawn / indications
    "indication_class",
    "withdrawn_flag",
    "withdrawn_year",
    "withdrawn_country",
    "withdrawn_reason",
    # Mechanism of action
    "mechanism_of_action",
    "direct_interaction",
    "molecular_mechanism",
    # Drug fields
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
    # PubChem enrichment
    "pubchem_cid",
    "pubchem_molecular_formula",
    "pubchem_molecular_weight",
    "pubchem_canonical_smiles",
    "pubchem_isomeric_smiles",
    "pubchem_inchi",
    "pubchem_inchi_key",
    "pubchem_iupac_name",
    "pubchem_registry_id",
    "pubchem_rn",
    # Standardized structures
    "standardized_inchi",
    "standardized_inchi_key",
    "standardized_smiles",
    # Nested JSON payloads
    "atc_classifications",
    "biotherapeutic",
    "chemical_probe",
    "cross_references",
    "helm_notation",
    "molecule_hierarchy",
    "molecule_properties",
    "molecule_structures",
    "molecule_synonyms",
    "all_names",
    "orphan",
    "veterinary",
    "chirality_chembl",
    "molecule_type_chembl",
    # Input columns
    "nstereo",
    "salt_chembl_id",
    # Metadata and hashes (from BaseSchema)
    "pipeline_version",
    "source_system",
    "chembl_release",
    "extracted_at",
    "hash_business_key",
    "hash_row",
    "index",
]


class TestItemSchema(BaseSchema):
    """Schema for normalized TestItem pipeline output."""

    molecule_chembl_id: Series[str] = pa.Field(
        nullable=False,
        regex=r"^CHEMBL\d+$",
        description="Идентификатор молекулы ChEMBL",
    )
    molregno: Series[object] = pa.Field(nullable=True, description="Внутренний регистровый номер")
    pref_name: Series[str] = pa.Field(nullable=True, description="Предпочтительное название")
    pref_name_key: Series[str] = pa.Field(nullable=True, description="Нормализованный ключ названия")
    parent_chembl_id: Series[str] = pa.Field(nullable=True, regex=r"^CHEMBL\d+$", description="Родительская молекула")
    parent_molregno: Series[object] = pa.Field(nullable=True, description="Родительский molregno")
    max_phase: Series[object] = pa.Field(nullable=True, description="Стадия разработки")
    therapeutic_flag: Series[object] = pa.Field(nullable=True, description="Терапевтическое применение")
    dosed_ingredient: Series[object] = pa.Field(nullable=True, description="Дозируемый ингредиент")
    first_approval: Series[object] = pa.Field(nullable=True, description="Год первой регистрации")
    structure_type: Series[str] = pa.Field(nullable=True, description="Тип структуры")
    molecule_type: Series[str] = pa.Field(nullable=True, description="Тип молекулы")

    # Physico-chemical properties
    mw_freebase: Series[object] = pa.Field(nullable=True, description="Молекулярная масса freebase")
    alogp: Series[object] = pa.Field(nullable=True, description="ALogP")
    hba: Series[object] = pa.Field(nullable=True, description="Hydrogen bond acceptors")
    hbd: Series[object] = pa.Field(nullable=True, description="Hydrogen bond donors")
    psa: Series[object] = pa.Field(nullable=True, description="Polar surface area")
    rtb: Series[object] = pa.Field(nullable=True, description="Rotatable bonds")
    ro3_pass: Series[object] = pa.Field(nullable=True, description="Rule of three pass flag")
    num_ro5_violations: Series[object] = pa.Field(nullable=True, description="Rule of five violations")
    acd_most_apka: Series[object] = pa.Field(nullable=True, description="ACD most acidic pKa")
    acd_most_bpka: Series[object] = pa.Field(nullable=True, description="ACD most basic pKa")
    acd_logp: Series[object] = pa.Field(nullable=True, description="ACD logP")
    acd_logd: Series[object] = pa.Field(nullable=True, description="ACD logD")
    molecular_species: Series[object] = pa.Field(nullable=True, description="Тип молекулярного вида")
    full_mwt: Series[object] = pa.Field(nullable=True, description="Полная молекулярная масса")
    aromatic_rings: Series[object] = pa.Field(nullable=True, description="Количество ароматических колец")
    heavy_atoms: Series[object] = pa.Field(nullable=True, description="Количество тяжелых атомов")
    qed_weighted: Series[object] = pa.Field(nullable=True, description="QED score")
    mw_monoisotopic: Series[object] = pa.Field(nullable=True, description="Моноизотопная масса")
    full_molformula: Series[object] = pa.Field(nullable=True, description="Полная формула")
    hba_lipinski: Series[object] = pa.Field(nullable=True, description="HBA Lipinski")
    hbd_lipinski: Series[object] = pa.Field(nullable=True, description="HBD Lipinski")
    num_lipinski_ro5_violations: Series[object] = pa.Field(nullable=True, description="RO5 нарушения по Липински")

    canonical_smiles: Series[str] = pa.Field(nullable=True, description="Канонический SMILES")
    standard_inchi: Series[str] = pa.Field(nullable=True, description="Standard InChI")
    standard_inchi_key: Series[str] = pa.Field(nullable=True, description="Standard InChI Key")

    oral: Series[object] = pa.Field(nullable=True, description="Пероральное применение")
    parenteral: Series[object] = pa.Field(nullable=True, description="Парентеральное применение")
    topical: Series[object] = pa.Field(nullable=True, description="Топическое применение")

    black_box_warning: Series[object] = pa.Field(nullable=True, description="Black box warning")
    natural_product: Series[object] = pa.Field(nullable=True, description="Природный продукт")
    first_in_class: Series[object] = pa.Field(nullable=True, description="Первый в классе")
    chirality: Series[object] = pa.Field(nullable=True, description="Хиральность")
    prodrug: Series[object] = pa.Field(nullable=True, description="Продраг")
    inorganic_flag: Series[object] = pa.Field(nullable=True, description="Неорганическое соединение")
    polymer_flag: Series[object] = pa.Field(nullable=True, description="Полимер")

    usan_year: Series[object] = pa.Field(nullable=True, description="Год USAN")
    availability_type: Series[object] = pa.Field(nullable=True, description="Тип доступности")
    usan_stem: Series[object] = pa.Field(nullable=True, description="USAN stem")
    usan_substem: Series[object] = pa.Field(nullable=True, description="USAN substem")
    usan_stem_definition: Series[object] = pa.Field(nullable=True, description="Определение USAN stem")

    indication_class: Series[object] = pa.Field(nullable=True, description="Класс индикации")
    withdrawn_flag: Series[object] = pa.Field(nullable=True, description="Флаг отзыва")
    withdrawn_year: Series[object] = pa.Field(nullable=True, description="Год отзыва")
    withdrawn_country: Series[object] = pa.Field(nullable=True, description="Страна отзыва")
    withdrawn_reason: Series[object] = pa.Field(nullable=True, description="Причина отзыва")

    mechanism_of_action: Series[object] = pa.Field(nullable=True, description="Механизм действия")
    direct_interaction: Series[object] = pa.Field(nullable=True, description="Прямое взаимодействие")
    molecular_mechanism: Series[object] = pa.Field(nullable=True, description="Молекулярный механизм")

    drug_chembl_id: Series[object] = pa.Field(nullable=True, description="ID лекарственного препарата")
    drug_name: Series[object] = pa.Field(nullable=True, description="Название препарата")
    drug_type: Series[object] = pa.Field(nullable=True, description="Тип препарата")
    drug_substance_flag: Series[object] = pa.Field(nullable=True, description="Флаг субстанции")
    drug_indication_flag: Series[object] = pa.Field(nullable=True, description="Флаг индикации")
    drug_antibacterial_flag: Series[object] = pa.Field(nullable=True, description="Антибактериальный флаг")
    drug_antiviral_flag: Series[object] = pa.Field(nullable=True, description="Противовирусный флаг")
    drug_antifungal_flag: Series[object] = pa.Field(nullable=True, description="Противогрибковый флаг")
    drug_antiparasitic_flag: Series[object] = pa.Field(nullable=True, description="Противопаразитарный флаг")
    drug_antineoplastic_flag: Series[object] = pa.Field(nullable=True, description="Противоопухолевый флаг")
    drug_immunosuppressant_flag: Series[object] = pa.Field(nullable=True, description="Иммуносупрессивный флаг")
    drug_antiinflammatory_flag: Series[object] = pa.Field(nullable=True, description="Противовоспалительный флаг")

    pubchem_cid: Series[object] = pa.Field(nullable=True, description="PubChem CID")
    pubchem_molecular_formula: Series[str] = pa.Field(nullable=True, description="PubChem формула")
    pubchem_molecular_weight: Series[object] = pa.Field(nullable=True, description="PubChem масса")
    pubchem_canonical_smiles: Series[str] = pa.Field(nullable=True, description="PubChem canonical SMILES")
    pubchem_isomeric_smiles: Series[str] = pa.Field(nullable=True, description="PubChem isomeric SMILES")
    pubchem_inchi: Series[str] = pa.Field(nullable=True, description="PubChem InChI")
    pubchem_inchi_key: Series[str] = pa.Field(nullable=True, description="PubChem InChI Key")
    pubchem_iupac_name: Series[str] = pa.Field(nullable=True, description="PubChem IUPAC name")
    pubchem_registry_id: Series[str] = pa.Field(nullable=True, description="PubChem Registry ID")
    pubchem_rn: Series[str] = pa.Field(nullable=True, description="PubChem RN")

    standardized_inchi: Series[str] = pa.Field(nullable=True, description="Стандартизированный InChI")
    standardized_inchi_key: Series[str] = pa.Field(nullable=True, description="Стандартизированный InChI Key")
    standardized_smiles: Series[str] = pa.Field(nullable=True, description="Стандартизированный SMILES")

    atc_classifications: Series[str] = pa.Field(nullable=True, description="ATC классификации (JSON)")
    biotherapeutic: Series[str] = pa.Field(nullable=True, description="Biotherapeutic JSON")
    chemical_probe: Series[str] = pa.Field(nullable=True, description="Chemical probe JSON")
    cross_references: Series[str] = pa.Field(nullable=True, description="Cross references JSON")
    helm_notation: Series[str] = pa.Field(nullable=True, description="HELM нотация JSON")
    molecule_hierarchy: Series[str] = pa.Field(nullable=True, description="Иерархия молекулы JSON")
    molecule_properties: Series[str] = pa.Field(nullable=True, description="Сырые свойства JSON")
    molecule_structures: Series[str] = pa.Field(nullable=True, description="Структуры JSON")
    molecule_synonyms: Series[str] = pa.Field(nullable=True, description="Синонимы JSON")
    all_names: Series[str] = pa.Field(nullable=True, description="Агрегированные названия")
    orphan: Series[object] = pa.Field(nullable=True, description="Orphan статус")
    veterinary: Series[object] = pa.Field(nullable=True, description="Veterinary статус")
    chirality_chembl: Series[object] = pa.Field(nullable=True, description="Raw chirality from ChEMBL")
    molecule_type_chembl: Series[object] = pa.Field(nullable=True, description="Raw molecule type from ChEMBL")

    nstereo: Series[object] = pa.Field(nullable=True, description="Количество стереоцентров (input)")
    salt_chembl_id: Series[str] = pa.Field(nullable=True, description="ChEMBL ID соли")

    class Config:
        strict = True
        coerce = True
        ordered = True
        column_order = TESTITEM_COLUMN_ORDER

