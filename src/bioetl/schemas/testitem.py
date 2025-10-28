"""Pandera schemas for TestItem data according to IO_SCHEMAS_AND_DIAGRAMS.md."""

import pandera as pa
from pandera.typing import Series

from bioetl.schemas.base import BaseSchema


class TestItemSchema(BaseSchema):
    """Schema for ChEMBL TestItem (molecule) data.

    Primary Key: [molecule_chembl_id]
    Contains ~81 fields including all physico-chemical properties, drug_* fields, structures
    """

    # Primary Key
    molecule_chembl_id: Series[str] = pa.Field(
        nullable=False,
        regex=r'^CHEMBL\d+$',
        description="Первичный ключ",
    )

    # ChEMBL identifiers
    molregno: Series[int] = pa.Field(nullable=True, ge=1, description="Внутренний регистровый номер")
    parent_chembl_id: Series[str] = pa.Field(
        nullable=True,
        regex=r'^CHEMBL\d+$',
        description="Связь с родительской молекулой",
    )

    # Basic molecule information
    pref_name: Series[str] = pa.Field(nullable=True, description="Предпочтительное название")
    max_phase: Series[int] = pa.Field(nullable=True, ge=0, description="Максимальная стадия разработки")
    structure_type: Series[str] = pa.Field(nullable=True, description="Тип структуры")
    molecule_type: Series[str] = pa.Field(nullable=True, description="Тип молекулы")

    # Molecular properties
    mw_freebase: Series[float] = pa.Field(nullable=True, ge=0, description="Молекулярная масса")
    qed_weighted: Series[float] = pa.Field(nullable=True, description="QED score")

    # Structure
    standardized_smiles: Series[str] = pa.Field(nullable=True, description="Стандартизированная структура")
    standard_inchi: Series[str] = pa.Field(nullable=True, description="Standard InChI")
    standard_inchi_key: Series[str] = pa.Field(nullable=True, description="Standard InChI Key")

    # Additional properties
    heavy_atoms: Series[int] = pa.Field(nullable=True, ge=0, description="Heavy atoms count")
    aromatic_rings: Series[int] = pa.Field(nullable=True, ge=0, description="Aromatic rings count")
    rotatable_bonds: Series[int] = pa.Field(nullable=True, ge=0, description="Rotatable bonds count")
    hba: Series[int] = pa.Field(nullable=True, ge=0, description="Hydrogen bond acceptors")
    hbd: Series[int] = pa.Field(nullable=True, ge=0, description="Hydrogen bond donors")

    # Lipinski
    lipinski_ro5_violations: Series[int] = pa.Field(nullable=True, ge=0, description="Lipinski RO5 violations")
    lipinski_ro5_pass: Series[bool] = pa.Field(nullable=True, description="Lipinski RO5 pass")

    # Synonyms
    all_names: Series[str] = pa.Field(nullable=True, description="All names")
    molecule_synonyms: Series[str] = pa.Field(nullable=True, description="Molecule synonyms (JSON)")

    # Classification
    atc_classifications: Series[str] = pa.Field(nullable=True, description="ATC classifications (JSON)")

    # Fallback metadata
    fallback_error_code: Series[str] = pa.Field(nullable=True, description="Fallback error code or type")
    fallback_http_status: Series[int] = pa.Field(nullable=True, ge=0, description="HTTP status for fallback")
    fallback_retry_after_sec: Series[float] = pa.Field(nullable=True, ge=0, description="Retry-After header value")
    fallback_attempt: Series[int] = pa.Field(nullable=True, ge=0, description="Attempt number when fallback created")
    fallback_error_message: Series[str] = pa.Field(nullable=True, description="Fallback error message context")

    # PubChem enrichment (optional)
    pubchem_cid: Series[int] = pa.Field(nullable=True, ge=1, description="Идентификатор PubChem (enrichment)")
    pubchem_molecular_formula: Series[str] = pa.Field(nullable=True, description="PubChem molecular formula")
    pubchem_molecular_weight: Series[float] = pa.Field(nullable=True, ge=0, description="PubChem molecular weight")
    pubchem_canonical_smiles: Series[str] = pa.Field(nullable=True, description="PubChem canonical SMILES")
    pubchem_isomeric_smiles: Series[str] = pa.Field(nullable=True, description="PubChem isomeric SMILES")
    pubchem_inchi: Series[str] = pa.Field(nullable=True, description="PubChem InChI")
    pubchem_inchi_key: Series[str] = pa.Field(
        nullable=True,
        regex=r'^[A-Z]{14}-[A-Z]{10}-[A-Z]$',
        description="PubChem InChI Key"
    )
    pubchem_iupac_name: Series[str] = pa.Field(nullable=True, description="PubChem IUPAC name")
    pubchem_synonyms: Series[str] = pa.Field(nullable=True, description="PubChem synonyms")

    # System fields (from BaseSchema)
    # index, hash_row, hash_business_key, pipeline_version, source_system, chembl_release, extracted_at

    class Config:
        strict = True
        coerce = True
        ordered = True
        # Column order: business fields first, then system fields, then hash fields
        column_order = [
            "molecule_chembl_id",
            "molregno",
            "pref_name",
            "parent_chembl_id",
            "max_phase",
            "structure_type",
            "molecule_type",
            "mw_freebase",
            "qed_weighted",
            "standardized_smiles",
            "standard_inchi",
            "standard_inchi_key",
            "heavy_atoms",
            "aromatic_rings",
            "rotatable_bonds",
            "hba",
            "hbd",
            "lipinski_ro5_violations",
            "lipinski_ro5_pass",
            "all_names",
            "molecule_synonyms",
            "atc_classifications",
            "fallback_error_code",
            "fallback_http_status",
            "fallback_retry_after_sec",
            "fallback_attempt",
            "fallback_error_message",
            "pubchem_cid",
            "pubchem_molecular_formula",
            "pubchem_molecular_weight",
            "pubchem_canonical_smiles",
            "pubchem_isomeric_smiles",
            "pubchem_inchi",
            "pubchem_inchi_key",
            "pubchem_iupac_name",
            "pubchem_synonyms",
            "pipeline_version",
            "source_system",
            "chembl_release",
            "extracted_at",
            "hash_business_key",
            "hash_row",
            "index",
        ]

