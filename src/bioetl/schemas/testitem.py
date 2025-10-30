"""Pandera schemas for TestItem data according to IO_SCHEMAS_AND_DIAGRAMS.md."""

import pandas as pd
import pandera.pandas as pa

from bioetl.pandera_typing import Series

from bioetl.schemas.base import (
    FALLBACK_METADATA_COLUMN_ORDER,
    BaseSchema,
    FallbackMetadataMixin,
)


class TestItemSchema(FallbackMetadataMixin, BaseSchema):
    """Schema for ChEMBL TestItem (molecule) data with expanded field coverage."""

    molecule_chembl_id: Series[str] = pa.Field(
        nullable=False,
        regex=r'^CHEMBL\d+$',
        description="Primary identifier",
    )
    molregno: Series[pd.Int64Dtype] = pa.Field(
        nullable=True,
        ge=1,
        description="Internal ChEMBL registry number",
    )
    pref_name: Series[str] = pa.Field(nullable=True, description="Preferred name")
    pref_name_key: Series[str] = pa.Field(nullable=True, description="Normalized preferred name key")
    parent_chembl_id: Series[str] = pa.Field(
        nullable=True,
        regex=r'^CHEMBL\d+$',
        description="Parent molecule ChEMBL ID",
    )
    parent_molregno: Series[pd.Int64Dtype] = pa.Field(
        nullable=True,
        ge=1,
        description="Parent molregno",
    )
    therapeutic_flag: Series[bool] = pa.Field(nullable=True, description="Therapeutic flag")
    structure_type: Series[str] = pa.Field(nullable=True, description="Structure type")
    molecule_type: Series[str] = pa.Field(nullable=True, description="Molecule type")
    molecule_type_chembl: Series[str] = pa.Field(nullable=True, description="Molecule type from ChEMBL")
    max_phase: Series[pd.Int64Dtype] = pa.Field(
        nullable=True,
        ge=0,
        description="Maximum clinical phase",
    )
    first_approval: Series[pd.Int64Dtype] = pa.Field(
        nullable=True,
        ge=0,
        description="First approval year",
    )
    dosed_ingredient: Series[bool] = pa.Field(nullable=True, description="Dosed ingredient flag")
    availability_type: Series[pd.Int64Dtype] = pa.Field(
        nullable=True,
        ge=0,
        description="Availability type",
    )
    chirality: Series[str] = pa.Field(nullable=True, description="Chirality descriptor")
    chirality_chembl: Series[str] = pa.Field(nullable=True, description="ChEMBL chirality descriptor")
    mechanism_of_action: Series[str] = pa.Field(nullable=True, description="Mechanism of action")
    direct_interaction: Series[bool] = pa.Field(nullable=True, description="Direct interaction flag")
    molecular_mechanism: Series[bool] = pa.Field(nullable=True, description="Molecular mechanism flag")
    oral: Series[bool] = pa.Field(nullable=True, description="Oral administration flag")
    parenteral: Series[bool] = pa.Field(nullable=True, description="Parenteral administration flag")
    topical: Series[bool] = pa.Field(nullable=True, description="Topical administration flag")
    black_box_warning: Series[bool] = pa.Field(nullable=True, description="Black box warning flag")
    natural_product: Series[bool] = pa.Field(nullable=True, description="Natural product flag")
    first_in_class: Series[bool] = pa.Field(nullable=True, description="First-in-class flag")
    prodrug: Series[bool] = pa.Field(nullable=True, description="Prodrug flag")
    inorganic_flag: Series[bool] = pa.Field(nullable=True, description="Inorganic flag")
    polymer_flag: Series[bool] = pa.Field(nullable=True, description="Polymer flag")
    usan_year: Series[pd.Int64Dtype] = pa.Field(
        nullable=True,
        ge=0,
        description="USAN year",
    )
    usan_stem: Series[str] = pa.Field(nullable=True, description="USAN stem")
    usan_substem: Series[str] = pa.Field(nullable=True, description="USAN substem")
    usan_stem_definition: Series[str] = pa.Field(nullable=True, description="USAN stem definition")
    indication_class: Series[str] = pa.Field(nullable=True, description="Indication class")
    withdrawn_flag: Series[bool] = pa.Field(nullable=True, description="Withdrawn flag")
    withdrawn_year: Series[pd.Int64Dtype] = pa.Field(
        nullable=True,
        ge=0,
        description="Withdrawal year",
    )
    withdrawn_country: Series[str] = pa.Field(nullable=True, description="Withdrawal country")
    withdrawn_reason: Series[str] = pa.Field(nullable=True, description="Withdrawal reason")
    drug_chembl_id: Series[str] = pa.Field(nullable=True, description="Drug ChEMBL ID")
    drug_name: Series[str] = pa.Field(nullable=True, description="Drug name")
    drug_type: Series[str] = pa.Field(nullable=True, description="Drug type")
    drug_substance_flag: Series[bool] = pa.Field(nullable=True, description="Drug substance flag")
    drug_indication_flag: Series[bool] = pa.Field(nullable=True, description="Drug indication flag")
    drug_antibacterial_flag: Series[bool] = pa.Field(nullable=True, description="Drug antibacterial flag")
    drug_antiviral_flag: Series[bool] = pa.Field(nullable=True, description="Drug antiviral flag")
    drug_antifungal_flag: Series[bool] = pa.Field(nullable=True, description="Drug antifungal flag")
    drug_antiparasitic_flag: Series[bool] = pa.Field(nullable=True, description="Drug antiparasitic flag")
    drug_antineoplastic_flag: Series[bool] = pa.Field(nullable=True, description="Drug antineoplastic flag")
    drug_immunosuppressant_flag: Series[bool] = pa.Field(nullable=True, description="Drug immunosuppressant flag")
    drug_antiinflammatory_flag: Series[bool] = pa.Field(nullable=True, description="Drug anti-inflammatory flag")

    mw_freebase: Series[float] = pa.Field(nullable=True, ge=0, description="Free base molecular weight")
    alogp: Series[float] = pa.Field(nullable=True, description="ALogP")
    hba: Series[pd.Int64Dtype] = pa.Field(
        nullable=True,
        ge=0,
        description="Hydrogen bond acceptors",
    )
    hbd: Series[pd.Int64Dtype] = pa.Field(
        nullable=True,
        ge=0,
        description="Hydrogen bond donors",
    )
    psa: Series[float] = pa.Field(nullable=True, ge=0, description="Polar surface area")
    rtb: Series[pd.Int64Dtype] = pa.Field(
        nullable=True,
        ge=0,
        description="Rotatable bonds (rtb)",
    )
    ro3_pass: Series[bool] = pa.Field(nullable=True, description="Rule of three pass flag")
    num_ro5_violations: Series[pd.Int64Dtype] = pa.Field(
        nullable=True,
        ge=0,
        description="Number of RO5 violations",
    )
    acd_most_apka: Series[float] = pa.Field(nullable=True, description="ACD most acidic pKa")
    acd_most_bpka: Series[float] = pa.Field(nullable=True, description="ACD most basic pKa")
    acd_logp: Series[float] = pa.Field(nullable=True, description="ACD LogP")
    acd_logd: Series[float] = pa.Field(nullable=True, description="ACD LogD")
    molecular_species: Series[str] = pa.Field(nullable=True, description="Molecular species")
    full_mwt: Series[float] = pa.Field(nullable=True, ge=0, description="Full molecular weight")
    aromatic_rings: Series[pd.Int64Dtype] = pa.Field(
        nullable=True,
        ge=0,
        description="Aromatic ring count",
    )
    heavy_atoms: Series[pd.Int64Dtype] = pa.Field(
        nullable=True,
        ge=0,
        description="Heavy atom count",
    )
    qed_weighted: Series[float] = pa.Field(nullable=True, description="QED weighted score")
    mw_monoisotopic: Series[float] = pa.Field(nullable=True, ge=0, description="Monoisotopic molecular weight")
    full_molformula: Series[str] = pa.Field(nullable=True, description="Full molecular formula")
    hba_lipinski: Series[pd.Int64Dtype] = pa.Field(
        nullable=True,
        ge=0,
        description="Lipinski H-bond acceptors",
    )
    hbd_lipinski: Series[pd.Int64Dtype] = pa.Field(
        nullable=True,
        ge=0,
        description="Lipinski H-bond donors",
    )
    num_lipinski_ro5_violations: Series[pd.Int64Dtype] = pa.Field(
        nullable=True,
        ge=0,
        description="Lipinski RO5 violation count",
    )
    lipinski_ro5_violations: Series[pd.Int64Dtype] = pa.Field(
        nullable=True,
        ge=0,
        description="Alias for Lipinski RO5 violations",
    )
    lipinski_ro5_pass: Series[bool] = pa.Field(nullable=True, description="Lipinski RO5 pass flag")

    standardized_smiles: Series[str] = pa.Field(nullable=True, description="Standardized SMILES")
    standard_inchi: Series[str] = pa.Field(nullable=True, description="Standard InChI")
    standard_inchi_key: Series[str] = pa.Field(nullable=True, description="Standard InChI key")

    all_names: Series[str] = pa.Field(nullable=True, description="Aggregated synonyms")
    molecule_hierarchy: Series[str] = pa.Field(nullable=True, description="Molecule hierarchy JSON")
    molecule_properties: Series[str] = pa.Field(nullable=True, description="Molecule properties JSON")
    molecule_structures: Series[str] = pa.Field(nullable=True, description="Molecule structures JSON")
    molecule_synonyms: Series[str] = pa.Field(nullable=True, description="Molecule synonyms JSON")
    atc_classifications: Series[str] = pa.Field(nullable=True, description="ATC classifications JSON")
    cross_references: Series[str] = pa.Field(nullable=True, description="Cross references JSON")
    biotherapeutic: Series[str] = pa.Field(nullable=True, description="Biotherapeutic JSON")
    chemical_probe: Series[str] = pa.Field(nullable=True, description="Chemical probe JSON")
    orphan: Series[str] = pa.Field(nullable=True, description="Orphan designation JSON")
    veterinary: Series[str] = pa.Field(nullable=True, description="Veterinary JSON")
    helm_notation: Series[str] = pa.Field(nullable=True, description="HELM notation JSON")

    pubchem_cid: Series[pd.Int64Dtype] = pa.Field(
        nullable=True,
        ge=1,
        description="PubChem CID",
    )
    pubchem_molecular_formula: Series[str] = pa.Field(nullable=True, description="PubChem molecular formula")
    pubchem_molecular_weight: Series[float] = pa.Field(nullable=True, ge=0, description="PubChem molecular weight")
    pubchem_canonical_smiles: Series[str] = pa.Field(nullable=True, description="PubChem canonical SMILES")
    pubchem_isomeric_smiles: Series[str] = pa.Field(nullable=True, description="PubChem isomeric SMILES")
    pubchem_inchi: Series[str] = pa.Field(nullable=True, description="PubChem InChI")
    pubchem_inchi_key: Series[str] = pa.Field(
        nullable=True,
        regex=r'^[A-Z]{14}-[A-Z]{10}-[A-Z]$',
        description="PubChem InChI key",
    )
    pubchem_iupac_name: Series[str] = pa.Field(nullable=True, description="PubChem IUPAC name")
    pubchem_registry_id: Series[str] = pa.Field(nullable=True, description="PubChem registry ID")
    pubchem_rn: Series[str] = pa.Field(nullable=True, description="PubChem RN")
    pubchem_synonyms: Series[str] = pa.Field(nullable=True, description="PubChem synonyms JSON")
    pubchem_enriched_at: Series[str] = pa.Field(nullable=True, description="PubChem enrichment timestamp")
    pubchem_cid_source: Series[str] = pa.Field(nullable=True, description="PubChem CID source")
    pubchem_fallback_used: Series[bool] = pa.Field(nullable=True, description="PubChem fallback used flag")
    pubchem_enrichment_attempt: Series[pd.Int64Dtype] = pa.Field(
        nullable=True,
        ge=0,
        description="PubChem enrichment attempt",
    )
    pubchem_lookup_inchikey: Series[str] = pa.Field(nullable=True, description="Lookup InChIKey used for PubChem resolution")

    _column_order = [
        "index",
        "hash_row",
        "hash_business_key",
        "pipeline_version",
        "run_id",
        "source_system",
        "chembl_release",
        "extracted_at",
        *FALLBACK_METADATA_COLUMN_ORDER,
        # Primary/business keys first
        "molecule_chembl_id",
        
        # Core ChEMBL business fields
        "molregno",
        "pref_name",
        "pref_name_key",
        "parent_chembl_id",
        "parent_molregno",
        "therapeutic_flag",
        "structure_type",
        "molecule_type",
        "molecule_type_chembl",
        "max_phase",
        "first_approval",
        "dosed_ingredient",
        "availability_type",
        "chirality",
        "chirality_chembl",
        "mechanism_of_action",
        "direct_interaction",
        "molecular_mechanism",
        "oral",
        "parenteral",
        "topical",
        "black_box_warning",
        "natural_product",
        "first_in_class",
        "prodrug",
        "inorganic_flag",
        "polymer_flag",
        "usan_year",
        "usan_stem",
        "usan_substem",
        "usan_stem_definition",
        "indication_class",
        "withdrawn_flag",
        "withdrawn_year",
        "withdrawn_country",
        "withdrawn_reason",
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
        
        # Properties
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
        "lipinski_ro5_violations",
        "lipinski_ro5_pass",
        
        # Structures and text
        "standardized_smiles",
        "standard_inchi",
        "standard_inchi_key",
        "all_names",
        
        # Nested JSON blobs
        "molecule_hierarchy",
        "molecule_properties",
        "molecule_structures",
        "molecule_synonyms",
        "atc_classifications",
        "cross_references",
        "biotherapeutic",
        "chemical_probe",
        "orphan",
        "veterinary",
        "helm_notation",
    ] + [
        # PubChem enrichment fields
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
        "pubchem_synonyms",
        "pubchem_enriched_at",
        "pubchem_cid_source",
        "pubchem_fallback_used",
        "pubchem_enrichment_attempt",
        "pubchem_lookup_inchikey",
    ]

    class Config:
        strict = True
        coerce = True
        ordered = True
