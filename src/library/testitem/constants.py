"""Constants for testitem ChEMBL API fields."""

from __future__ import annotations

# ChEMBL API fields to request when fetching molecule data
# These fields must be explicitly requested in the API call
TESTITEM_CHEMBL_FIELDS = (
    # Core identifiers
    "molecule_chembl_id",
    "molregno",
    "pref_name",
    "parent_molecule_chembl_id",
    
    # Basic properties
    "max_phase",
    "molecule_type",
    "first_approval",
    "structure_type",
    "therapeutic_flag",
    "dosed_ingredient",
    
    # Routes of administration
    "oral",
    "parenteral",
    "topical",
    
    # Warnings
    "black_box_warning",
    
    # Structure flags
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
    "indication_class",
    
    # Withdrawal information
    "withdrawn_flag",
    "withdrawn_year",
    "withdrawn_country",
    "withdrawn_reason",
    
    # Mechanism of action
    "mechanism_of_action",
    "direct_interaction",
    "molecular_mechanism",
    
    # Drug information
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
    
    # Nested structures - use dot notation for ChEMBL API
    "molecule_structures.canonical_smiles",
    "molecule_structures.standard_inchi",
    "molecule_structures.standard_inchi_key",
    
    # Molecule properties (physical-chemical properties)
    "molecule_properties.mw_freebase",
    "molecule_properties.alogp",
    "molecule_properties.hba",
    "molecule_properties.hbd",
    "molecule_properties.psa",
    "molecule_properties.rtb",
    "molecule_properties.ro3_pass",
    "molecule_properties.num_ro5_violations",
    "molecule_properties.acd_most_apka",
    "molecule_properties.acd_most_bpka",
    "molecule_properties.acd_logp",
    "molecule_properties.acd_logd",
    "molecule_properties.molecular_species",
    "molecule_properties.full_mwt",
    "molecule_properties.aromatic_rings",
    "molecule_properties.heavy_atoms",
    "molecule_properties.qed_weighted",
    "molecule_properties.mw_monoisotopic",
    "molecule_properties.full_molformula",
    "molecule_properties.hba_lipinski",
    "molecule_properties.hbd_lipinski",
    "molecule_properties.num_lipinski_ro5_violations",
    
    # Molecule hierarchy
    "molecule_hierarchy.parent_chembl_id",
    "molecule_hierarchy.parent_molregno",
    
    # Additional nested structures
    "molecule_synonyms",
    "cross_references",
    "atc_classifications",
    "biotherapeutic",
    "helm_notation",
    "orphan",
    "veterinary",
    "chemical_probe",
)

# Additional fields that require special handling
TESTITEM_ENRICHMENT_FIELDS = (
    "pref_name_key",
    "salt_chembl_id",
)

__all__ = [
    "TESTITEM_CHEMBL_FIELDS",
    "TESTITEM_ENRICHMENT_FIELDS",
]
