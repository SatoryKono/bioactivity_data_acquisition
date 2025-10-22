"""Pandera schemas for target data validation."""

from __future__ import annotations

import importlib.util

from pandera.typing import Series

_PANDERA_PANDAS_SPEC = importlib.util.find_spec("pandera.pandas")
if _PANDERA_PANDAS_SPEC is not None:  # pragma: no cover - import side effect
    import pandera.pandas as pa  # type: ignore[no-redef]
else:  # pragma: no cover - import side effect
    import pandera as pa


class TargetInputSchema(pa.DataFrameModel):
    """Schema for input target data from CSV files."""

    target_chembl_id: Series[str] = pa.Field(description="ChEMBL target identifier")

    class Config:
        strict = False  # Allow extra columns
        coerce = True


class TargetNormalizedSchema(pa.DataFrameModel):
    """Schema for normalized target data after enrichment."""

    # Business key - only required field
    target_chembl_id: Series[str] = pa.Field(nullable=False)
    
    # ChEMBL core fields
    pref_name: Series[str] = pa.Field(nullable=True, description="Preferred name from ChEMBL")
    target_type: Series[str] = pa.Field(nullable=True, description="Target type from ChEMBL")
    organism: Series[str] = pa.Field(nullable=True, description="Organism from ChEMBL")
    
    # UniProt primary fields
    uniprot_id_primary: Series[str] = pa.Field(nullable=True, description="Primary UniProt ID")
    uniprot_ids_all: Series[str] = pa.Field(nullable=True, description="All UniProt IDs")
    
    # Isoform information
    isoform_ids: Series[str] = pa.Field(nullable=True, description="Isoform identifiers")
    isoform_names: Series[str] = pa.Field(nullable=True, description="Isoform names")
    isoform_synonyms: Series[str] = pa.Field(nullable=True, description="Isoform synonyms")
    
    # HGNC enrichment fields (из ChEMBL cross-references)
    hgnc_id: Series[str] = pa.Field(
        nullable=True, 
        description="HGNC identifier from ChEMBL cross-references"
    )
    hgnc_name: Series[str] = pa.Field(
        nullable=True, 
        description="HGNC gene name from ChEMBL cross-references"
    )
    
    # Gene symbol (из UniProt API)
    gene_symbol: Series[str] = pa.Field(
        nullable=True, 
        description="Primary gene symbol from UniProt"
    )
    
    # Protein names
    protein_name_canonical: Series[str] = pa.Field(nullable=True, description="Canonical protein name")
    protein_name_alt: Series[str] = pa.Field(nullable=True, description="Alternative protein names")
    
    # Organism taxonomy
    taxon_id: Series[str] = pa.Field(nullable=True, description="Taxon ID")
    lineage_superkingdom: Series[str] = pa.Field(nullable=True, description="Superkingdom lineage")
    lineage_phylum: Series[str] = pa.Field(nullable=True, description="Phylum lineage")
    lineage_class: Series[str] = pa.Field(nullable=True, description="Class lineage")
    
    # Sequence information
    sequence_length: Series[str] = pa.Field(nullable=True, description="Sequence length")
    
    # Features
    features_signal_peptide: Series[bool] = pa.Field(nullable=True, description="Signal peptide feature")
    features_transmembrane: Series[bool] = pa.Field(nullable=True, description="Transmembrane feature")
    features_topology: Series[str] = pa.Field(nullable=True, description="Topology features")
    
    # Post-translational modifications
    ptm_glycosylation: Series[bool] = pa.Field(nullable=True, description="Glycosylation PTM")
    ptm_lipidation: Series[bool] = pa.Field(nullable=True, description="Lipidation PTM")
    ptm_disulfide_bond: Series[bool] = pa.Field(nullable=True, description="Disulfide bond PTM")
    ptm_modified_residue: Series[bool] = pa.Field(nullable=True, description="Modified residue PTM")
    
    # Cross-references
    xref_chembl: Series[str] = pa.Field(nullable=True, description="ChEMBL cross-reference")
    xref_uniprot: Series[str] = pa.Field(nullable=True, description="UniProt cross-reference")
    xref_ensembl: Series[str] = pa.Field(nullable=True, description="Ensembl cross-reference")
    xref_iuphar: Series[str] = pa.Field(nullable=True, description="IUPHAR cross-reference")
    
    # GtoPdb enrichment fields
    gtop_target_id: Series[str] = pa.Field(nullable=True, description="Guide to Pharmacology target ID")
    gtop_synonyms: Series[str] = pa.Field(nullable=True, description="Guide to Pharmacology synonyms")
    gtop_natural_ligands_n: Series[str] = pa.Field(nullable=True, description="Number of natural ligands")
    gtop_interactions_n: Series[str] = pa.Field(nullable=True, description="Number of interactions")
    gtop_function_text_short: Series[str] = pa.Field(nullable=True, description="Short function description")
    
    # UniProt metadata
    uniprot_last_update: Series[str] = pa.Field(nullable=True, description="UniProt last update date")
    uniprot_version: Series[str] = pa.Field(nullable=True, description="UniProt version")
    pipeline_version: Series[str] = pa.Field(nullable=True, description="Pipeline version")
    timestamp_utc: Series[str] = pa.Field(nullable=True, description="UTC timestamp")
    
    # Database cross-references
    pfam: Series[str] = pa.Field(nullable=True, description="Pfam cross-reference")
    interpro: Series[str] = pa.Field(nullable=True, description="InterPro cross-reference")
    xref_pdb: Series[str] = pa.Field(nullable=True, description="PDB cross-reference")
    xref_alphafold: Series[str] = pa.Field(nullable=True, description="AlphaFold cross-reference")
    
    # UniProt specific fields
    uniProtkbId: Series[str] = pa.Field(nullable=True, description="UniProtKB ID")
    secondaryAccessions: Series[str] = pa.Field(nullable=True, description="Secondary accessions")
    recommendedName: Series[str] = pa.Field(nullable=True, description="Recommended protein name")
    geneName: Series[str] = pa.Field(nullable=True, description="Gene name")
    secondaryAccessionNames: Series[str] = pa.Field(nullable=True, description="Secondary accession names")
    
    # Functional annotations
    molecular_function: Series[str] = pa.Field(nullable=True, description="Molecular function")
    cellular_component: Series[str] = pa.Field(nullable=True, description="Cellular component")
    subcellular_location: Series[str] = pa.Field(nullable=True, description="Subcellular location")
    topology: Series[str] = pa.Field(nullable=True, description="Topology")
    transmembrane: Series[bool] = pa.Field(nullable=True, description="Transmembrane")
    intramembrane: Series[bool] = pa.Field(nullable=True, description="Intramembrane")
    glycosylation: Series[bool] = pa.Field(nullable=True, description="Glycosylation")
    lipidation: Series[bool] = pa.Field(nullable=True, description="Lipidation")
    disulfide_bond: Series[bool] = pa.Field(nullable=True, description="Disulfide bond")
    modified_residue: Series[bool] = pa.Field(nullable=True, description="Modified residue")
    phosphorylation: Series[bool] = pa.Field(nullable=True, description="Phosphorylation")
    acetylation: Series[bool] = pa.Field(nullable=True, description="Acetylation")
    ubiquitination: Series[bool] = pa.Field(nullable=True, description="Ubiquitination")
    signal_peptide: Series[bool] = pa.Field(nullable=True, description="Signal peptide")
    propeptide: Series[bool] = pa.Field(nullable=True, description="Propeptide")
    
    # Database families and domains
    GuidetoPHARMACOLOGY: Series[str] = pa.Field(nullable=True, description="Guide to Pharmacology")
    family: Series[str] = pa.Field(nullable=True, description="Protein family")
    SUPFAM: Series[str] = pa.Field(nullable=True, description="SUPFAM")
    PROSITE: Series[str] = pa.Field(nullable=True, description="PROSITE")
    InterPro: Series[str] = pa.Field(nullable=True, description="InterPro")
    Pfam: Series[str] = pa.Field(nullable=True, description="Pfam")
    PRINTS: Series[str] = pa.Field(nullable=True, description="PRINTS")
    TCDB: Series[str] = pa.Field(nullable=True, description="TCDB")
    
    # ChEMBL specific fields
    tax_id: Series[str] = pa.Field(nullable=True, description="Taxon ID from ChEMBL")
    species_group_flag: Series[str] = pa.Field(nullable=True, description="Species group flag")
    target_components: Series[str] = pa.Field(nullable=True, description="Target components")
    protein_classifications: Series[str] = pa.Field(nullable=True, description="Protein classifications")
    cross_references: Series[str] = pa.Field(nullable=True, description="Cross references")
    gene_symbol_list: Series[str] = pa.Field(nullable=True, description="Gene symbol list")
    protein_synonym_list: Series[str] = pa.Field(nullable=True, description="Protein synonym list")
    reactions: Series[str] = pa.Field(nullable=True, description="Reactions")
    reaction_ec_numbers: Series[str] = pa.Field(nullable=True, description="Reaction EC numbers")
    
    # Protein class predictions
    protein_class_pred_L1: Series[str] = pa.Field(nullable=True, description="Protein class prediction L1")
    protein_class_pred_L2: Series[str] = pa.Field(nullable=True, description="Protein class prediction L2")
    protein_class_pred_L3: Series[str] = pa.Field(nullable=True, description="Protein class prediction L3")
    protein_class_pred_rule_id: Series[str] = pa.Field(nullable=True, description="Protein class prediction rule ID")
    protein_class_pred_evidence: Series[str] = pa.Field(nullable=True, description="Protein class prediction evidence")
    protein_class_pred_confidence: Series[str] = pa.Field(nullable=True, description="Protein class prediction confidence")
    
    # IUPHAR fields
    iuphar_target_id: Series[str] = pa.Field(nullable=True, description="IUPHAR target ID")
    iuphar_family_id: Series[str] = pa.Field(nullable=True, description="IUPHAR family ID")
    iuphar_type: Series[str] = pa.Field(nullable=True, description="IUPHAR type")
    iuphar_class: Series[str] = pa.Field(nullable=True, description="IUPHAR class")
    iuphar_subclass: Series[str] = pa.Field(nullable=True, description="IUPHAR subclass")
    iuphar_chain: Series[str] = pa.Field(nullable=True, description="IUPHAR chain")
    iuphar_name: Series[str] = pa.Field(nullable=True, description="IUPHAR name")
    iuphar_full_id_path: Series[str] = pa.Field(nullable=True, description="IUPHAR full ID path")
    iuphar_full_name_path: Series[str] = pa.Field(nullable=True, description="IUPHAR full name path")

    class Config:
        strict = False  # allow extra columns from enrichments
        coerce = True


__all__ = ["TargetInputSchema", "TargetNormalizedSchema"]


