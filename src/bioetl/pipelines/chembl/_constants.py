"""ChEMBL provider-specific reusable constants."""

from __future__ import annotations

__all__ = [
    "API_ACTIVITY_FIELDS",
    "API_DOCUMENT_FIELDS",
    "ASSAY_MUST_HAVE_FIELDS",
    "DOCUMENT_MUST_HAVE_FIELDS",
    "TESTITEM_MUST_HAVE_FIELDS",
]

API_ACTIVITY_FIELDS: tuple[str, ...] = (
    "activity_id",
    "assay_chembl_id",
    "testitem_chembl_id",
    "molecule_chembl_id",
    "target_chembl_id",
    "document_chembl_id",
    "type",
    "relation",
    "value",
    "units",
    "standard_type",
    "standard_relation",
    "standard_value",
    "standard_units",
    "standard_text_value",
    "standard_flag",
    "upper_value",
    "lower_value",
    "pchembl_value",
    "activity_comment",
    "bao_endpoint",
    "bao_format",
    "bao_label",
    "canonical_smiles",
    "ligand_efficiency",
    "target_organism",
    "target_tax_id",
    "data_validity_comment",
    "potential_duplicate",
    "activity_properties",
    "assay_type",
    "assay_description",
    "assay_organism",
    "assay_tax_id",
    "parent_molecule_chembl_id",
    "molecule_pref_name",
    "record_id",
    "src_id",
    "target_pref_name",
    "text_value",
    "standard_upper_value",
    "uo_units",
    "qudt_units",
    "curated_by",
)

API_DOCUMENT_FIELDS: tuple[str, ...] = (
    "document_chembl_id",
    "doc_type",
    "journal",
    "journal_full_title",
    "doi",
    "src_id",
    "title",
    "abstract",
    "year",
    "volume",
    "issue",
    "first_page",
    "last_page",
    "pubmed_id",
    "authors",
)

ASSAY_MUST_HAVE_FIELDS: tuple[str, ...] = (
    "assay_chembl_id",
)

DOCUMENT_MUST_HAVE_FIELDS: tuple[str, ...] = (
    "document_chembl_id",
    "doi",
    "issue",
)

TESTITEM_MUST_HAVE_FIELDS: tuple[str, ...] = (
    "molecule_chembl_id",
    "pref_name",
    "molecule_type",
    "availability_type",
    "chirality",
    "first_approval",
    "first_in_class",
    "indication_class",
    "helm_notation",
    "molecule_properties",
    "molecule_structures",
    "molecule_hierarchy",
)

