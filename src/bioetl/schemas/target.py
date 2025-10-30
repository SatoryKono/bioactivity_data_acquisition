"""Pandera schemas for Target data."""

from __future__ import annotations

import pandas as pd

from bioetl.pandera_pandas import pa
from bioetl.pandera_typing import Series
from bioetl.schemas.base import BaseSchema


class TargetSchema(BaseSchema):
    """Schema for ChEMBL Target data."""

    # ChEMBL identifiers and isoform level details
    target_chembl_id: Series[str] = pa.Field(nullable=False)
    isoform_ids: Series[str] = pa.Field(nullable=True)
    isoform_names: Series[str] = pa.Field(nullable=True)
    isoforms: Series[str] = pa.Field(nullable=True)
    pref_name: Series[str] = pa.Field(nullable=True)
    target_type: Series[str] = pa.Field(nullable=True)

    organism: Series[str] = pa.Field(nullable=True)
    tax_id: Series[pd.Int64Dtype] = pa.Field(nullable=True)
    gene_symbol: Series[str] = pa.Field(nullable=True)
    hgnc_id: Series[str] = pa.Field(nullable=True)
    lineage: Series[str] = pa.Field(nullable=True)

    primaryAccession: Series[str] = pa.Field(nullable=True)
    target_names: Series[str] = pa.Field(nullable=True)
    target_uniprot_id: Series[str] = pa.Field(nullable=True)
    organism_chembl: Series[str] = pa.Field(nullable=True)
    species_group_flag: Series[str] = pa.Field(nullable=True)
    target_components: Series[str] = pa.Field(nullable=True)
    protein_classifications: Series[str] = pa.Field(nullable=True)
    cross_references: Series[str] = pa.Field(nullable=True)
    target_names_chembl: Series[str] = pa.Field(nullable=True)
    pH_dependence: Series[str] = pa.Field(nullable=True)
    pH_dependence_chembl: Series[str] = pa.Field(nullable=True)
    target_organism: Series[str] = pa.Field(nullable=True)
    target_tax_id: Series[str] = pa.Field(nullable=True)
    target_uniprot_accession: Series[str] = pa.Field(nullable=True)
    target_isoform: Series[str] = pa.Field(nullable=True)
    isoform_ids_chembl: Series[str] = pa.Field(nullable=True)
    isoform_names_chembl: Series[str] = pa.Field(nullable=True)
    isoforms_chembl: Series[str] = pa.Field(nullable=True)

    uniprot_accession: Series[str] = pa.Field(nullable=True)
    uniprot_id_primary: Series[str] = pa.Field(nullable=True)
    uniprot_ids_all: Series[str] = pa.Field(nullable=True)
    isoform_count: Series[int] = pa.Field(nullable=True, ge=0)
    has_alternative_products: Series[pd.BooleanDtype] = pa.Field(nullable=True)
    has_uniprot: Series[pd.BooleanDtype] = pa.Field(nullable=True)
    has_iuphar: Series[pd.BooleanDtype] = pa.Field(nullable=True)

    iuphar_type: Series[str] = pa.Field(nullable=True)
    iuphar_class: Series[str] = pa.Field(nullable=True)
    iuphar_subclass: Series[str] = pa.Field(nullable=True)
    data_origin: Series[str] = pa.Field(nullable=True)

    class Config:
        strict = True
        coerce = True
        ordered = False

    _column_order = [
        "index",
        "hash_row",
        "hash_business_key",
        "pipeline_version",
        "run_id",
        "source_system",
        "chembl_release",
        "extracted_at",
        "target_chembl_id",
        "isoform_ids",
        "isoform_names",
        "isoforms",
        "pref_name",
        "target_type",
        "organism",
        "tax_id",
        "gene_symbol",
        "hgnc_id",
        "lineage",
        "primaryAccession",
        "target_names",
        "target_uniprot_id",
        "organism_chembl",
        "species_group_flag",
        "target_components",
        "protein_classifications",
        "cross_references",
        "target_names_chembl",
        "pH_dependence",
        "pH_dependence_chembl",
        "target_organism",
        "target_tax_id",
        "target_uniprot_accession",
        "target_isoform",
        "isoform_ids_chembl",
        "isoform_names_chembl",
        "isoforms_chembl",
        "uniprot_accession",
        "uniprot_id_primary",
        "uniprot_ids_all",
        "isoform_count",
        "has_alternative_products",
        "has_uniprot",
        "has_iuphar",
        "iuphar_type",
        "iuphar_class",
        "iuphar_subclass",
        "data_origin",
    ]


class TargetComponentSchema(BaseSchema):
    """Schema for Target Components."""

    # ChEMBL identifiers
    target_chembl_id: Series[str] = pa.Field(nullable=False)
    component_id: Series[int] = pa.Field(nullable=True, ge=1)
    accession: Series[str] = pa.Field(nullable=True)

    # UniProt enrichment
    gene_symbol: Series[str] = pa.Field(nullable=True)
    sequence_length: Series[int] = pa.Field(nullable=True, ge=0)
    is_canonical: Series[pd.BooleanDtype] = pa.Field(nullable=True)

    class Config:
        strict = True
        coerce = True
        ordered = False


class ProteinClassSchema(BaseSchema):
    """Schema for Protein Class."""

    # ChEMBL identifiers
    target_chembl_id: Series[str] = pa.Field(nullable=False)
    protein_class_id: Series[int] = pa.Field(nullable=True, ge=1)

    # Classification hierarchy
    l1: Series[str] = pa.Field(nullable=True)
    l2: Series[str] = pa.Field(nullable=True)
    l3: Series[str] = pa.Field(nullable=True)
    l4: Series[str] = pa.Field(nullable=True)

    class Config:
        strict = True
        coerce = True
        ordered = False


class XrefSchema(BaseSchema):
    """Schema for Cross-references."""

    # ChEMBL identifiers
    target_chembl_id: Series[str] = pa.Field(nullable=False)

    # Cross-reference
    xref_id: Series[int] = pa.Field(nullable=True, ge=1)
    xref_src_db: Series[str] = pa.Field(nullable=True)
    xref_src_id: Series[str] = pa.Field(nullable=True)

    class Config:
        strict = True
        coerce = True
        ordered = False

