"""Pandera schemas for Target data."""

import pandas as pd
import pandera as pa
from pandera.typing import Series

from bioetl.schemas.base import BaseSchema


class TargetSchema(BaseSchema):
    """Schema for ChEMBL Target data."""

    # ChEMBL identifiers
    target_chembl_id: Series[str] = pa.Field(nullable=False)
    pref_name: Series[str] = pa.Field(nullable=True)
    target_type: Series[str] = pa.Field(nullable=True)

    organism: Series[str] = pa.Field(nullable=True)
    tax_id: Series[pd.Int64Dtype] = pa.Field(nullable=True, ge=1)
    gene_symbol: Series[str] = pa.Field(nullable=True)
    hgnc_id: Series[str] = pa.Field(nullable=True)
    lineage: Series[str] = pa.Field(nullable=True)

    uniprot_id_primary: Series[str] = pa.Field(nullable=True)
    uniprot_ids_all: Series[str] = pa.Field(nullable=True)
    isoform_count: Series[int] = pa.Field(nullable=True, ge=0)
    has_alternative_products: Series[bool] = pa.Field(nullable=True)
    has_uniprot: Series[bool] = pa.Field(nullable=True)
    has_iuphar: Series[bool] = pa.Field(nullable=True)
    uniprot_accession: Series[str] = pa.Field(nullable=True)

    iuphar_type: Series[str] = pa.Field(nullable=True)
    iuphar_class: Series[str] = pa.Field(nullable=True)
    iuphar_subclass: Series[str] = pa.Field(nullable=True)
    data_origin: Series[str] = pa.Field(nullable=True)

    class Config:
        strict = True
        coerce = True
        ordered = False

    _column_order = [
        "target_chembl_id",
        "pref_name",
        "target_type",
        "organism",
        "tax_id",
        "gene_symbol",
        "hgnc_id",
        "lineage",
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
        "pipeline_version",
        "source_system",
        "chembl_release",
        "extracted_at",
        "hash_business_key",
        "hash_row",
        "index",
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
    is_canonical: Series[bool] = pa.Field(nullable=True)

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

