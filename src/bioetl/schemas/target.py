"""Pandera schemas for Target data."""

import pandas as pd
import pandera.pandas as pa
from pandera.typing import Series

from bioetl.schemas.base import BaseSchema


class TargetSchema(BaseSchema):
    """Schema for ChEMBL Target data."""

    # ChEMBL identifiers
    target_chembl_id: Series[str] = pa.Field(nullable=False)
    pref_name: Series[str] = pa.Field(nullable=True)
    target_type: Series[str] = pa.Field(nullable=True)

    # Organism
    organism: Series[str] = pa.Field(nullable=True)
    tax_id: Series[pd.Int64Dtype] = pa.Field(nullable=True, ge=1)
    lineage: Series[str] = pa.Field(nullable=True)

    # Cross-references
    hgnc_id: Series[str] = pa.Field(nullable=True)
    gene_symbol: Series[str] = pa.Field(nullable=True)

    # UniProt enrichment (optional)
    uniprot_id_primary: Series[str] = pa.Field(nullable=True)
    uniprot_ids_all: Series[str] = pa.Field(nullable=True)
    isoform_count: Series[pd.Int64Dtype] = pa.Field(nullable=True, ge=0)
    has_alternative_products: Series[bool] = pa.Field(nullable=True)
    has_uniprot: Series[bool] = pa.Field(nullable=True)
    has_iuphar: Series[bool] = pa.Field(nullable=True)

    # IUPHAR classification (optional)
    iuphar_type: Series[str] = pa.Field(nullable=True)
    iuphar_class: Series[str] = pa.Field(nullable=True)
    iuphar_subclass: Series[str] = pa.Field(nullable=True)

    # Provenance
    data_origin: Series[str] = pa.Field(nullable=True)

    class Config(BaseSchema.Config):
        strict = False
        coerce = True
        ordered = False


class TargetComponentSchema(BaseSchema):
    """Schema for Target Components."""

    # ChEMBL identifiers
    target_chembl_id: Series[str] = pa.Field(nullable=False)
    component_id: Series[str] = pa.Field(nullable=True)
    accession: Series[str] = pa.Field(nullable=True)
    canonical_accession: Series[str] = pa.Field(nullable=True)
    isoform_accession: Series[str] = pa.Field(nullable=True)
    isoform_name: Series[str] = pa.Field(nullable=True)

    # UniProt enrichment
    gene_symbol: Series[str] = pa.Field(nullable=True)
    sequence_length: Series[int] = pa.Field(nullable=True, ge=0)
    is_canonical: Series[bool] = pa.Field(nullable=True)
    data_origin: Series[str] = pa.Field(nullable=True)
    merge_rank: Series[int] = pa.Field(nullable=True, ge=0)

    class Config(BaseSchema.Config):
        strict = False
        coerce = True
        ordered = False


class ProteinClassSchema(BaseSchema):
    """Schema for Protein Class."""

    # ChEMBL identifiers
    target_chembl_id: Series[str] = pa.Field(nullable=False)
    class_level: Series[str] = pa.Field(nullable=True)
    class_name: Series[str] = pa.Field(nullable=True)
    full_path: Series[str] = pa.Field(nullable=True)

    class Config(BaseSchema.Config):
        strict = False
        coerce = True
        ordered = False


class XrefSchema(BaseSchema):
    """Schema for Cross-references."""

    # ChEMBL identifiers
    target_chembl_id: Series[str] = pa.Field(nullable=False)

    # Cross-reference
    xref_id: Series[str] = pa.Field(nullable=True)
    xref_src_db: Series[str] = pa.Field(nullable=True)
    component_id: Series[str] = pa.Field(nullable=True)

    class Config(BaseSchema.Config):
        strict = False
        coerce = True
        ordered = False

