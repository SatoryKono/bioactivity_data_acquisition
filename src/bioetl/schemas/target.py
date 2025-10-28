"""Pandera schemas for Target data."""

import pandera as pa
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
    taxonomy: Series[int] = pa.Field(nullable=True, ge=1)

    # Cross-references
    hgnc_id: Series[str] = pa.Field(nullable=True)

    # UniProt enrichment (optional)
    uniprot_accession: Series[str] = pa.Field(nullable=True)

    # IUPHAR classification (optional)
    iuphar_type: Series[str] = pa.Field(nullable=True)
    iuphar_class: Series[str] = pa.Field(nullable=True)
    iuphar_subclass: Series[str] = pa.Field(nullable=True)

    class Config:
        strict = True
        coerce = True
        ordered = False


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

