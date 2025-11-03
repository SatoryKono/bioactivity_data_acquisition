"""Pandera schema for UniProt pipeline output."""

from __future__ import annotations

import pandas as pd

from bioetl.pandera_pandas import pa
from bioetl.pandera_typing import Series
from bioetl.schemas.base import BaseSchema


class UniProtSchema(BaseSchema):
    """Schema describing UniProt enrichment output."""

    uniprot_accession: Series[str] = pa.Field(nullable=False)
    gene_symbol: Series[str] = pa.Field(nullable=True)
    organism: Series[str] = pa.Field(nullable=True)
    taxonomy_id: Series[pd.Int64Dtype] = pa.Field(nullable=True)

    uniprot_canonical_accession: Series[str] = pa.Field(nullable=True)
    uniprot_merge_strategy: Series[str] = pa.Field(nullable=True)
    uniprot_gene_primary: Series[str] = pa.Field(nullable=True)
    uniprot_gene_synonyms: Series[str] = pa.Field(nullable=True)
    uniprot_protein_name: Series[str] = pa.Field(nullable=True)
    uniprot_sequence_length: Series[pd.Int64Dtype] = pa.Field(nullable=True, ge=0)
    uniprot_taxonomy_id: Series[pd.Int64Dtype] = pa.Field(nullable=True)
    uniprot_taxonomy_name: Series[str] = pa.Field(nullable=True)
    uniprot_lineage: Series[str] = pa.Field(nullable=True)
    uniprot_secondary_accessions: Series[str] = pa.Field(nullable=True)
    ortholog_source: Series[str] = pa.Field(nullable=True)

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
        "uniprot_accession",
        "gene_symbol",
        "organism",
        "taxonomy_id",
        "uniprot_canonical_accession",
        "uniprot_merge_strategy",
        "uniprot_gene_primary",
        "uniprot_gene_synonyms",
        "uniprot_protein_name",
        "uniprot_sequence_length",
        "uniprot_taxonomy_id",
        "uniprot_taxonomy_name",
        "uniprot_lineage",
        "uniprot_secondary_accessions",
        "ortholog_source",
    ]
