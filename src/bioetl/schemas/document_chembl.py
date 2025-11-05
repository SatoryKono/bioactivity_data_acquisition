"""Pandera schema describing the normalized ChEMBL document dataset."""

from __future__ import annotations

import pandera as pa
from pandera import Check, Column

from ._common import (
    chembl_id_column,
    create_chembl_schema,
    doi_column,
    hash_column,
    non_negative_int_column,
    positive_int_column,
    standard_string_column,
)

SCHEMA_VERSION = "1.0.0"

COLUMN_ORDER = (
    "document_chembl_id",
    "doc_type",
    "journal",
    "journal_full_title",
    "doi",
    "doi_chembl",
    "src_id",
    "title",
    "abstract",
    "doi_clean",
    "pubmed_id",
    "year",
    "journal_abbrev",
    "volume",
    "issue",
    "first_page",
    "last_page",
    "authors",
    "authors_count",
    "source",
    "hash_business_key",
    "hash_row",
    "term",
    "weight",
)

DocumentSchema = create_chembl_schema(
    {
        "document_chembl_id": chembl_id_column(nullable=False, unique=True),
        "doc_type": standard_string_column(),
        "journal": standard_string_column(),
        "journal_full_title": standard_string_column(),
        "doi": standard_string_column(),
        "doi_chembl": standard_string_column(),
        "src_id": standard_string_column(),
        "title": standard_string_column(),
        "abstract": standard_string_column(),
        "doi_clean": doi_column(),
        "pubmed_id": positive_int_column(),
        "year": Column(  # type: ignore[assignment]
            pa.Int64,  # type: ignore[arg-type]
            checks=[Check.ge(1500), Check.le(2100)],  # type: ignore[arg-type]
            nullable=True,
        ),
        "journal_abbrev": standard_string_column(),
        "volume": standard_string_column(),
        "issue": standard_string_column(),
        "first_page": standard_string_column(),
        "last_page": standard_string_column(),
        "authors": standard_string_column(),
        "authors_count": non_negative_int_column(),
        "source": Column(pa.String, Check.eq("ChEMBL"), nullable=False),  # type: ignore[assignment]
        "hash_business_key": hash_column(nullable=False),
        "hash_row": hash_column(nullable=False),
        "term": standard_string_column(),
        "weight": standard_string_column(),
    },
    schema_name="DocumentSchema",
    version=SCHEMA_VERSION,
)

__all__ = ["SCHEMA_VERSION", "COLUMN_ORDER", "DocumentSchema"]

