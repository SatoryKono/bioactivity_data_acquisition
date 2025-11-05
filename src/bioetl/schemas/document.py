"""Pandera schema describing the normalized ChEMBL document dataset."""

from __future__ import annotations

import pandera as pa
from pandera import Check, Column, DataFrameSchema

SCHEMA_VERSION = "1.0.0"

COLUMN_ORDER = (
    "document_chembl_id",
    "title",
    "abstract",
    "doi",
    "doi_clean",
    "pubmed_id",
    "year",
    "journal",
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
)

DocumentSchema = DataFrameSchema(
    {
        "document_chembl_id": Column(  # type: ignore[assignment]
            pa.String,  # type: ignore[arg-type]
            Check.str_matches(r"^CHEMBL\d+$"),  # type: ignore[arg-type]
            nullable=False,
            unique=True,
        ),
        "title": Column(pa.String, nullable=True),  # type: ignore[assignment]
        "abstract": Column(pa.String, nullable=True),  # type: ignore[assignment]
        "doi": Column(pa.String, nullable=True),  # type: ignore[assignment]
        "doi_clean": Column(  # type: ignore[assignment]
            pa.String,  # type: ignore[arg-type]
            Check.str_matches(r"^10\.\d{4,9}/\S+$"),  # type: ignore[arg-type]
            nullable=True,
        ),
        "pubmed_id": Column(  # type: ignore[assignment]
            pa.Int64,  # type: ignore[arg-type]
            Check.ge(1),  # type: ignore[arg-type]
            nullable=True,
        ),
        "year": Column(  # type: ignore[assignment]
            pa.Int64,  # type: ignore[arg-type]
            checks=[Check.ge(1500), Check.le(2100)],  # type: ignore[arg-type]
            nullable=True,
        ),
        "journal": Column(pa.String, nullable=True),  # type: ignore[assignment]
        "journal_abbrev": Column(pa.String, nullable=True),  # type: ignore[assignment]
        "volume": Column(pa.String, nullable=True),  # type: ignore[assignment]
        "issue": Column(pa.String, nullable=True),  # type: ignore[assignment]
        "first_page": Column(pa.String, nullable=True),  # type: ignore[assignment]
        "last_page": Column(pa.String, nullable=True),  # type: ignore[assignment]
        "authors": Column(pa.String, nullable=True),  # type: ignore[assignment]
        "authors_count": Column(  # type: ignore[assignment]
            pa.Int64,  # type: ignore[arg-type]
            Check.ge(0),  # type: ignore[arg-type]
            nullable=True,
        ),
        "source": Column(pa.String, Check.eq("ChEMBL"), nullable=False),  # type: ignore[assignment]
        "hash_business_key": Column(pa.String, Check.str_length(64, 64), nullable=False),  # type: ignore[assignment]
        "hash_row": Column(pa.String, Check.str_length(64, 64), nullable=False),  # type: ignore[assignment]
    },
    ordered=True,
    coerce=False,
    name=f"DocumentSchema_v{SCHEMA_VERSION}",
)

__all__ = ["SCHEMA_VERSION", "COLUMN_ORDER", "DocumentSchema"]

