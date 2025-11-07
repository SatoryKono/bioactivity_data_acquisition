"""Pandera schema describing the normalized ChEMBL document dataset."""

from __future__ import annotations

from bioetl.schemas.base import create_schema
from bioetl.schemas.common import (
    chembl_id_column,
    doi_column,
    nullable_int64_column,
    nullable_string_column,
    string_column_with_check,
    uuid_column,
)

SCHEMA_VERSION = "1.1.0"

COLUMN_ORDER = (
    "document_chembl_id",
    "doc_type",
    "journal",
    "journal_full_title",
    "doi",
    "src_id",
    "title",
    "abstract",
    "doi_clean",
    "pubmed_id",
    "year",
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
    "load_meta_id",
)

DocumentSchema = create_schema(
    columns={
        "document_chembl_id": chembl_id_column(nullable=False, unique=True),
        "doc_type": nullable_string_column(),
        "journal": nullable_string_column(),
        "journal_full_title": nullable_string_column(),
        "doi": nullable_string_column(),
        "src_id": nullable_string_column(),
        "title": nullable_string_column(),
        "abstract": nullable_string_column(),
        "doi_clean": doi_column(),
        "pubmed_id": nullable_int64_column(ge=1),
        "year": nullable_int64_column(ge=1500, le=2100),
        "volume": nullable_string_column(),
        "issue": nullable_string_column(),
        "first_page": nullable_string_column(),
        "last_page": nullable_string_column(),
        "authors": nullable_string_column(),
        "authors_count": nullable_int64_column(ge=0),
        "source": string_column_with_check(isin={"ChEMBL"}, nullable=False),
        "hash_business_key": string_column_with_check(str_length=(64, 64), nullable=False),
        "hash_row": string_column_with_check(str_length=(64, 64), nullable=False),
        "term": nullable_string_column(),
        "weight": nullable_string_column(),
        "load_meta_id": uuid_column(nullable=False),
    },
    version=SCHEMA_VERSION,
    name="DocumentSchema",
)

__all__ = ["SCHEMA_VERSION", "COLUMN_ORDER", "DocumentSchema"]
