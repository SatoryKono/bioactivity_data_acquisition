"""Pandera schema describing the normalized ChEMBL document dataset."""

from __future__ import annotations

from bioetl.schemas.base_abstract_schema import create_schema
from bioetl.schemas.common_column_factory import SchemaColumnFactory

SCHEMA_VERSION = "1.1.0"

COLUMN_ORDER: list[str] = [
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
]

REQUIRED_FIELDS: list[str] = [
    "document_chembl_id",
    "source",
    "hash_business_key",
    "hash_row",
    "load_meta_id",
]

BUSINESS_KEY_FIELDS: list[str] = [
    "document_chembl_id",
]

ROW_HASH_FIELDS: list[str] = [
    column for column in COLUMN_ORDER if column not in {"hash_row", "hash_business_key"}
]

CF = SchemaColumnFactory

DocumentSchema = create_schema(
    columns={
        "document_chembl_id": CF.chembl_id(nullable=False, unique=True),
        "doc_type": CF.string(),
        "journal": CF.string(),
        "journal_full_title": CF.string(),
        "doi": CF.string(),
        "src_id": CF.string(),
        "title": CF.string(),
        "abstract": CF.string(),
        "doi_clean": CF.doi(),
        "pubmed_id": CF.int64(ge=1),
        "year": CF.int64(ge=1500, le=2100),
        "volume": CF.string(),
        "issue": CF.string(),
        "first_page": CF.string(),
        "last_page": CF.string(),
        "authors": CF.string(),
        "authors_count": CF.int64(ge=0),
        "source": CF.string(isin={"ChEMBL"}, nullable=False),
        "hash_business_key": CF.string(length=(64, 64), nullable=False),
        "hash_row": CF.string(length=(64, 64), nullable=False),
        "term": CF.string(),
        "weight": CF.string(),
        "load_meta_id": CF.uuid(nullable=False),
    },
    version=SCHEMA_VERSION,
    name="DocumentSchema",
    column_order=COLUMN_ORDER,
)

__all__ = [
    "SCHEMA_VERSION",
    "COLUMN_ORDER",
    "REQUIRED_FIELDS",
    "BUSINESS_KEY_FIELDS",
    "ROW_HASH_FIELDS",
    "DocumentSchema",
]
