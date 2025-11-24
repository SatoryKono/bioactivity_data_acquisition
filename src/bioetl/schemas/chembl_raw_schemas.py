"""Lightweight Pandera schemas for raw Chembl payloads.

These schemas describe the minimal column contract for raw DataFrames
returned by Chembl pipelines immediately after ``extract_all`` and
before domain-specific normalisation.
"""

from __future__ import annotations

from typing import Final

from bioetl.schemas.base_abstract_schema import create_schema
from bioetl.schemas.common_column_factory import SchemaColumnFactory

SCHEMA_VERSION: Final[str] = "0.1.0"

CF = SchemaColumnFactory


# Activity raw schema -----------------------------------------------------

ACTIVITY_RAW_COLUMN_ORDER: list[str] = [
    "activity_id",
]

ActivityRawSchema = create_schema(
    columns={
        "activity_id": CF.int64(nullable=False),
    },
    version=SCHEMA_VERSION,
    name="ActivityRawSchema",
    column_order=ACTIVITY_RAW_COLUMN_ORDER,
)


# Assay raw schema --------------------------------------------------------

ASSAY_RAW_COLUMN_ORDER: list[str] = [
    "assay_chembl_id",
]

AssayRawSchema = create_schema(
    columns={
        "assay_chembl_id": CF.chembl_id(nullable=False, unique=True),
    },
    version=SCHEMA_VERSION,
    name="AssayRawSchema",
    column_order=ASSAY_RAW_COLUMN_ORDER,
)


# Document raw schema -----------------------------------------------------

DOCUMENT_RAW_COLUMN_ORDER: list[str] = [
    "document_chembl_id",
]

DocumentRawSchema = create_schema(
    columns={
        "document_chembl_id": CF.chembl_id(nullable=False, unique=True),
    },
    version=SCHEMA_VERSION,
    name="DocumentRawSchema",
    column_order=DOCUMENT_RAW_COLUMN_ORDER,
)


# Test item raw schema ----------------------------------------------------

TESTITEM_RAW_COLUMN_ORDER: list[str] = [
    "molecule_chembl_id",
]

TestItemRawSchema = create_schema(
    columns={
        "molecule_chembl_id": CF.chembl_id(nullable=False, unique=True),
    },
    version=SCHEMA_VERSION,
    name="TestItemRawSchema",
    column_order=TESTITEM_RAW_COLUMN_ORDER,
)


# Target raw schema -------------------------------------------------------

TARGET_RAW_COLUMN_ORDER: list[str] = [
    "target_chembl_id",
]

TargetRawSchema = create_schema(
    columns={
        "target_chembl_id": CF.chembl_id(nullable=False, unique=True),
    },
    version=SCHEMA_VERSION,
    name="TargetRawSchema",
    column_order=TARGET_RAW_COLUMN_ORDER,
)


__all__ = [
    "SCHEMA_VERSION",
    "ACTIVITY_RAW_COLUMN_ORDER",
    "ASSAY_RAW_COLUMN_ORDER",
    "DOCUMENT_RAW_COLUMN_ORDER",
    "TESTITEM_RAW_COLUMN_ORDER",
    "TARGET_RAW_COLUMN_ORDER",
    "ActivityRawSchema",
    "AssayRawSchema",
    "DocumentRawSchema",
    "TestItemRawSchema",
    "TargetRawSchema",
]
