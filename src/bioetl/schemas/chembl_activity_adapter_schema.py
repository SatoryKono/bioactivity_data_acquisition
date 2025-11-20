"""Lightweight Pandera schema for the simplified ChemblActivityPipeline adapter.

This schema is intentionally narrower than the full ActivitySchema: it only
covers the columns produced by the simplified adapter pipeline and required
for determinism and hashing in golden tests.
"""

from __future__ import annotations

from bioetl.schemas import base_abstract_schema, common_column_factory

SCHEMA_VERSION = "0.1.0"

CF = common_column_factory.SchemaColumnFactory
row_meta = CF.row_metadata()

COLUMN_ORDER: list[str] = [
    "activity_id",
    "row_subtype",
    "row_index",
    "assay_id",
    "value",
    "is_active",
    "hash_row",
    "hash_business_key",
    "load_meta_id",
]

REQUIRED_FIELDS: list[str] = [
    "activity_id",
    "row_subtype",
    "row_index",
    "load_meta_id",
    "hash_row",
]

BUSINESS_KEY_FIELDS: list[str] = [
    "activity_id",
]

# For the adapter we keep row hash fields simple: all columns ordered except hashes
ROW_HASH_FIELDS: list[str] = [
    name
    for name in COLUMN_ORDER
    if name not in {"hash_row", "hash_business_key"}
]

ActivityAdapterSchema = base_abstract_schema.create_schema(
    columns={
        "activity_id": CF.int64(nullable=False),
        **row_meta,
        "assay_id": CF.string(),
        "value": CF.float64(),
        "is_active": CF.boolean_flag(),
        "hash_row": CF.string(length=(64, 64), nullable=False),
        "hash_business_key": CF.string(length=(64, 64)),
        "load_meta_id": CF.uuid(nullable=False),
    },
    version=SCHEMA_VERSION,
    name="ActivityAdapterSchema",
    column_order=COLUMN_ORDER,
)

__all__ = [
    "SCHEMA_VERSION",
    "COLUMN_ORDER",
    "REQUIRED_FIELDS",
    "BUSINESS_KEY_FIELDS",
    "ROW_HASH_FIELDS",
    "ActivityAdapterSchema",
]
