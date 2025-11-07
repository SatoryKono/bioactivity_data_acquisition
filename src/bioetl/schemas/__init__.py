"""Pandera schema registry used by BioETL pipelines."""

from __future__ import annotations

from dataclasses import dataclass
from importlib import import_module
from typing import Mapping, MutableMapping, Sequence

import pandera as pa

__all__ = ["SchemaRegistryEntry", "SchemaRegistry", "SCHEMA_REGISTRY", "get_schema"]


@dataclass(frozen=True, slots=True)
class SchemaRegistryEntry:
    """Resolved Pandera schema metadata."""

    identifier: str
    name: str
    version: str
    column_order: tuple[str, ...]
    schema: pa.DataFrameSchema


class SchemaRegistry:
    """In-memory registry of Pandera schemas."""

    def __init__(self) -> None:
        self._entries: MutableMapping[str, SchemaRegistryEntry] = {}

    def register(
        self,
        identifier: str,
        *,
        schema: pa.DataFrameSchema,
        version: str,
        column_order: Sequence[str] | None = None,
        name: str | None = None,
    ) -> SchemaRegistryEntry:
        """Register a schema and return the created entry."""

        if identifier in self._entries:
            msg = f"Schema '{identifier}' is already registered"
            raise ValueError(msg)
        normalized_order = tuple(column_order or tuple(schema.columns.keys()))
        if len(set(normalized_order)) != len(normalized_order):
            msg = f"Schema '{identifier}' column order contains duplicates"
            raise ValueError(msg)
        missing_columns = [name for name in normalized_order if name not in schema.columns]
        if missing_columns:
            msg = (
                f"Schema '{identifier}' column order references missing columns: {missing_columns}"
            )
            raise ValueError(msg)
        entry = SchemaRegistryEntry(
            identifier=identifier,
            name=name or schema.name or identifier.split(".")[-1],
            version=version,
            column_order=normalized_order,
            schema=schema,
        )
        self._entries[identifier] = entry
        return entry

    def get(self, identifier: str) -> SchemaRegistryEntry:
        """Return a registered schema, loading it dynamically if required."""

        if identifier in self._entries:
            return self._entries[identifier]

        module_path, attr = _split_identifier(identifier)
        module = import_module(module_path)
        schema = getattr(module, attr)
        version = getattr(module, "SCHEMA_VERSION", "0.0.0")
        column_order = getattr(module, "COLUMN_ORDER", tuple(getattr(schema, "columns", {}).keys()))
        name = getattr(schema, "name", attr)

        if not isinstance(schema, pa.DataFrameSchema):
            msg = f"Object '{identifier}' is not a pandera.DataFrameSchema"
            raise TypeError(msg)

        return self.register(
            identifier,
            schema=schema,
            version=str(version),
            column_order=column_order,
            name=name,
        )

    def as_mapping(self) -> Mapping[str, SchemaRegistryEntry]:
        """Return a read-only mapping view of registered schemas."""

        return dict(self._entries)


def _split_identifier(identifier: str) -> tuple[str, str]:
    """Return module path and attribute name for ``identifier``."""

    if ":" in identifier:
        module_path, attr = identifier.split(":", 1)
    else:
        module_path, _, attr = identifier.rpartition(".")
    if not module_path or not attr:
        msg = f"Invalid schema identifier '{identifier}'"
        raise ValueError(msg)
    return module_path, attr


SCHEMA_REGISTRY = SchemaRegistry()

# Register built-in schemas.
from .activity import (  # noqa: E402  (import after registry definition)
    ActivitySchema,
    COLUMN_ORDER as ACTIVITY_COLUMN_ORDER,
    SCHEMA_VERSION as ACTIVITY_SCHEMA_VERSION,
)

SCHEMA_REGISTRY.register(
    "bioetl.schemas.activity.activity_chembl.ActivitySchema",
    schema=ActivitySchema,
    version=ACTIVITY_SCHEMA_VERSION,
    column_order=ACTIVITY_COLUMN_ORDER,
    name=ActivitySchema.name,
)

# Register testitem schema
from .testitem import (  # noqa: E402  (import after registry definition)
    TestItemSchema,
    COLUMN_ORDER as TESTITEM_COLUMN_ORDER,
    SCHEMA_VERSION as TESTITEM_SCHEMA_VERSION,
)

SCHEMA_REGISTRY.register(
    "bioetl.schemas.testitem.testitem_chembl.TestItemSchema",
    schema=TestItemSchema,
    version=TESTITEM_SCHEMA_VERSION,
    column_order=TESTITEM_COLUMN_ORDER,
    name=TestItemSchema.name,
)

# Register assay schema
from .assay import (  # noqa: E402  (import after registry definition)
    AssaySchema,
    COLUMN_ORDER as ASSAY_COLUMN_ORDER,
    SCHEMA_VERSION as ASSAY_SCHEMA_VERSION,
)

SCHEMA_REGISTRY.register(
    "bioetl.schemas.assay.assay_chembl.AssaySchema",
    schema=AssaySchema,
    version=ASSAY_SCHEMA_VERSION,
    column_order=ASSAY_COLUMN_ORDER,
    name=AssaySchema.name,
)

# Register document schema
from .document import (  # noqa: E402  (import after registry definition)
    DocumentSchema,
    COLUMN_ORDER as DOCUMENT_COLUMN_ORDER,
    SCHEMA_VERSION as DOCUMENT_SCHEMA_VERSION,
)

SCHEMA_REGISTRY.register(
    "bioetl.schemas.document.document_chembl.DocumentSchema",
    schema=DocumentSchema,
    version=DOCUMENT_SCHEMA_VERSION,
    column_order=DOCUMENT_COLUMN_ORDER,
    name=DocumentSchema.name,
)

# Register target schema
from .target import (  # noqa: E402  (import after registry definition)
    TargetSchema,
    COLUMN_ORDER as TARGET_COLUMN_ORDER,
    SCHEMA_VERSION as TARGET_SCHEMA_VERSION,
)

SCHEMA_REGISTRY.register(
    "bioetl.schemas.target.target_chembl.TargetSchema",
    schema=TargetSchema,
    version=TARGET_SCHEMA_VERSION,
    column_order=TARGET_COLUMN_ORDER,
    name=TargetSchema.name,
)

# Register load_meta schema
from .load_meta import (  # noqa: E402  (import after registry definition)
    LoadMetaSchema,
    COLUMN_ORDER as LOAD_META_COLUMN_ORDER,
    SCHEMA_VERSION as LOAD_META_SCHEMA_VERSION,
)

SCHEMA_REGISTRY.register(
    "bioetl.schemas.load_meta.LoadMetaSchema",
    schema=LoadMetaSchema,
    version=LOAD_META_SCHEMA_VERSION,
    column_order=LOAD_META_COLUMN_ORDER,
    name=LoadMetaSchema.name,
)


def get_schema(identifier: str) -> SchemaRegistryEntry:
    """Return the schema entry identified by ``identifier``."""

    return SCHEMA_REGISTRY.get(identifier)
