"""Pandera schema registry used by BioETL pipelines."""

from __future__ import annotations

from collections.abc import Mapping, MutableMapping, Sequence
from dataclasses import dataclass
from importlib import import_module

import pandera.pandas as pa

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


def _register_builtin_schema(
    *,
    identifier: str,
    module_path: str,
    schema_attr: str,
    version_attr: str,
    column_order_attr: str,
) -> None:
    """Import module lazily and register declared schema."""

    module = import_module(module_path)
    schema: pa.DataFrameSchema = getattr(module, schema_attr)
    version = getattr(module, version_attr)
    column_order = getattr(module, column_order_attr)
    name = getattr(schema, "name", identifier.split(".")[-1])

    SCHEMA_REGISTRY.register(
        identifier,
        schema=schema,
        version=str(version),
        column_order=column_order,
        name=name,
    )


_register_builtin_schema(
    identifier="bioetl.schemas.activity.activity_chembl.ActivitySchema",
    module_path="bioetl.schemas.activity.activity_chembl",
    schema_attr="ActivitySchema",
    version_attr="SCHEMA_VERSION",
    column_order_attr="COLUMN_ORDER",
)
_register_builtin_schema(
    identifier="bioetl.schemas.testitem.testitem_chembl.TestItemSchema",
    module_path="bioetl.schemas.testitem.testitem_chembl",
    schema_attr="TestItemSchema",
    version_attr="SCHEMA_VERSION",
    column_order_attr="COLUMN_ORDER",
)
_register_builtin_schema(
    identifier="bioetl.schemas.assay.assay_chembl.AssaySchema",
    module_path="bioetl.schemas.assay.assay_chembl",
    schema_attr="AssaySchema",
    version_attr="SCHEMA_VERSION",
    column_order_attr="COLUMN_ORDER",
)
_register_builtin_schema(
    identifier="bioetl.schemas.document.document_chembl.DocumentSchema",
    module_path="bioetl.schemas.document.document_chembl",
    schema_attr="DocumentSchema",
    version_attr="SCHEMA_VERSION",
    column_order_attr="COLUMN_ORDER",
)
_register_builtin_schema(
    identifier="bioetl.schemas.target.target_chembl.TargetSchema",
    module_path="bioetl.schemas.target.target_chembl",
    schema_attr="TargetSchema",
    version_attr="SCHEMA_VERSION",
    column_order_attr="COLUMN_ORDER",
)
_register_builtin_schema(
    identifier="bioetl.schemas.load_meta.LoadMetaSchema",
    module_path="bioetl.schemas.load_meta",
    schema_attr="LoadMetaSchema",
    version_attr="SCHEMA_VERSION",
    column_order_attr="COLUMN_ORDER",
)


def get_schema(identifier: str) -> SchemaRegistryEntry:
    """Return the schema entry identified by ``identifier``."""

    return SCHEMA_REGISTRY.get(identifier)
