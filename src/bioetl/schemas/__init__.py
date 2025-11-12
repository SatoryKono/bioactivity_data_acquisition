"""Pandera schema registry used by BioETL pipelines."""

from __future__ import annotations

from collections.abc import Mapping, MutableMapping, Sequence
from dataclasses import dataclass
from importlib import import_module

import pandera as pa

from . import (
    chembl_activity_schema,
    chembl_assay_schema,
    chembl_document_schema,
    chembl_target_schema,
    chembl_testitem_schema,
)

__all__ = [
    "SchemaRegistryEntry",
    "SchemaRegistry",
    "SCHEMA_REGISTRY",
    "get_schema",
    "ChemblActivitySchema",
    "ChemblAssaySchema",
    "ChemblDocumentSchema",
    "ChemblTargetSchema",
    "ChemblTestItemSchema",
    "CHEMBL_ACTIVITY_COLUMN_ORDER",
    "CHEMBL_ASSAY_COLUMN_ORDER",
    "CHEMBL_DOCUMENT_COLUMN_ORDER",
    "CHEMBL_TARGET_COLUMN_ORDER",
    "CHEMBL_TESTITEM_COLUMN_ORDER",
    "CHEMBL_ACTIVITY_STANDARD_TYPES",
    "CHEMBL_ACTIVITY_RELATIONS",
    "CHEMBL_ACTIVITY_PROPERTY_KEYS",
]

ChemblActivitySchema = chembl_activity_schema.ActivitySchema
CHEMBL_ACTIVITY_COLUMN_ORDER = chembl_activity_schema.COLUMN_ORDER
CHEMBL_ACTIVITY_STANDARD_TYPES = chembl_activity_schema.STANDARD_TYPES
CHEMBL_ACTIVITY_RELATIONS = chembl_activity_schema.RELATIONS
CHEMBL_ACTIVITY_PROPERTY_KEYS = chembl_activity_schema.ACTIVITY_PROPERTY_KEYS

ChemblAssaySchema = chembl_assay_schema.AssaySchema
CHEMBL_ASSAY_COLUMN_ORDER = chembl_assay_schema.COLUMN_ORDER

ChemblDocumentSchema = chembl_document_schema.DocumentSchema
CHEMBL_DOCUMENT_COLUMN_ORDER = chembl_document_schema.COLUMN_ORDER

ChemblTargetSchema = chembl_target_schema.TargetSchema
CHEMBL_TARGET_COLUMN_ORDER = chembl_target_schema.COLUMN_ORDER

ChemblTestItemSchema = chembl_testitem_schema.TestItemSchema
CHEMBL_TESTITEM_COLUMN_ORDER = chembl_testitem_schema.COLUMN_ORDER


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

        if not isinstance(version, str):
            msg = f"Schema '{identifier}' version must be string."
            raise TypeError(msg)

        if identifier in self._entries:
            msg = f"Schema '{identifier}' is already registered"
            raise ValueError(msg)

        metadata = schema.metadata or {}
        metadata_column_order = metadata.get("column_order")
        metadata_name = metadata.get("name")
        metadata_version = metadata.get("version")

        effective_column_order: Sequence[str] | None = column_order
        if effective_column_order is None and metadata_column_order:
            effective_column_order = tuple(metadata_column_order)

        normalized_order = tuple(effective_column_order or tuple(schema.columns.keys()))
        if len(set(normalized_order)) != len(normalized_order):
            msg = f"Schema '{identifier}' column order contains duplicates"
            raise ValueError(msg)
        missing_columns = [name for name in normalized_order if name not in schema.columns]
        if missing_columns:
            msg = (
                f"Schema '{identifier}' column order references missing columns: {missing_columns}"
            )
            raise ValueError(msg)

        if metadata_column_order is not None and tuple(metadata_column_order) != normalized_order:
            msg = (
                f"Schema '{identifier}' metadata column_order does not match registered order."
            )
            raise ValueError(msg)

        if metadata_version is not None and str(metadata_version) != version:
            msg = (
                f"Schema '{identifier}' metadata version '{metadata_version}' "
                f"does not match declared version '{version}'."
            )
            raise ValueError(msg)

        resolved_name = name or metadata_name or schema.name or identifier.split(".")[-1]
        entry = SchemaRegistryEntry(
            identifier=identifier,
            name=resolved_name,
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
        column_order = getattr(
            module,
            "COLUMN_ORDER",
            schema.metadata.get("column_order") if schema.metadata else None,
        )
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
    identifier="bioetl.schemas.chembl_activity_schema.ActivitySchema",
    module_path="bioetl.schemas.chembl_activity_schema",
    schema_attr="ActivitySchema",
    version_attr="SCHEMA_VERSION",
    column_order_attr="COLUMN_ORDER",
)
_register_builtin_schema(
    identifier="bioetl.schemas.chembl_testitem_schema.TestItemSchema",
    module_path="bioetl.schemas.chembl_testitem_schema",
    schema_attr="TestItemSchema",
    version_attr="SCHEMA_VERSION",
    column_order_attr="COLUMN_ORDER",
)
_register_builtin_schema(
    identifier="bioetl.schemas.chembl_assay_schema.AssaySchema",
    module_path="bioetl.schemas.chembl_assay_schema",
    schema_attr="AssaySchema",
    version_attr="SCHEMA_VERSION",
    column_order_attr="COLUMN_ORDER",
)
_register_builtin_schema(
    identifier="bioetl.schemas.chembl_document_schema.DocumentSchema",
    module_path="bioetl.schemas.chembl_document_schema",
    schema_attr="DocumentSchema",
    version_attr="SCHEMA_VERSION",
    column_order_attr="COLUMN_ORDER",
)
_register_builtin_schema(
    identifier="bioetl.schemas.chembl_target_schema.TargetSchema",
    module_path="bioetl.schemas.chembl_target_schema",
    schema_attr="TargetSchema",
    version_attr="SCHEMA_VERSION",
    column_order_attr="COLUMN_ORDER",
)
_register_builtin_schema(
    identifier="bioetl.schemas.chembl_metadata_schema.LoadMetaSchema",
    module_path="bioetl.schemas.chembl_metadata_schema",
    schema_attr="LoadMetaSchema",
    version_attr="SCHEMA_VERSION",
    column_order_attr="COLUMN_ORDER",
)


def get_schema(identifier: str) -> SchemaRegistryEntry:
    """Return the schema entry identified by ``identifier``."""

    return SCHEMA_REGISTRY.get(identifier)
