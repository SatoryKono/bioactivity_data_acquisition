"""Pandera schema registry used by BioETL pipelines."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, MutableMapping, Sequence
from dataclasses import dataclass, field
from importlib import import_module

import pandera as pa

from bioetl.core.schema.vocabulary_bindings import SchemaVocabularyBinding

from . import (
    activity,
    chembl_activity_schema,
    chembl_assay_schema,
    chembl_document_schema,
    chembl_target_schema,
    chembl_testitem_schema,
)
from .migrations import load_builtin_migrations
from .versioning import (
    SCHEMA_MIGRATION_REGISTRY,
    SchemaMigration,
    SchemaMigrationError,
    SchemaMigrationPathError,
    SchemaMigrationRegistry,
    SchemaVersionMismatchError,
)

__all__ = [
    "SchemaDescriptor",
    "SchemaRegistryEntry",
    "SchemaRegistry",
    "SCHEMA_REGISTRY",
    "get_schema",
    "ActivitySchema",
    "ChemblActivitySchema",
    "ChemblAssaySchema",
    "ChemblDocumentSchema",
    "ChemblTargetSchema",
    "ChemblTestItemSchema",
    "ACTIVITY_COLUMN_ORDER",
    "CHEMBL_ACTIVITY_COLUMN_ORDER",
    "CHEMBL_ASSAY_COLUMN_ORDER",
    "CHEMBL_DOCUMENT_COLUMN_ORDER",
    "CHEMBL_TARGET_COLUMN_ORDER",
    "CHEMBL_TESTITEM_COLUMN_ORDER",
    "ACTIVITY_RELATIONS",
    "CHEMBL_ACTIVITY_RELATIONS",
    "ACTIVITY_PROPERTY_KEYS",
    "CHEMBL_ACTIVITY_PROPERTY_KEYS",
    "SchemaMigration",
    "SchemaMigrationRegistry",
    "SchemaMigrationError",
    "SchemaMigrationPathError",
    "SchemaVersionMismatchError",
    "SCHEMA_MIGRATION_REGISTRY",
]

ActivitySchema = activity.ActivitySchema
ACTIVITY_COLUMN_ORDER = activity.COLUMN_ORDER
ACTIVITY_RELATIONS = activity.RELATIONS
ACTIVITY_PROPERTY_KEYS = activity.ACTIVITY_PROPERTY_KEYS

ChemblActivitySchema = chembl_activity_schema.ActivitySchema
CHEMBL_ACTIVITY_COLUMN_ORDER = chembl_activity_schema.COLUMN_ORDER
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


def _clone_schema_with_metadata(
    schema: pa.DataFrameSchema,
    extra_metadata: Mapping[str, object],
) -> pa.DataFrameSchema:
    merged_metadata: dict[str, object] = {}
    if schema.metadata:
        merged_metadata.update(schema.metadata)
    merged_metadata.update(dict(extra_metadata))

    unique_value = schema.unique
    if isinstance(unique_value, list):
        unique_arg: object = list(unique_value)
    else:
        unique_arg = unique_value

    checks = list(schema.checks) if schema.checks is not None else None
    parsers = list(schema.parsers) if schema.parsers is not None else None

    return pa.DataFrameSchema(
        columns=dict(schema.columns),
        checks=checks,
        index=schema.index,
        dtype=schema.dtype,
        coerce=schema.coerce,
        strict=schema.strict,
        ordered=schema.ordered,
        unique=unique_arg,
        report_duplicates=schema.report_duplicates,
        unique_column_names=schema.unique_column_names,
        add_missing_columns=schema.add_missing_columns,
        drop_invalid_rows=schema.drop_invalid_rows,
        name=schema.name,
        title=schema.title,
        description=schema.description,
        parsers=parsers,
        metadata=merged_metadata,
    )


if not hasattr(pa.DataFrameSchema, "set_metadata"):

    def _set_metadata(  # type: ignore[override]
        self: pa.DataFrameSchema,
        metadata: Mapping[str, object],
    ) -> pa.DataFrameSchema:
        if not isinstance(metadata, Mapping):
            msg = "metadata must be a mapping"
            raise TypeError(msg)
        return _clone_schema_with_metadata(self, metadata)

    pa.DataFrameSchema.set_metadata = _set_metadata  # type: ignore[attr-defined]


@dataclass(frozen=True, slots=True)
class SchemaDescriptor:
    """Aggregated Pandera schema descriptor with attached metadata."""

    identifier: str
    schema: pa.DataFrameSchema
    version: str
    name: str
    column_order: tuple[str, ...] = field(default_factory=tuple)
    business_key_fields: tuple[str, ...] = field(default_factory=tuple)
    required_fields: tuple[str, ...] = field(default_factory=tuple)
    row_hash_fields: tuple[str, ...] = field(default_factory=tuple)
    description: str | None = None
    dataset_type: str | None = None
    vocabulary_bindings: tuple[SchemaVocabularyBinding, ...] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        if not self.identifier:
            msg = "Schema descriptor identifier must be a non-empty string."
            raise ValueError(msg)
        if not isinstance(self.schema, pa.DataFrameSchema):
            msg = "Schema descriptor requires a pandera.DataFrameSchema instance."
            raise TypeError(msg)
        if not self.version:
            msg = f"Schema '{self.identifier}' version must be a non-empty string."
            raise ValueError(msg)
        if not self.name:
            msg = f"Schema '{self.identifier}' resolved name must be provided."
            raise ValueError(msg)

        order = self.column_order
        if not order:
            object.__setattr__(self, "column_order", tuple(self.schema.columns.keys()))
            order = self.column_order

        duplicates = {column for column in order if order.count(column) > 1}
        if duplicates:
            msg = (
                f"Schema '{self.identifier}' column order contains duplicates: "
                f"{sorted(duplicates)}"
            )
            raise ValueError(msg)

        missing_columns = [column for column in order if column not in self.schema.columns]
        if missing_columns:
            msg = (
                f"Schema '{self.identifier}' column order references missing columns: "
                f"{missing_columns}"
            )
            raise ValueError(msg)

        metadata = self.schema.metadata or {}
        metadata_name = metadata.get("name")
        if metadata_name is not None and str(metadata_name) != self.name:
            msg = (
                f"Schema '{self.identifier}' metadata name '{metadata_name}' "
                f"does not match resolved name '{self.name}'."
            )
            raise ValueError(msg)

        metadata_version = metadata.get("version")
        if metadata_version is not None and str(metadata_version) != self.version:
            msg = (
                f"Schema '{self.identifier}' metadata version '{metadata_version}' "
                f"does not match declared version '{self.version}'."
            )
            raise ValueError(msg)

        metadata_order = metadata.get("column_order")
        if metadata_order is not None and tuple(metadata_order) != order:
            msg = (
                f"Schema '{self.identifier}' metadata column_order does not match registered order."
            )
            raise ValueError(msg)

        if "business_key_fields" in metadata:
            metadata_business = _normalize_metadata_sequence(metadata["business_key_fields"])
            if metadata_business != self.business_key_fields:
                msg = (
                    f"Schema '{self.identifier}' metadata business_key_fields "
                    "does not match registered business key fields."
                )
                raise ValueError(msg)

        if "required_fields" in metadata:
            metadata_required = _normalize_metadata_sequence(metadata["required_fields"])
            if metadata_required != self.required_fields:
                msg = (
                    f"Schema '{self.identifier}' metadata required_fields "
                    "does not match registered required fields."
                )
                raise ValueError(msg)

        if "row_hash_fields" in metadata:
            metadata_row_hash = _normalize_metadata_sequence(metadata["row_hash_fields"])
            if metadata_row_hash != self.row_hash_fields:
                msg = (
                    f"Schema '{self.identifier}' metadata row_hash_fields "
                    "does not match registered row hash fields."
                )
                raise ValueError(msg)

        for field_name in self.business_key_fields:
            if field_name not in self.schema.columns:
                msg = (
                    f"Schema '{self.identifier}' business key field '{field_name}' "
                    "is not present in schema columns."
                )
                raise ValueError(msg)
            if field_name not in self.column_order:
                msg = (
                    f"Schema '{self.identifier}' column_order missing business key field '{field_name}'."
                )
                raise ValueError(msg)

        for field_name in self.required_fields:
            if field_name not in self.schema.columns:
                msg = (
                    f"Schema '{self.identifier}' required field '{field_name}' "
                    "is not present in schema columns."
                )
                raise ValueError(msg)
            if field_name not in self.column_order:
                msg = (
                    f"Schema '{self.identifier}' column_order missing required field '{field_name}'."
                )
                raise ValueError(msg)

        for field_name in self.row_hash_fields:
            if field_name not in self.schema.columns:
                msg = (
                    f"Schema '{self.identifier}' row hash field '{field_name}' "
                    "is not present in schema columns."
                )
                raise ValueError(msg)

        for binding in self.vocabulary_bindings:
            if binding.column not in self.schema.columns:
                msg = (
                    f"Schema '{self.identifier}' vocabulary binding references "
                    f"missing column '{binding.column}'."
                )
                raise ValueError(msg)

    @property
    def vocabulary_requirements(self) -> tuple[str, ...]:
        """Return unique vocabulary identifiers declared by the schema."""

        identifiers = {binding.vocabulary_id for binding in self.vocabulary_bindings}
        return tuple(sorted(identifiers))

    @property
    def metadata(self) -> Mapping[str, object]:
        """Return an immutable copy of the schema metadata."""

        return dict(self.schema.metadata or {})

    @classmethod
    def from_components(
        cls,
        *,
        identifier: str,
        schema: pa.DataFrameSchema,
        version: str,
        name: str | None = None,
        column_order: Sequence[str] | None = None,
        business_key_fields: Sequence[str] | None = None,
        required_fields: Sequence[str] | None = None,
        row_hash_fields: Sequence[str] | None = None,
        description: str | None = None,
        dataset_type: str | None = None,
        extra_metadata: Mapping[str, object] | None = None,
    ) -> "SchemaDescriptor":
        """Build a descriptor from schema components and enforce metadata consistency."""

        if not isinstance(schema, pa.DataFrameSchema):
            msg = f"Object '{identifier}' is not a pandera.DataFrameSchema"
            raise TypeError(msg)

        metadata: dict[str, object] = dict(schema.metadata or {})
        if extra_metadata:
            metadata.update(dict(extra_metadata))

        resolved_name = name or str(metadata.get("name")) or schema.name or identifier.split(".")[-1]
        metadata["name"] = resolved_name

        metadata_version = metadata.get("version")
        if metadata_version is not None and str(metadata_version) != str(version):
            msg = (
                f"Schema '{identifier}' metadata version '{metadata_version}' "
                f"does not match declared version '{version}'."
            )
            raise ValueError(msg)
        metadata["version"] = str(version)

        resolved_column_order = _resolve_column_order(schema, column_order, identifier)
        metadata["column_order"] = resolved_column_order

        resolved_business_keys = _resolve_descriptor_sequence(
            metadata, "business_key_fields", business_key_fields
        )
        resolved_required_fields = _resolve_descriptor_sequence(
            metadata, "required_fields", required_fields
        )
        resolved_row_hash_fields = _resolve_descriptor_sequence(
            metadata, "row_hash_fields", row_hash_fields
        )

        if description is not None:
            metadata["description"] = description
        elif "description" in metadata:
            description = str(metadata["description"])

        if dataset_type is not None:
            metadata["dataset_type"] = dataset_type
        elif "dataset_type" in metadata:
            dataset_type = str(metadata["dataset_type"])

        resolved_bindings = _resolve_vocabulary_bindings(schema, metadata)
        if resolved_bindings:
            metadata["vocabulary_bindings"] = [binding.to_metadata() for binding in resolved_bindings]
        elif "vocabulary_bindings" in metadata:
            metadata["vocabulary_bindings"] = []

        normalized_schema = schema.set_metadata(metadata)  # pyright: ignore[reportAttributeAccessIssue]

        description_value = metadata.get("description")
        dataset_type_value = metadata.get("dataset_type")

        return cls(
            identifier=identifier,
            schema=normalized_schema,
            version=str(version),
            name=resolved_name,
            column_order=resolved_column_order,
            business_key_fields=resolved_business_keys,
            required_fields=resolved_required_fields,
            row_hash_fields=resolved_row_hash_fields,
            description=str(description_value) if isinstance(description_value, str) else None,
            dataset_type=str(dataset_type_value) if isinstance(dataset_type_value, str) else None,
            vocabulary_bindings=resolved_bindings,
        )


SchemaRegistryEntry = SchemaDescriptor


def _normalize_optional_sequence(values: Sequence[str] | None) -> tuple[str, ...]:
    if values is None:
        return ()
    return tuple(values)


def _normalize_metadata_sequence(value: object) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        return (value,)
    if isinstance(value, Iterable) and not isinstance(value, (str, bytes)):
        return tuple(str(item) for item in value)
    return ()


def _normalize_binding_statuses(value: object) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, (str, bytes)):
        return (str(value),)
    if isinstance(value, Iterable) and not isinstance(value, (str, bytes)):
        return tuple(str(item) for item in value)
    return ()


def _binding_from_metadata(payload: object) -> SchemaVocabularyBinding | None:
    if not isinstance(payload, Mapping):
        return None
    column = payload.get("column")
    vocabulary_id = payload.get("vocabulary_id") or payload.get("id")
    if not isinstance(column, str) or not column:
        return None
    if not isinstance(vocabulary_id, str) or not vocabulary_id:
        return None
    statuses = _normalize_binding_statuses(payload.get("allowed_statuses"))
    required = bool(payload.get("required", True))
    return SchemaVocabularyBinding(
        column=column,
        vocabulary_id=vocabulary_id,
        allowed_statuses=statuses,
        required=required,
    )


def _resolve_vocabulary_bindings(
    schema: pa.DataFrameSchema,
    metadata: Mapping[str, object],
) -> tuple[SchemaVocabularyBinding, ...]:
    bindings_by_column: dict[str, SchemaVocabularyBinding] = {}
    existing = metadata.get("vocabulary_bindings")
    if isinstance(existing, Iterable) and not isinstance(existing, (str, bytes)):
        for payload in existing:
            binding = _binding_from_metadata(payload)
            if binding is not None:
                bindings_by_column[binding.column] = binding

    for column_name, column in schema.columns.items():
        binding = SchemaVocabularyBinding.from_column_metadata(column_name, column.metadata)
        if binding is not None:
            bindings_by_column[binding.column] = binding

    ordered_bindings: list[SchemaVocabularyBinding] = []
    for column_name in schema.columns:
        if column_name in bindings_by_column:
            ordered_bindings.append(bindings_by_column[column_name])
    return tuple(ordered_bindings)


def _resolve_column_order(
    schema: pa.DataFrameSchema,
    column_order: Sequence[str] | None,
    identifier: str,
) -> tuple[str, ...]:
    if column_order is None:
        metadata = schema.metadata or {}
        metadata_order = metadata.get("column_order")
        if metadata_order is not None:
            column_order = tuple(metadata_order)  # type: ignore[assignment]
    if column_order is None:
        column_order = tuple(schema.columns.keys())
    normalized = tuple(column_order)
    duplicates = {column for column in normalized if normalized.count(column) > 1}
    if duplicates:
        msg = f"Schema '{identifier}' column order contains duplicates: {sorted(duplicates)}"
        raise ValueError(msg)
    missing_columns = [column for column in normalized if column not in schema.columns]
    if missing_columns:
        msg = (
            f"Schema '{identifier}' column order references missing columns: "
            f"{missing_columns}"
        )
        raise ValueError(msg)
    return normalized


def _resolve_descriptor_sequence(
    metadata: dict[str, object],
    key: str,
    values: Sequence[str] | None,
) -> tuple[str, ...]:
    provided = _normalize_optional_sequence(values)
    existing = _normalize_metadata_sequence(metadata.get(key))
    resolved = provided or existing
    if resolved:
        metadata[key] = resolved
    elif key in metadata:
        metadata[key] = ()
    return resolved


class SchemaRegistry:
    """In-memory registry of Pandera schemas."""

    def __init__(self) -> None:
        self._entries: MutableMapping[str, SchemaDescriptor] = {}

    def register(self, descriptor: SchemaDescriptor) -> SchemaDescriptor:
        """Register a schema and return the created descriptor."""

        identifier = descriptor.identifier
        if identifier in self._entries:
            msg = f"Schema '{identifier}' is already registered"
            raise ValueError(msg)

        self._validate_descriptor(descriptor)
        self._entries[identifier] = descriptor
        return descriptor

    def _validate_descriptor(self, descriptor: SchemaDescriptor) -> None:
        schema_columns = list(descriptor.schema.columns.keys())
        column_order = list(descriptor.column_order)

        duplicates = {column for column in column_order if column_order.count(column) > 1}
        if duplicates:
            msg = (
                f"Schema '{descriptor.identifier}' column order contains duplicates: "
                f"{sorted(duplicates)}"
            )
            raise ValueError(msg)

        missing = [name for name in column_order if name not in schema_columns]
        if missing:
            msg = (
                f"Schema '{descriptor.identifier}' column order references missing columns: "
                f"{missing}"
            )
            raise ValueError(msg)

        metadata = descriptor.schema.metadata or {}
        metadata_version = metadata.get("version")
        if metadata_version is not None and str(metadata_version) != descriptor.version:
            msg = (
                f"Schema '{descriptor.identifier}' metadata version '{metadata_version}' "
                f"does not match declared version '{descriptor.version}'."
            )
            raise ValueError(msg)

        metadata_order = metadata.get("column_order")
        if metadata_order is not None and tuple(metadata_order) != tuple(column_order):
            msg = (
                f"Schema '{descriptor.identifier}' metadata column_order does not match registered order."
            )
            raise ValueError(msg)

        metadata_name = metadata.get("name")
        if metadata_name is not None and str(metadata_name) != descriptor.name:
            msg = (
                f"Schema '{descriptor.identifier}' metadata name '{metadata_name}' "
                f"does not match resolved name '{descriptor.name}'."
            )
            raise ValueError(msg)

        for hashed_column in ("hash_row", "hash_business_key"):
            if hashed_column not in schema_columns:
                msg = f"Schema '{descriptor.identifier}' missing required column '{hashed_column}'"
                raise ValueError(msg)
            if hashed_column not in column_order:
                msg = (
                    f"Schema '{descriptor.identifier}' column_order missing required column "
                    f"'{hashed_column}'"
                )
                raise ValueError(msg)

        for field_name in descriptor.required_fields:
            if field_name not in schema_columns:
                msg = (
                    f"Schema '{descriptor.identifier}' required field '{field_name}' "
                    "is not present in schema columns."
                )
                raise ValueError(msg)
        for field_name in descriptor.business_key_fields:
            if field_name not in schema_columns:
                msg = (
                    f"Schema '{descriptor.identifier}' business key field '{field_name}' "
                    "is not present in schema columns."
                )
                raise ValueError(msg)
        for field_name in descriptor.row_hash_fields:
            if field_name not in schema_columns:
                msg = (
                    f"Schema '{descriptor.identifier}' row hash field '{field_name}' "
                    "is not present in schema columns."
                )
                raise ValueError(msg)

    def get(self, identifier: str) -> SchemaDescriptor:
        """Return a registered schema, loading it dynamically if required."""

        if identifier in self._entries:
            return self._entries[identifier]

        module_path, attr = _split_identifier(identifier)
        module = import_module(module_path)
        schema = getattr(module, attr)

        descriptor = SchemaDescriptor.from_components(
            identifier=identifier,
            schema=schema,
            version=str(getattr(module, "SCHEMA_VERSION", "0.0.0")),
            name=getattr(schema, "name", attr),
            column_order=getattr(
                module,
                "COLUMN_ORDER",
                schema.metadata.get("column_order") if schema.metadata else None,
            ),
            business_key_fields=getattr(module, "BUSINESS_KEY_FIELDS", None),
            required_fields=getattr(module, "REQUIRED_FIELDS", None),
            row_hash_fields=getattr(module, "ROW_HASH_FIELDS", None),
            description=getattr(module, "SCHEMA_DESCRIPTION", None),
            dataset_type=getattr(module, "DATASET_TYPE", None),
        )
        return self.register(descriptor)

    def as_mapping(self) -> Mapping[str, SchemaDescriptor]:
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

    descriptor = SchemaDescriptor.from_components(
        identifier=identifier,
        schema=schema,
        version=str(version),
        column_order=column_order,
        name=name,
        business_key_fields=getattr(module, "BUSINESS_KEY_FIELDS", None),
        required_fields=getattr(module, "REQUIRED_FIELDS", None),
        row_hash_fields=getattr(module, "ROW_HASH_FIELDS", None),
        description=getattr(module, "SCHEMA_DESCRIPTION", None),
        dataset_type=getattr(module, "DATASET_TYPE", None),
    )
    SCHEMA_REGISTRY.register(descriptor)


_register_builtin_schema(
    identifier="bioetl.schemas.chembl_activity_schema.ActivitySchema",
    module_path="bioetl.schemas.chembl_activity_schema",
    schema_attr="ActivitySchema",
    version_attr="SCHEMA_VERSION",
    column_order_attr="COLUMN_ORDER",
)
_register_builtin_schema(
    identifier="bioetl.schemas.activity.ActivitySchema",
    module_path="bioetl.schemas.activity",
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

load_builtin_migrations()


def get_schema(identifier: str, *, version: str | None = None) -> SchemaDescriptor:
    """Return the schema descriptor identified by ``identifier``."""

    descriptor = SCHEMA_REGISTRY.get(identifier)
    if version is not None and descriptor.version != version:
        raise SchemaVersionMismatchError(
            schema_identifier=identifier,
            expected_version=version,
            actual_version=descriptor.version,
        )
    return descriptor

