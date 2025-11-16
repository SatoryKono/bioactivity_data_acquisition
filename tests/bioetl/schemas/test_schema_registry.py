"""Schema registry consistency tests."""

from __future__ import annotations

from importlib import import_module

from bioetl.schemas import SCHEMA_REGISTRY
from bioetl.schemas.metadata_utils import metadata_dict, normalize_sequence


def test_registry_column_order_and_versions() -> None:
    """All registered schemas expose consistent metadata."""

    for identifier, entry in SCHEMA_REGISTRY.as_mapping().items():
        schema_columns = list(entry.schema.columns.keys())
        column_order = list(entry.column_order)

        assert len(column_order) == len(
            set(column_order)
        ), f"{identifier} column_order contains duplicates: {column_order}"

        missing = [name for name in column_order if name not in schema_columns]
        assert not missing, f"{identifier} column_order references missing columns: {missing}"

        metadata = metadata_dict(entry.metadata)
        assert metadata.get("name") == entry.name
        assert metadata.get("version") == entry.version
        assert normalize_sequence(metadata.get("column_order")) == entry.column_order

        for hashed_column in ("hash_row", "hash_business_key"):
            assert (
                hashed_column in schema_columns
            ), f"{identifier} missing required column '{hashed_column}'"
            assert (
                hashed_column in column_order
            ), f"{identifier} column_order missing '{hashed_column}'"

        module_path, _ = identifier.rsplit(".", 1)
        module = import_module(module_path)
        declared_version = getattr(module, "SCHEMA_VERSION", None)
        if declared_version is not None:
            assert (
                str(declared_version) == entry.version
            ), f"{identifier} version mismatch: registry={entry.version}, module={declared_version}"

        if entry.business_key_fields:
            assert all(
                field in schema_columns for field in entry.business_key_fields
            ), f"{identifier} business key fields missing from schema"
            metadata_business = normalize_sequence(metadata.get("business_key_fields"))
            assert (
                metadata_business == entry.business_key_fields
            ), f"{identifier} metadata business key fields mismatch"

        if entry.required_fields:
            assert all(
                field in schema_columns for field in entry.required_fields
            ), f"{identifier} required fields missing from schema"
            metadata_required = normalize_sequence(metadata.get("required_fields"))
            assert (
                metadata_required == entry.required_fields
            ), f"{identifier} metadata required fields mismatch"

        if entry.row_hash_fields:
            assert all(
                field in schema_columns for field in entry.row_hash_fields
            ), f"{identifier} row hash fields missing from schema"
            metadata_row_hash = normalize_sequence(metadata.get("row_hash_fields"))
            assert (
                metadata_row_hash == entry.row_hash_fields
            ), f"{identifier} metadata row hash fields mismatch"