"""Schema registry consistency tests."""

from __future__ import annotations

from importlib import import_module

from bioetl.schemas import SCHEMA_REGISTRY


def test_registry_column_order_and_versions() -> None:
    """All registered schemas expose consistent metadata."""

    for identifier, entry in SCHEMA_REGISTRY.as_mapping().items():
        schema_columns = list(entry.schema.columns.keys())
        column_order = list(entry.column_order)

        assert len(column_order) == len(set(column_order)), (
            f"{identifier} column_order contains duplicates: {column_order}"
        )

        missing = [name for name in column_order if name not in schema_columns]
        assert not missing, (
            f"{identifier} column_order references missing columns: {missing}"
        )

        for hashed_column in ("hash_row", "hash_business_key"):
            assert hashed_column in schema_columns, (
                f"{identifier} missing required column '{hashed_column}'"
            )
            assert hashed_column in column_order, (
                f"{identifier} column_order missing '{hashed_column}'"
            )

        module_path, _ = identifier.rsplit(".", 1)
        module = import_module(module_path)
        declared_version = getattr(module, "SCHEMA_VERSION", None)
        if declared_version is not None:
            assert str(declared_version) == entry.version, (
                f"{identifier} version mismatch: registry={entry.version}, module={declared_version}"
            )

