"""Tests for schema versioning registry primitives."""

from __future__ import annotations

import pandas as pd
import pytest

from bioetl.schemas.versioning import (
    SchemaMigration,
    SchemaMigrationRegistry,
    SchemaMigrationPathError,
)


def _increment_migration(
    schema_identifier: str, from_version: str, to_version: str, *, delta: int
) -> SchemaMigration:
    def _transform(df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        result["value"] = result["value"] + delta
        return result

    return SchemaMigration(
        schema_identifier=schema_identifier,
        from_version=from_version,
        to_version=to_version,
        transform_fn=_transform,
        description=f"increment by {delta}",
    )


def test_resolve_path_and_apply_migrations() -> None:
    registry = SchemaMigrationRegistry()
    schema_id = "tests.schemas.simple"
    registry.register(_increment_migration(schema_id, "1.0.0", "1.1.0", delta=1))
    registry.register(_increment_migration(schema_id, "1.1.0", "1.2.0", delta=10))

    path = registry.resolve_path(schema_id, "1.0.0", "1.2.0")
    assert len(path) == 2
    assert path[0].to_version == "1.1.0"
    assert path[1].to_version == "1.2.0"

    df = pd.DataFrame({"value": [1, 2, 3]})
    migrated = registry.apply_migrations(df, path)
    assert migrated["value"].tolist() == [12, 13, 14]


def test_register_prevents_cycles() -> None:
    registry = SchemaMigrationRegistry()
    schema_id = "tests.schemas.simple"
    registry.register(_increment_migration(schema_id, "1.0.0", "1.1.0", delta=1))
    with pytest.raises(ValueError):
        registry.register(_increment_migration(schema_id, "1.1.0", "1.0.0", delta=1))


def test_resolve_path_respects_max_hops() -> None:
    registry = SchemaMigrationRegistry()
    schema_id = "tests.schemas.simple"
    registry.register(_increment_migration(schema_id, "1.0.0", "1.1.0", delta=1))
    registry.register(_increment_migration(schema_id, "1.1.0", "1.2.0", delta=1))
    registry.register(_increment_migration(schema_id, "1.2.0", "1.3.0", delta=1))

    with pytest.raises(SchemaMigrationPathError):
        registry.resolve_path(schema_id, "1.0.0", "1.3.0", max_hops=2)


def test_register_rejects_duplicate_edges() -> None:
    registry = SchemaMigrationRegistry()
    schema_id = "tests.schemas.simple"
    registry.register(_increment_migration(schema_id, "1.0.0", "1.1.0", delta=1))
    with pytest.raises(ValueError):
        registry.register(_increment_migration(schema_id, "1.0.0", "1.1.0", delta=2))

