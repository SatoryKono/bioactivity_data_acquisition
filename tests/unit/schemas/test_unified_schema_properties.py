"""Property-based tests ensuring UnifiedSchema registrations stay consistent."""

from __future__ import annotations

import hashlib

import pytest

hypothesis = pytest.importorskip("hypothesis")
strategies = pytest.importorskip("hypothesis.strategies")
from hypothesis import given

from bioetl.core.unified_schema import (
    get_registry,
    get_schema,
    get_schema_metadata,
    is_registered,
)


_ENTITIES = ("document", "assay", "activity", "target", "testitem")


def _column_order_hash(columns: list[str]) -> str:
    digest = hashlib.sha256()
    digest.update("\n".join(columns).encode("utf-8"))
    return digest.hexdigest()


@pytest.mark.parametrize("entity", _ENTITIES)
def test_default_registrations_are_available(entity: str) -> None:
    assert is_registered(entity)
    metadata = get_schema_metadata(entity)
    assert metadata is not None
    schema = get_schema(entity, metadata.version)
    assert schema is metadata.schema


@given(strategies.sampled_from(_ENTITIES))
def test_column_order_matches_schema(entity: str) -> None:
    schema = get_schema(entity)
    metadata = get_schema_metadata(entity)
    assert metadata is not None

    try:
        column_order = list(schema.get_column_order())
    except Exception:  # pragma: no cover - defensive guard
        column_order = []

    registry_columns = list(metadata.schema.get_column_order())
    assert column_order == registry_columns


@pytest.mark.parametrize("entity", _ENTITIES)
def test_column_order_hash_stability(entity: str) -> None:
    metadata = get_schema_metadata(entity)
    assert metadata is not None
    columns = list(metadata.schema.get_column_order())
    current_hash = _column_order_hash(columns)

    registry = get_registry()
    resolved = registry.get(entity, metadata.version)
    expected_columns = list(resolved.get_column_order())
    assert current_hash == _column_order_hash(expected_columns)
