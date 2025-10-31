"""Smoke tests for the UnifiedSchema facade."""

from __future__ import annotations

from bioetl.core import unified_schema
from bioetl.schemas import BaseSchema


class _FacadeSmokeSchema(BaseSchema):
    """Minimal schema used to verify facade registration helpers."""

    class Config(BaseSchema.Config):
        name = "unit.facade-smoke"


def test_facade_exposes_singleton_registry() -> None:
    """``get_registry`` should return the shared singleton instance."""

    first = unified_schema.get_registry()
    second = unified_schema.get_registry()
    assert first is second


def test_facade_registers_and_resolves_schema() -> None:
    """Registering via the facade should expose schema and metadata lookups."""

    entity = "unit_facade_entity"
    version = "0.0.1"

    unified_schema.register_schema(entity, version, _FacadeSmokeSchema)

    assert unified_schema.is_registered(entity)
    assert unified_schema.is_registered(entity, version)

    resolved = unified_schema.get_schema(entity, version)
    assert resolved is _FacadeSmokeSchema

    metadata = unified_schema.get_schema_metadata(entity, version)
    assert metadata is not None
    assert metadata.entity == entity
    assert metadata.version == version
    assert metadata.schema_id == f"{entity}.output"


def test_facade_handles_missing_schema() -> None:
    """Gracefully handle lookups for entities that were never registered."""

    missing_entity = "unit_facade_missing"

    assert unified_schema.is_registered(missing_entity) is False
    assert unified_schema.get_schema_metadata(missing_entity) is None
