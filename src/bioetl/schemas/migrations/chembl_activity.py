"""ChEMBL Activity schema migrations."""

from __future__ import annotations

from bioetl.schemas.versioning import SCHEMA_MIGRATION_REGISTRY, SchemaMigration

__all__ = ["register_migrations"]


def register_migrations() -> None:
    """Register built-in ChEMBL Activity schema migrations."""

    # No migrations yet. Future migrations must register using:
    # SCHEMA_MIGRATION_REGISTRY.register(SchemaMigration(...))
    _ = (SCHEMA_MIGRATION_REGISTRY, SchemaMigration)


