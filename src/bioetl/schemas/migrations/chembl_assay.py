"""ChEMBL Assay schema migrations."""

from __future__ import annotations

from bioetl.schemas.versioning import SCHEMA_MIGRATION_REGISTRY, SchemaMigration

__all__ = ["register_migrations"]


def register_migrations() -> None:
    """Register built-in ChEMBL Assay schema migrations."""

    _ = (SCHEMA_MIGRATION_REGISTRY, SchemaMigration)


