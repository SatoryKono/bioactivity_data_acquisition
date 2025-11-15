# 02. Schema Versioning and Migrations

The ChEMBL-derived datasets evolve over time: columns are renamed, new business
keys appear, and constraints tighten. To keep every pipeline deterministic and
auditable we model schema changes explicitly via the schema migration registry.
This document describes the primitives, registration process, and authoring
guidelines.

## Registry Primitives

Module `src/bioetl/schemas/versioning.py` defines the core building blocks:

- `SchemaMigration`: an immutable record describing a single hop from
  `from_version` to `to_version` together with a `transform_fn`. The callable
  receives a `pd.DataFrame` and **must** return a new DataFrame; in-place
  mutations are forbidden because migrations have to be referentially
  transparent.
- `SchemaMigrationRegistry`: an in-memory DAG that stores migrations per schema
  identifier, resolves the shortest path between two versions, enforces
  acyclicity, and applies the transformations sequentially.
- `SCHEMA_MIGRATION_REGISTRY`: the singleton instance used throughout the
  runtime. Pipelines never touch the registry directly; instead they call the
  orchestration helpers in `PipelineBase`, which resolve the expected version,
  check contract violations, and log every applied hop.

The registry refuses to register edges that would introduce cycles or duplicate
an existing transition. Failing to obey these rules raises
`SchemaMigrationError`, which propagates as `SchemaVersionMismatchError` inside
pipeline code.

## Declaring Migrations

All built-in migrations live under `src/bioetl/schemas/migrations/`. Each module
focuses on one dataset (e.g., `chembl_activity.py`) and exposes a
`register_migrations()` function invoked by `bioetl.schemas` during import. A
minimal migration looks like:

```python
from bioetl.schemas.versioning import SchemaMigration, SCHEMA_MIGRATION_REGISTRY


def register_migrations() -> None:
    def _rename_property(df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        result["new_column"] = result.pop("old_column")
        return result

    SCHEMA_MIGRATION_REGISTRY.register(
        SchemaMigration(
            schema_identifier="bioetl.schemas.chembl_activity_schema.ActivitySchema",
            from_version="1.6.0",
            to_version="1.7.0",
            transform_fn=_rename_property,
            description="renamed old_column to new_column",
        )
    )
```

Authoring rules:

1. **Determinism first** – transform functions must sort data deterministically
   whenever rows are re-generated and must never rely on wall-clock time,
   randomness, or I/O beyond Pandas operations.
2. **Validate after migration** – migrations should not emit data that violates
   the target Pandera schema. Use unit tests in `tests/schemas/` or
   pipeline-specific golden tests to assert compliance.
3. **Document every change** – include a concise `description`, update the
   relevant pipeline documentation, and bump `SCHEMA_VERSION` in the schema
   module itself. Config owners must update `validation.schema_*_version`
   accordingly.
4. **Bounded hops** – migration DAGs must stay shallow. When a new major version
   ships, add one migration from the previous version instead of chaining
   through multiple historical revisions.

## Runtime Integration

`PipelineBase.validate` and `PipelineBase.run_schema_validation` automatically:

1. Resolve the schema descriptor via `bioetl.schemas.get_schema`.
2. Compare the runtime version with `schema_out_version` / `schema_in_version`
   from the pipeline configuration.
3. If versions match, proceed with Pandera validation as usual.
4. If versions differ and `allow_schema_migration` is `True`, resolve a
   migration path (bounded by `max_schema_migration_hops`), apply each hop with
   full logging, and validate the migrated DataFrame.
5. If migrations are disabled or a path cannot be found, raise
   `SchemaVersionMismatchError` to block the run.

Since every run records `schema_version`, `migrated_from_version`, and
`migrations_applied` inside the validation summary and exported `meta.yaml`,
operators can trace the origin of each artifact and audit upgrade rollouts.
