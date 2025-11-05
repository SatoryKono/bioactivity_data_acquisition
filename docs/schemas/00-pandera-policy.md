# Pandera Schema Governance Policy

> **Note**: Implementation status: **planned**. All file paths referencing `src/bioetl/` or `tests/` in this document describe the intended
architecture and are not yet implemented in the codebase.

## 1. Scope

This policy defines how Pandera schemas are authored, versioned, validated, and regression-tested across all BioETL pipelines.
It complements the determinism requirements and ensures that every dataset emitted by `PipelineBase` adheres to a frozen, well
understood contract.

- **Runtime enforcement**: The `validate` stage in [`PipelineBase`][ref: repo:src/bioetl/pipelines/base.py@refactoring_001] loads
  the schema declared in the pipeline configuration and executes `schema.validate(df, lazy=True)`. Any violation raises a
  `SchemaError`, halting the run before write operations.
- **Continuous testing**: Schema-specific tests in the `test_refactoring_32` branch (for example,
  `[ref: repo:tests/schemas/test_activity_schema.py@test_refactoring_32]`) run the same validation logic with golden datasets to
  guarantee parity between developer machines and CI.

## 2. Schema Versioning & Freeze Order

Every Pandera schema **must** define a semantic version (`MAJOR.MINOR.PATCH`) stored alongside the schema and recorded in
`meta.yaml` for each pipeline execution.

- **Freeze order requirements**: Schemas are created with `strict=True`, `ordered=True`, and `coerce=True`. Column order becomes a
  contractual artifact; any reordering is a breaking change unless explicitly handled through a migration plan.
- **Version bump rules**:
  - **PATCH**: Documentation-only updates or adjustments that do not affect validation logic.
  - **MINOR**: Backward-compatible extensions such as adding nullable columns with defaults, loosening value ranges, or adding
    optional checks flagged as warnings.
  - **MAJOR**: Any change that can invalidate downstream consumers—renaming or removing columns, tightening constraints,
      changing data types, or altering ordered column lists.
- **Freeze approval**: Before promoting a schema version, the owning team signs off that the column order, business keys, and
  type coercion rules are frozen for the lifetime of that release cycle.

## 3. Allowed vs. Breaking Changes

| Change Type | Classification | Rationale & Required Action |
| --- | --- | --- |
| Add nullable column with explicit default | Allowed (MINOR) | Update schema, bump minor, regenerate golden outputs. |
| Loosen check bounds (e.g., widen numeric range) | Allowed (MINOR) | Maintain compatibility while capturing more data. |
| Strengthen check (e.g., new uniqueness constraint) | Breaking (MAJOR) | Can reject previously valid data; requires migration plan. |
| Rename or remove column | Breaking (MAJOR) | Downstream tooling relies on column names. |
| Reorder columns | Breaking (MAJOR) | Violates freeze-order guarantee and determinism. |
| Change dtype coercion (int → str) | Breaking (MAJOR) | Alters serialization and hashing. |
| Update documentation/comments only | Allowed (PATCH) | No runtime effect; no golden regeneration required. |

## 4. Golden Snapshot Maintenance

Golden artifacts provide regression coverage for schema behavior.

1. **Storage**: Golden CSV/Parquet outputs and `meta.yaml` snapshots live under `tests/golden/<pipeline>/` (mirrored on the
    `test_refactoring_32` branch until merged).
2. **Regeneration triggers**:
   - Schema version bump (of any level) or deterministic policy change.
   - Updates to canonical sorting or hashing rules.
3. **Process**:
   - Run the pipeline locally with `--golden` fixtures to produce fresh outputs.
   - Execute schema tests (see `[ref: repo:tests/schemas/test_pipeline_contract.py@test_refactoring_32]`).
   - Verify hashes and column order against the regenerated artifacts.
   - Commit updated golden files alongside the schema version bump notes.
4. **Review checklist**:
   - `meta.yaml` reflects the new `schema_version`.
   - Golden outputs are bitwise identical across repeated runs (deterministic mode).
   - CI pipeline `schema` job passes with the refreshed artifacts.

## 5. Runtime Enforcement & Monitoring

`PipelineBase.validate()` enforces the following invariants before the `write` stage:

1. Loads the schema module defined in `validate.schema_out`.
2. Executes Pandera validation with lazy error collection.
3. Applies `ensure_column_order()` to align DataFrame order with the schema definition.
4. Records `schema_version` in `meta.yaml` for traceability.

Monitoring hooks in `PipelineBase` emit structured logs summarizing validation stats and attach failure payloads to S3 for
post-mortem review (planned enhancements tracked in `[ref: repo:docs/qc/03-checklists-and-ci.md@refactoring_001]`).

## 6. Schema Drift Handling & CLI Controls

Schema drift is any deviation between the runtime dataset and the frozen schema.

- **Fail-closed (default)**: Pipelines abort on first validation failure. This mode is activated by default and enforced when the
  CLI flag `--fail-on-schema-drift` is present (default) or `--allow-schema-drift` is omitted.
- **Fail-open (opt-in)**: Operators may bypass hard failures for exploratory runs by passing `--allow-schema-drift`. In this mode
  validation errors are logged as warnings, but `meta.yaml` records `schema_valid: false`.
- **Column validation toggle**: `--no-validate-columns` bypasses the post-validation column-order enforcement, while
  `--validate-columns` (default) ensures deterministic ordering.

Operational guidance:

1. Production pipelines **must** run in fail-closed mode. Scheduled jobs enforce this via deployment manifests.
2. Fail-open mode is permitted only for debugging and must never write to shared production buckets.
3. Any drift event triggers a schema review, golden artifact refresh, and potential version bump following Sections 2–4 above.

## 7. Change Management Workflow

1. Propose schema change via RFC, referencing impacted pipelines and downstream consumers.
2. Implement schema update, bump version, regenerate goldens, and update documentation (including this policy if rules change).
3. Open PR with:
   - Updated schema module and version constant.
   - Regenerated golden snapshots.
   - Test evidence from `pytest -m schema` (tracked in `test_refactoring_32`).
4. Release management tags the pipeline with the new schema version and coordinates deployment windows.

By following this policy, BioETL ensures that schema evolution is intentional, auditable, and compatible with deterministic
pipelines and reproducible analytics.

