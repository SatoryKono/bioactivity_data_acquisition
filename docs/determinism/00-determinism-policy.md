# Determinism Policy Overview

> This document provides the canonical entry point for determinism requirements. It summarizes the run-time contract enforced by `PipelineBase` and points to follow-up specifications in [`01-determinism-policy.md`](./01-determinism-policy.md) for the exhaustive rationale, background material, and implementation notes.

## Document Hierarchy and Scope

- **This file (`00-…`)** — authoritative checklist for pipeline engineers. It captures the non-negotiable mechanics that every deterministic run must apply, mirroring the behaviour baked into `PipelineBase` and the determinism regression tests tracked on branch `test_refactoring_32`. The items below are the minimum a pipeline author must satisfy before consulting more detailed guidance.
- **`01-determinism-policy.md`** — the deep-dive companion covering design discussions, motivating examples, and broader governance topics such as golden testing and schema migrations. Use it when you need the “why” or additional background context for an invariant already summarized here.【F:docs/determinism/01-determinism-policy.md†L5-L168】【F:docs/determinism/01-determinism-policy.md†L151-L224】

## Fixed Sort Keys per Entity

Stable row ordering is enforced centrally by `PipelineBase` and verified by the determinism golden tests. The table below is the authoritative mapping of entity → sort key list; the extended rationale for each pipeline lives in the companion document to avoid duplication.【F:docs/determinism/01-determinism-policy.md†L24-L34】

| Pipeline | Primary sort keys (in evaluation order) |
| --- | --- |
| `activity` | `assay_id`, `testitem_id`, `activity_id` |
| `assay` | `assay_id` |
| `target` | `target_id` |
| `document` | `year`, `document_id` |
| `testitem` | `testitem_id` |

**Operational notes**

1. Sorting is always executed with a **stable algorithm**, `na_position="last"`, and `ascending=True`, guaranteeing consistent placement of duplicate keys and null handling.【F:docs/determinism/01-determinism-policy.md†L183-L186】
2. Sort key declarations live under `determinism.sort.by` in every pipeline configuration; missing keys cause the pipeline tests to fail before any write occurs.【F:docs/determinism/01-determinism-policy.md†L24-L34】【F:docs/determinism/01-determinism-policy.md†L151-L161】

## Hash Computation Algorithms

`PipelineBase` materializes two integrity hashes per record. Both rely on SHA-256 digests over UTF-8 encoded payloads, and both run against canonicalized values (trimmed strings, normalized casing, UTC timestamps, JSON with sorted keys for complex types) before hashing.【F:docs/determinism/01-determinism-policy.md†L36-L70】

### `hash_row`

1. Select the fields declared in `determinism.hashing.row_fields`.
2. Drop keys with null-equivalent values after canonicalization.
3. Serialize the remaining key/value pairs to compact JSON with `sort_keys=True` and no whitespace padding.
4. Compute the SHA-256 hex digest of the UTF-8 encoded JSON string; prefixing with `"sha256:"` is optional but recommended for clarity.
5. Exclude inherently unstable fields such as `generated_at` or `run_id` to avoid spurious churn.【F:docs/determinism/01-determinism-policy.md†L53-L63】

### `hash_business_key`

1. Concatenate canonicalized values of the configured business-key columns in a deterministic order.
2. Encode the concatenated string as UTF-8 and compute the SHA-256 hex digest.
3. Do **not** apply any salt. Collisions are monitored through golden tests and schema versioning; any schema change touching the business key must increment the schema version and refresh goldens.【F:docs/determinism/01-determinism-policy.md†L64-L71】【F:docs/determinism/01-determinism-policy.md†L151-L161】

## `meta.yaml` Reference Snapshot

Every successful run writes a sidecar `meta.yaml` file next to the dataset artifact. Keys are serialized in alphabetical order to keep diffs deterministic, and the payload captures both configuration lineage and result metrics.【F:docs/determinism/01-determinism-policy.md†L73-L119】

```yaml
# Example emitted by PipelineBase (values representative, not literal)
artifacts:
  dataset: data/output/activity/activity.parquet
  quality_report: data/output/activity/qc/activity_quality_report.csv
column_order:
  - activity_id
  - assay_id
  - testitem_id
  - value
  - units
column_order_source: schema
config_fingerprint: "sha256:abcde12345..."
config_snapshot:
  path: configs/pipelines/chembl/activity.yaml
  sha256: "sha256:fghij67890..."
deduplicated_count: 0
generated_at_utc: "2025-11-03T01:15:00.123456Z"
hash_algo: sha256
hash_business_key: "sha256:9f86d081884c7d65..."
hash_policy_version: "1.0"
inputs:
  - path: data/input/activity.csv
    sha256: "sha256:..."
outputs:
  - path: data/output/activity/activity.parquet
    sha256: "sha256:..."
  - path: data/output/activity/qc/activity_quality_report.csv
    sha256: "sha256:..."
pipeline:
  name: activity
  version: "1.0.0"
qc:
  duplicates: 0
  missing_values: 125
  referential_integrity_violations: 0
row_count: 10000
run_id: "20251103-011500-abcdef"
sample_hash_row: "sha256:ab0c12de34..."
schema_version: "1.2.0"
source_lineage:
  chembl_release: "33"
```

**Emission contract**

- `meta.yaml` is written atomically using the same helper as the dataset (see below).
- Any addition/removal of keys requires updating the schema version and regenerating golden fixtures; the determinism tests will flag unexpected drift automatically.【F:docs/determinism/01-determinism-policy.md†L151-L168】

## Atomic Write Procedure (tmp → rename)

Deterministic runs must never leave partially written artifacts. `PipelineBase` enforces an atomic writer that stages files via a temporary path and swaps them into place once the write completes.【F:docs/determinism/01-determinism-policy.md†L169-L224】

1. **Generate target paths** — derive the final dataset filename and locate the sibling `meta.yaml` path.
2. **Write to a temporary file** — stream the dataset (CSV/Parquet) into a temp file within the destination directory. Flush buffers and `fsync` to guarantee durability before renaming.
3. **Rename atomically** — promote the temp file into place with `os.replace`, ensuring the operation is atomic on POSIX-compliant filesystems. The same helper writes `meta.yaml` through `write_json_as_yaml(..., sort_keys=True)` to guarantee stable ordering.
4. **Post-write validation** — optionally re-load the dataset to confirm the row count, hash columns, and schema; this mirrors the checks executed in the determinism regression suite and guards against silent truncation.
5. **Structured logging** — emit success/failure telemetry so CI golden tests can correlate writes with deterministic expectations.

By following this sequence, every pipeline inherits the same durability guarantees, and determinism regressions surface immediately through the golden tests that guard the `test_refactoring_32` branch.【F:docs/determinism/01-determinism-policy.md†L151-L224】
