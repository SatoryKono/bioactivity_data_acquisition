# ADR 003: Canonical Data Contract and Storage Layout

- **Date:** 2024-05-05
- **Status:** Accepted
- **Deciders:** @data-platform, @ml-platform
- **Tags:** data, storage, contracts

## Context

The pipelines emit normalized datasets consumed by downstream analytics and ML jobs. Historically, outputs have been stored inconsistently (mix of CSV, Parquet, JSONL) and directories varied per pipeline. `docs/output/00-output-layout.md` and `docs/qc/` define expectations for deterministic output and QC traces, but without an ADR teams hesitate to enforce the contract. We must standardize the storage format, partitioning, and metadata required to keep lineage traceable.

## Decision

- All canonical pipeline outputs are written as partitioned Parquet datasets under `data/output/<pipeline>/YYYY/MM/DD/`.
- Alongside Parquet files, we emit a `manifest.json` capturing schema version, source snapshot identifiers, and QC digest. This manifest is required for downstream ingestion.
- Raw extractions stay in `data/raw/<source>/` with immutable naming derived from source identifiers. Transformations may only read from raw snapshots or canonical outputs.
- Metadata schemas are defined in `bioetl.schemas.manifest` and validated before publishing.

Alternative considered: leave format choice to each pipeline. Rejected because consumers need uniform ingestion paths and we rely on deterministic QC diffs that depend on column ordering and serialization.

## Consequences

- Storage usage becomes predictable and easier to garbage-collect by snapshot date.
- Pipelines must update writing utilities to go through `bioetl.storage.parquet_writer` which enforces canonical metadata.
- Downstream teams can depend on the manifest for lineage; failure to produce it blocks releases.
- Follow-up: add CLI tooling (`bioetl pipelines validate-output`) to ensure manifests and Parquet schemas align before publishing.

## References

- `docs/output/00-output-layout.md`
- `docs/qc/03-checklists-and-ci.md`
- `src/bioetl/storage/parquet_writer.py`
