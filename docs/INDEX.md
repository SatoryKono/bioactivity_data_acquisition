# Documentation Index

Central navigation hub for `bioetl` documentation, synchronized with the live
repository layout.

## Core Concepts

- **[Repository Topology](00-repository-topology.md)** — directory layers,
  sources of truth, and artifact placement. Code examples live under `src/`,
  mirrored by `tests/` (see the dedicated topology document).
- **[ETL Contract](etl_contract/00-etl-overview.md)** — pipeline lifecycle and
  `PipelineBase` contract. Implementations are under
  `src/bioetl/pipelines/`, with validation tests in `tests/bioetl/pipelines/`.
- **[Source Architecture](sources/00-sources-architecture.md)** — client/parser
  stack for external systems. Source adapters are in `src/bioetl/sources/`;
  interface coverage is documented in `sources/01-interface-matrix.md`.
- **[Determinism Policy](determinism/00-determinism-policy.md)** — requirements
  for byte-identical outputs (stable sort, UTC timestamps, atomic writes).
  Regression checks: `tests/bioetl/tools/test_determinism_check.py`.
- **[Vault Policy](security/00-vault-policy.md)** — HashiCorp Vault workflow for
  secrets. CLI profiles load secret placeholders from `configs/` and follow the
  patterns in `styleguide/09-secrets-config.md`.

## Repository Structure

Canonical directory overview (full detail in `docs/00-repository-topology.md`):

- `src/` — application and pipeline modules (extract/transform/validate/export
  components).
- `tests/` — unit, integration, and golden tests mirroring `src/` packages.
- `configs/` — typed configuration models and profiles.
- `docs/` — authoritative documentation (this index is the entry point).
- `data/` — deterministic fixtures and sample outputs for development.
- `scripts/` — maintenance and diagnostic CLI utilities linked from `docs/cli/`.
- `.cache/` — local-only intermediate artifacts; excluded from commits and CI.

## Technical Specifications

- **[Typed Configurations](configs/00-typed-configs-and-profiles.md)** — Pydantic
  configuration layers, profile inheritance, and validation across `configs/`
  and `tests/bioetl/configs/`.
- **[CLI Reference](cli/00-cli-overview.md)** — Typer-based CLI contract,
  command registry, and exit codes backed by `src/bioetl/cli/`.
- **[HTTP Clients](http/00-http-clients-and-retries.md)** — UnifiedAPIClient
  guidelines (timeouts, retry/backoff, caching).
- **[Structured Logging](logging/00-overview.md)** — UnifiedLogger usage, event
  shape, and context propagation.
- **[Quality Assurance](qc/00-qc-overview.md)** — QC metrics, golden tests, and
  audit requirements for pipeline outputs.
- **[Architecture Decision Records](adr/)** — accepted design decisions. Author
  new ADRs from `adr/00-template.md` and register them here.

## Pipeline Documentation

`[pipelines/10-pipelines-catalog.md](pipelines/10-pipelines-catalog.md)` is the
single source of pipeline statuses, interfaces, and naming. Pipeline documents
follow the `<NN>-<entity>-<source>-<topic>.md` convention (see
`styleguide/00-naming-conventions.md#11-pipeline-documentation-file-naming`).

- **ChEMBL pipelines** — activity, assay, document, target, and test item flows;
  overview files live under `pipelines/*-chembl/00-*.md`.
- **Mapping and non-ChEMBL pipelines** — UniProt, IUPHAR, PubChem, and mapping
  pipelines, catalogued in the same directory structure.
- **Document pipelines (PubMed, OpenAlex, Crossref, Semantic Scholar)** — marked
  as archived and not implemented; historical specifications only. No CLI
  commands are registered (details in the catalog).

## Style Guides

Normative references for implementation and review:

- **[Naming Conventions](styleguide/00-naming-conventions.md)**
- **[Python Code Style](styleguide/01-python-code-style.md)**
- **[Logging Guidelines](styleguide/02-logging-guidelines.md)**
- **[Data Schemas and Validation](styleguide/03-data-schemas.md)**
- **[Deterministic I/O](styleguide/04-deterministic-io.md)**
- **[Testing Standards](styleguide/05-testing-standards.md)**
- **[CLI Contracts](styleguide/06-cli-contracts.md)**
- **[API Clients](styleguide/07-api-clients.md)**
- **[ETL Pipeline Architecture](styleguide/08-etl-architecture.md)**
- **[Secrets and Configuration](styleguide/09-secrets-config.md)**
- **[Documentation Standards](styleguide/10-documentation-standards.md)**

## Additional Resources

- **[Prompt Archives](00-prompt/00-01-etl-contract.md)** — historical design
  prompts that informed the initial documentation set. They are reference-only
  and may not reflect current implementations.
