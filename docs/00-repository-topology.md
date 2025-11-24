# Repository Topology

This document describes the layered layout of the `bioetl` repository and the
rules for storing deterministic artifacts.

## Repository Layers

### `src/`

Primary application and pipeline code. Modules are organized by component type
(clients, normalizers, pipelines, writers) and implement the contracts defined
in `docs/etl_contract/` and `docs/pipelines/`. The Typer CLI lives in
`src/bioetl/cli/` and depends on shared abstractions such as
`bioetl.clients.client_exceptions` instead of direct HTTP libraries. CLI
configuration models are imported from
`bioetl.config.models.models` (or `.policies` for policy objects),
ensuring that
runtime options derive from the typed configuration registry.

### `tests/`

Unit, integration, and golden tests reside under `tests/bioetl/`. Directory
structure mirrors `src/` to simplify navigation and example discovery. Shared
fixtures live in `tests/bioetl/conftest.py`, while reusable helpers are grouped
inside `tests/bioetl/unit/utils/`.

### `configs/`

Typed configuration models and profiles documented in
[`docs/configs/00-typed-configs-and-profiles.md`](configs/00-typed-configs-and-profiles.md).
This directory houses Pydantic models, profile YAML, and generated artifacts for
documentation. Configuration files are the single source of truth for pipeline
parameters and stay in sync with documentation through CI validation.

### `docs/`

Authoritative documentation, including this topology, the ETL contract, style
guides, and pipeline catalogs. Indexing is maintained by `docs/INDEX.md`.

### `data/`

Deterministic datasets required for development: curated fixtures, test
snapshots, and golden examples that are safe to version-control. Large
production extracts and private data must not be committed. Favor small,
deterministic samples for reproducible walkthroughs.

### `scripts/`

Maintenance tooling: CLI helpers, migration scripts, and diagnostics. Scripts
must be idempotent, documented under `docs/cli/`, and parameterized via profiles
from `configs/`.

### `.cache/`

Local-only cache for intermediate files (e.g., HTTP responses, compiled lookup
tables). The directory is ignored by Git and CI; contents are ephemeral and can
be safely purged at any time.

## Sources of Truth

- **Configurations** — defined in `configs/` and documented in `docs/configs/`.
  All changes are code-reviewed and verified by CI pipelines.
- **Secrets** — stored in HashiCorp Vault and surfaced via environment variables.
  Local templates such as `.env.key.template` remain empty placeholders. See
  [`styleguide/09-secrets-config.md`](styleguide/09-secrets-config.md) for the
  mandatory practices. Where `README.md` is available it provides the high-level
  entry point; if `README.md` is absent or incomplete, the documentation in
  `docs/` is authoritative.
- **Documentation** — the `docs/` directory contains the canonical specifications
  for architecture, contracts, and processes. Any structural code change must be
  reflected here and passes the documentation validation pipeline.

## Artifact Placement

1. **Long-lived artifacts** (schemas, stable fixtures) belong in `data/` or
   `tests/bioetl/` and must be deterministic.
2. **Temporary or bulky artifacts** are written to `.cache/` or to external
   storage (S3, GCS). They are excluded from commits and cleaned before release.
3. **Generated reports and documentation** are published via CI to GitHub Pages.
   Local previews may use `site/`, which stays untracked.
4. **Secrets and access keys** must never enter the repository. Use environment
   variables and the Vault integration outlined in
   [`styleguide/09-secrets-config.md`](styleguide/09-secrets-config.md).
