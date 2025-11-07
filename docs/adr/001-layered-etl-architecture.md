# ADR 001: Layered ETL Architecture Boundaries

- **Date:** 2024-05-05
- **Status:** Accepted
- **Deciders:** @data-platform, @ml-platform
- **Tags:** architecture, layering, etl

## Context

The `bioetl` runtime orchestrates heterogeneous extraction pipelines spanning multiple external sources (ChEMBL, PubMed, etc.). Components currently live under `src/bioetl/` and blend orchestration, IO, and domain logic. Without explicit boundaries, testing and reuse become cumbersome, and teams risk duplicating logic across pipelines. We also need to align with existing documentation in `docs/pipelines/` and `docs/sources/` that already assumes a layered separation between clients, normalizers, and orchestration.

## Decision

We formalize a three-layer architecture:

1. **Orchestration layer (`bioetl.pipelines`):** Typer/CLI entrypoints and pipeline coordinators drive scheduling, retries, and inter-component wiring.
2. **Domain layer (`bioetl.domain`, `bioetl.schemas`):** Pure data transformations, validation schemas (Pandera, Pydantic), and deterministic business rules live here. These modules must not talk to IO or configuration stores directly.
3. **Infrastructure layer (`bioetl.clients`, `bioetl.adapters`, `bioetl.storage`):** Handles external HTTP clients, storage adapters, and serialization. These modules expose typed interfaces consumed by the orchestration layer.

Cross-layer dependencies are strictly top-down: orchestration → domain → infrastructure (via interfaces). Shared utilities that are layer-agnostic (e.g., common enums) stay in `bioetl.shared`. Each pipeline must define its components following this stratification and keep documentation synced with the new structure.

Alternative considered: keep ad-hoc organization and rely on conventions enforced through documentation only. Rejected because it makes architecture drift unmanageable and complicates future migrations to orchestrators.

## Consequences

- We must refactor new code to respect the layer boundaries and add import lints in CI to prevent upward dependencies.
- Testing becomes easier: infrastructure components can be mocked while validating domain logic deterministically.
- Documentation in `docs/pipelines/*` must reference the layer-specific modules, and onboarding materials should highlight the separation.
- Follow-up: add automated checks (e.g., `pytest --layer` marker) to detect boundary violations.

## References

- `docs/sources/00-sources-architecture.md`
- `docs/pipelines/00-pipeline-base.md`
- `src/bioetl/` module layout
