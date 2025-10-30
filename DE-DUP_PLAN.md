# De-duplication Plan

## Summary
The legacy `library.target` package has been fully removed. The modern `bioetl` implementation now provides the authoritative target pipeline components, so the older modules are no longer required.

## BioETL Supersedes Legacy Modules
- `bioetl.pipelines.target` is the maintained pipeline and replaces the legacy `library.target.pipeline` implementation.
- Supporting functionality (normalisation, validation, quality checks, and I/O) is served by the cohesive `bioetl` package and associated schemas.

## CLI Hooks
- No CLI entry points referenced `library.target` directly at the time of removal. Existing tooling (for example, `src/scripts/run_target.py` and the consolidated `bioetl.cli.main` application) already use the `bioetl` pipeline stack, so no additional CLI work was required.

## Follow-up
- Monitor downstream documentation or deployment scripts for any references to the removed modules and update them to point to the `bioetl` equivalents if discovered.

## Shared Determinism Configuration
- Introduced `configs/includes/determinism_defaults.yaml` to centralize determinism defaults for all pipelines extending ChEMBL sources.
- Updated activity, assay, document, and testitem pipeline configs to extend the shared include instead of duplicating the determinism defaults.
## Shared ChEMBL Client Contract
- Pipelines that communicate with the ChEMBL API must call `bioetl.pipelines.base.create_chembl_client` during construction.
- The helper applies the canonical defaults (base URL, batch sizing, URL-length guards) via `ensure_target_source_config` and materialises a `UnifiedAPIClient` using `APIClientFactory.from_pipeline_config`.
- Callers must persist the returned client alongside the resolved batch and limit metadata to honour the shared runtime contract and keep tests focused on the helper when monkeypatching.
