# De-duplication Plan

## Summary
The legacy `library.target` package has been fully removed. The modern `bioetl` implementation now provides the authoritative target pipeline components, so the older modules are no longer required.

## BioETL Supersedes Legacy Modules
- `bioetl.pipelines.target` is the maintained pipeline and replaces the legacy `library.target.pipeline` implementation.
- Supporting functionality (normalisation, validation, quality checks, and I/O) is served by the cohesive `bioetl` package and associated schemas.

## CLI Hooks
- No CLI entry points referenced `library.target` directly at the time of removal. Existing tooling (for example, `src/scripts/run_target.py` and the consolidated `bioetl.cli.main` application) already use the `bioetl` pipeline stack, so no additional CLI work was required.
- Consolidated the boilerplate Typer wiring for activity-like pipelines into the shared `scripts` registry.  The helper now exposes a
  single `register_pipeline_command(app, key)` call that reads defaults from a central map.  This keeps default paths, bespoke
  options (such as the document mode choices), and test fixtures synchronised while avoiding future copy/paste drift across the
  individual `run_*.py` modules.

## Follow-up
- Monitor downstream documentation or deployment scripts for any references to the removed modules and update them to point to the `bioetl` equivalents if discovered.

## Shared Determinism Configuration
- Introduced `configs/includes/determinism_defaults.yaml` to centralize determinism defaults for all pipelines extending ChEMBL sources.
- Updated activity, assay, document, and testitem pipeline configs to extend the shared include instead of duplicating the determinism defaults.
## Shared ChEMBL Client Contract
- Pipelines that communicate with the ChEMBL API must call `PipelineBase._init_chembl_client()` during construction (legacy call sites may continue using `bioetl.pipelines.base.create_chembl_client`).
- The helper applies the canonical defaults (base URL, batch sizing, URL-length guards) via `ensure_target_source_config` and materialises a `UnifiedAPIClient` using `APIClientFactory.from_pipeline_config` before returning the resolved context.
- Callers must persist the returned client alongside the resolved batch and limit metadata to honour the shared runtime contract, and tests should monkeypatch `_init_chembl_client` to intercept client creation in a single location.
