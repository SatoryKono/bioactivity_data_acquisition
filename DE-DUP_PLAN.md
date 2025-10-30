# De-duplication Plan

## Summary

The legacy `library.target` package has been fully removed. The modern
`bioetl` implementation now provides the authoritative target pipeline
components, so the older modules are no longer required.

## BioETL Supersedes Legacy Modules

- `bioetl.pipelines.target` is the maintained pipeline and replaces the
  legacy `library.target.pipeline` implementation.
- Supporting functionality (normalisation, validation, quality checks, and I/O)
  is served by the cohesive `bioetl` package and associated schemas.

## CLI Hooks

- No CLI entry points referenced `library.target` directly at the time of
  removal. Existing tooling (for example, `src/scripts/run_target.py` and the
  consolidated `bioetl.cli.main` application) already use the `bioetl` pipeline
  stack, so no additional CLI work was required.
- Consolidated the boilerplate Typer wiring for activity-like pipelines into
  the shared `scripts` registry. The helper now exposes a single
  `register_pipeline_command(app, key)` call that reads defaults from a central
  map. This keeps default paths, bespoke options (such as the document mode
  choices), and test fixtures synchronised while avoiding future copy/paste
  drift across the individual `run_*.py` modules.

## Follow-up

- Monitor downstream documentation or deployment scripts for any references to
  the removed modules and update them to point to the `bioetl` equivalents if
  discovered.

## Shared Determinism Configuration

- Introduced `configs/includes/determinism.yaml` to centralize determinism
  defaults for all pipelines extending ChEMBL sources.
- Updated activity, assay, document, target, and testitem pipeline configs to
  extend the shared include instead of duplicating the determinism defaults.

## Shared ChEMBL Client Contract

- Pipelines that communicate with the ChEMBL API must call
  `PipelineBase._init_chembl_client()` during construction (legacy call sites
  may continue using `bioetl.pipelines.base.create_chembl_client`).
- The helper applies the canonical defaults (base URL, batch sizing,
  URL-length guards) via `ensure_target_source_config` and materialises a
  `UnifiedAPIClient` using `APIClientFactory.from_pipeline_config` before
  returning the resolved context.
- Callers must persist the returned client alongside the resolved batch and
  limit metadata to honour the shared runtime contract, and tests should
  monkeypatch `_init_chembl_client` to intercept client creation in a single
  location.

## Clone Remediation Plan — test_refactoring_11

| Clone Group | Action | Risk | Check | Artifact |
| --- | --- | --- | --- | --- |
| Schema validation boilerplate across pipelines (`AssayPipeline.validate`, `TestItemPipeline.validate`, `TargetPipeline.validate`, `DocumentPipeline.validate`) | Extract a reusable `PipelineBase.run_schema_validation()` helper that accepts schema, dataset metadata, QC metric callbacks, and referential integrity hooks to drive the shared flow. | Regression in pipeline-specific side-effects (e.g., extra QC summary fields). | Re-run `pytest tests/unit/test_pipelines.py::test_pipeline_run_resets_per_run_state` and pipeline-specific validation suites. Compare QC summary JSON hashes pre/post. | Planned patch: `patches/schema_validation_helper.diff` |
| CLI sample limit wrapper duplication (`bioetl.cli.command`, `scripts/run_target`) | Introduce `bioetl.cli.limits.apply_sample_limit(pipeline, limit)` and invoke from both Typer commands. Remove inline closures. | Unexpected persistence of sample overrides between runs if helper mutates shared state. | Execute `pytest tests/unit/test_cli_contract.py::test_cli_default_behaviour` with and without `--sample`. Verify log entries for `applying_sample_limit`. | Planned patch: `patches/sample_limit_helper.diff` |
| Repeated `_summarize_schema_errors` implementations (`assay`, `testitem`) | Move shared summariser to `PipelineBase` (e.g., `_summarize_schema_errors(failure_cases)`) or to `bioetl.utils.validation`. | Differences in severity/issue payload expectations per pipeline. | Targeted pytest cases covering schema validation error paths plus golden QC snapshots. | Planned patch: `patches/schema_error_summary.diff` |
| QC metric calculators duplicated (`ActivityPipeline._calculate_qc_metrics`, `TestItemPipeline._calculate_qc_metrics`) | Build composable metric builders in `bioetl.utils.qc` (duplicate ratio, fallback rate) returning structured payloads; pipelines pass column names/threshold keys. | Metric naming divergence; thresholds might rely on pipeline-specific config keys. | Run `pytest tests/unit/test_pipelines.py::test_activity_pipeline_qc_metrics` and `::test_testitem_pipeline_validation_thresholds` (add if missing). Inspect QC summary diff. | Planned patch: `patches/qc_metric_builders.diff` |
| Adapter/client `close()` duplicates (`bioetl.adapters.base.BaseAdapter.close`, `bioetl.core.api_client.UnifiedAPIClient.close`) | Provide context manager mixin or align on shared interface ensuring adapters delegate to client `close` without duplicating docstrings. | Breaking existing subclass overrides expecting `close()` signature. | Static analysis via `mypy` to ensure inheritance remains valid; targeted unit tests closing adapters. | Planned patch: `patches/close_contract_unify.diff` |
| Package `__dir__` helpers duplicated (`bioetl.pipelines.__dir__`, `bioetl.schemas.__dir__`) | Extract `_sorted_exports(__all__)` utility to DRY up module-level `__dir__` logic. | Accidentally exposing private names if `__all__` mutated at runtime. | Run `python -m compileall src/bioetl` to ensure import-time side effects unchanged; add unit assertion for `dir(bioetl.schemas)`. | Planned patch: `patches/dir_helper.diff` |
| Chemistry normaliser `validate` stubs duplicated (`ChemistryStringNormalizer`, `ChemistryUnitsNormalizer`, etc.) | Introduce mixin `StringNormaliserMixin.validate()` returning `_is_na` or `isinstance(str)` to eliminate repeated guards. | Validators must remain lightweight; introducing inheritance should not slow down hot loops. | Benchmark normaliser micro tests; run `pytest tests/unit/test_identifier_normalizer.py`. | Planned patch: `patches/chemistry_normalizer_mixin.diff` |
| Test double classes duplicated across suites (`tests/unit/test_pipelines.py`, `tests/unit/test_cli_contract.py`, `tests/unit/test_pipeline_extract_helper.py`) | Create shared fixtures/helpers (e.g., `make_recording_pipeline`) in `tests/conftest.py` and reuse via parametrisation. | Fixture scope mistakes may leak state between tests. | `pytest -k "pipeline" --maxfail=1` ensuring isolation plus coverage of CLI contract tests. | Planned patch: `patches/test_pipeline_fixtures.diff` |
