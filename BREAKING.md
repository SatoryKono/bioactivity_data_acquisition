# Breaking Changes

## Removed legacy/adapter shims
- Legacy adapter entry points under `bioetl` are no longer supported by the CLI registry. Only pipelines inheriting `UnifiedPipelineBase` can be registered and executed.
- Command registry validation now rejects non-pipeline classes, preventing accidental registration of helper or adapter modules.

## Migration guidance
- Invoke pipelines through the canonical CLI commands (`bioetl run <pipeline>`). Ensure custom pipelines subclass `UnifiedPipelineBase` before wiring them into the registry.
- If you previously relied on legacy adapter wrappers, migrate to the concrete pipeline implementations in `bioetl.pipelines.chembl` and update automation scripts accordingly.
