---
description: Detailed plan for refactoring legacy ChemblActivityPipeline
---

# Refactoring Plan: ChemblActivityPipeline

## Context

`ChemblActivityPipeline` (in `src/bioetl/pipelines/chembl/activity/run.py`) is currently marked as **LEGACY CODE**. It overrides core orchestration logic (`run()` method) instead of using the modern `LifecycleCoordinator` and `ChemblCommonPipeline` mechanisms. This results in numerous static analysis errors (LSP violations, signature mismatches) and technical debt.

## Goal

Bring `ChemblActivityPipeline` up to modern standards by:
1.  Eliminating static analysis errors (mypy/pylint).
2.  Aligning method signatures with base classes (`ChemblCommonPipeline`, `PipelineRuntimeBase`).
3.  Removing custom orchestration logic in favor of the shared `LifecycleCoordinator`.
4.  Standardizing service usage (`WriteService`, `ArtifactPlanner`).

## Phases

### Phase 1: Immediate Cleanup (Safe Type Fixes & Lints)

**Objective**: Fix "noisy" static analysis errors that do not require architectural changes.

1.  **Fix `ActivityWriteService`**:
    *   Implement missing abstract method `write_metadata` (add dummy implementation matching `ChemblWriteService`).
    *   This resolves: *"Cannot instantiate abstract class..."*
2.  **Fix Type Annotations**:
    *   Update `ActivityWriter` config annotation to `PipelineConfig`.
    *   Update `UnifiedOutputWriter` usage to match expected types (`run_stem`, `config`).
    *   Fix `descriptor_from_options` argument type for `pagination` (`dict` vs `ChemblPaginationConfig`).
3.  **Cosmetic Fixes**:
    *   Wrap long lines (> 79 chars).
    *   Remove unused `type: ignore` comments where applicable.

### Phase 2: Signature Alignment

**Objective**: Ensure method overrides match the base class contracts to satisfy Liskov Substitution Principle (LSP).

1.  **Align `run()` signature**:
    *   Match `PipelineRuntimeBase.run`: `(self, output_dir: Path, *, run_tag: str | None = None, ...)`
    *   Ensure `dry_run` is typed consistently.
2.  **Align `extract()` signature**:
    *   Update annotation to `ChemblExtractionServiceDescriptor[Any] | ChemblExtractionDescriptor | None`.
3.  **Fix `resolve_chembl_release`**:
    *   Current implementation renames `config` arg. Rename back to `chembl_client` or match base signature `(self, chembl_client: Any) -> str`.

### Phase 3: Architecture Convergence (Deep Refactoring)

**Objective**: Remove legacy overrides and use `ChemblCommonPipeline` logic.

1.  **Migrate `ActivityWriteService`**:
    *   Evaluate if `ActivityWriter` logic can be moved to a standard `ChemblWriteService` configuration or if a custom `WriteService` is truly needed.
    *   If custom service is needed, ensure it fully implements the `WriteService` protocol correctly.
2.  **Standardize Artifact Planning**:
    *   Replace `ActivityArtifactPlanner` with standard `ArtifactPlanner` if possible, or register it cleanly via `artifact_planner_factory`.
3.  **Remove `run()` override**:
    *   **Critical Step**: The current `run()` method manually orchestrates stages.
    *   Refactor to use `LifecycleCoordinator` (inherited from `PipelineRuntimeBase`).
    *   Move custom logic (e.g., `build_descriptor` calls) into `prepare_run` or `ChemblExtractionService`.
4.  **Adopt `ChemblExtractionService`**:
    *   Instead of `ActivityExtractor` calling `client_factory` directly, wrap this logic in a `ChemblExtractionService` implementation.

### Phase 4: Finalization

1.  Remove "LEGACY CODE" warning from docstring.
2.  Enable strict type checking for the module.
3.  Update tests to ensure no regression in behavior.

## Execution Strategy

*   Start with **Phase 1** immediately to clear the noise.
*   Proceed to **Phase 2** to fix type checker errors.
*   **Phase 3** requires careful regression testing as it changes the runtime execution flow.
