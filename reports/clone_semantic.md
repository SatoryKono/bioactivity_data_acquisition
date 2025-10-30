# Semantic Clone Candidates

## Schema validation orchestration duplicated across pipelines
- **Pattern**: Each pipeline-specific `validate` implementation reorders columns, invokes `_validate_with_schema`, logs QC metrics, and applies referential integrity checks using nearly identical control flow.
- **Instances**:
  - `AssayPipeline.validate` lines 969-1094 [ref: repo:src/bioetl/pipelines/assay.py@test_refactoring_11]
  - `TestItemPipeline.validate` lines 1228-1348 [ref: repo:src/bioetl/pipelines/testitem.py@test_refactoring_11]
  - `TargetPipeline.validate` lines 359-630 (mirrors same schema/failure handling but with additional stage toggles) [ref: repo:src/bioetl/pipelines/target.py@test_refactoring_11]
  - `DocumentPipeline.validate` lines 1144-1358 reuse the same schema failure summarisation and QC gating patterns [ref: repo:src/bioetl/pipelines/document.py@test_refactoring_11]
- **Opportunity**: Extract a shared validator utility on `PipelineBase` that accepts schema metadata, QC metric callbacks, and referential check hooks to collapse duplicated boilerplate while keeping pipeline-specific checks pluggable.

## CLI sample limiting shim duplicated
- **Pattern**: Both Typer entrypoints bind an inline `limited_extract` wrapper that logs the sample cap and truncates the DataFrame before returning.
- **Instances**:
  - `create_pipeline_command` in `bioetl/cli/command.py` lines 182-206 [ref: repo:src/bioetl/cli/command.py@test_refactoring_11]
  - `run_target` Typer app in `scripts/run_target.py` lines 243-261 [ref: repo:src/scripts/run_target.py@test_refactoring_11]
- **Opportunity**: Introduce a reusable helper (e.g. `apply_sample_limit(pipeline, limit, logger)`) inside `bioetl.cli` and call it from both entrypoints to remove copy/paste wrappers.

## Test doubles for pipeline export logic repeatedly declared
- **Pattern**: Multiple parametrised tests declare identical inlined `RecordingPipeline`/`LimitRecordingPipeline` classes with stubbed `extract`, `transform`, `validate`, and `export` methods.
- **Instances**:
  - `tests/unit/test_pipelines.py` lines 90-155, 268-367, 390-501, 533-663, 1686-2053 [ref: repo:tests/unit/test_pipelines.py@test_refactoring_11]
  - `tests/unit/test_cli_contract.py` lines 232-307 [ref: repo:tests/unit/test_cli_contract.py@test_refactoring_11]
  - `tests/unit/test_pipeline_extract_helper.py` lines 61-91 [ref: repo:tests/unit/test_pipeline_extract_helper.py@test_refactoring_11]
- **Opportunity**: Move shared test doubles into fixtures (e.g. `tests/conftest.py`) or helper factories to prevent drift when adjusting pipeline export contract expectations.

## QC metric duplication across activity/testitem pipelines
- **Pattern**: `_calculate_qc_metrics` functions compute duplicate ratios, fallback ratios, severity thresholds, and detail payloads with similar logic but bespoke key prefixes.
- **Instances**:
  - `ActivityPipeline._calculate_qc_metrics` lines 1209-1288 [ref: repo:src/bioetl/pipelines/activity.py@test_refactoring_11]
  - `TestItemPipeline._calculate_qc_metrics` lines 1371-1422 [ref: repo:src/bioetl/pipelines/testitem.py@test_refactoring_11]
- **Opportunity**: Extract parameterised QC metric builders (duplicate detector, fallback ratio calculator) in `bioetl.utils.qc` so pipelines supply column names and threshold keys rather than re-implementing numeric plumbing.
