# Deprecations Register

| Symbol | Module | Date Announced | Removal Plan |
| --- | --- | --- | --- |
| _No deprecations registered._ | – | – | – |

## Removed [2025-11-01]

The following items were removed during repository cleanup:

### Temporary Files
- `md_scan*.txt` (7 files) — Markdown linter output files
- `md_report.txt` — Temporary lint report
- `coverage.xml` — Generated coverage artifact (should not be tracked in git)

### Archived Reports
The following reports have been moved to `docs/reports/archived/2025-11-01/` as historical artifacts:
- `acceptance-criteria-document.md`, `acceptance-criteria.md` — Acceptance criteria
- `assessment.md`, `gaps.md`, `pr-plan.md`, `test-plan.md` — Planning documents
- `COMPLETED_IMPLEMENTATION.md`, `implementation-status.md`, `implementation-examples.md` — Implementation reports
- `DOCUMENT_PIPELINE_VERIFICATION.md` — Pipeline verification report
- `FINAL_100_ROWS_REPORT.md`, `FINAL_100_ROWS_SUCCESS.md`, `FINAL_RUN_RESULTS.md`, `FINAL_STATUS.md`, `FINAL_VALIDATION_REPORT.md` — Final status reports
- `PROGRESS_SUMMARY.md`, `RUN_RESULTS_SUMMARY.md` — Progress summaries
- `REQUIREMENTS_AUDIT.md`, `REQUIREMENTS_UPDATED.md` — Requirements audits
- `RISK_REGISTER.md` — Risk register
- `SCHEMA_COMPLIANCE_REPORT.md`, `SCHEMA_GAP_ANALYSIS.md` — Schema analysis reports
- `SCHEMA_SYNC_*.md` (8 files) — Schema synchronization reports

### Archived Patches
Historical patch files have been moved to `docs/reports/archived/patches/`:
- `0001-chembl-helper.patch`
- `0002-pipeline-refactor.patch`
- `0003-test-docs.patch`
- `001-determinism-defaults.patch`
- `cli_registry_refactor.patch`

## Update Policy

All deprecations MUST be recorded in this table when the `DeprecationWarning` is introduced. Each entry MUST specify the earliest
release where the removal is planned (SemVer `MAJOR.MINOR`), and the date the warning was announced. When the removal is executed,
update the table to reflect the outcome and link to the corresponding changelog entry.

Changes to public APIs MUST follow Semantic Versioning 2.0.0. Any incompatible change is deferred to the next MAJOR release; while
a warning is active, the MINOR version MUST increment on releases that introduce or update the deprecation plan.
