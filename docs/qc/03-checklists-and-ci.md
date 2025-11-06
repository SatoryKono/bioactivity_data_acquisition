# CI Integration and Checklists

This document provides checklists and guidelines for integrating the QC/QA framework and Golden Tests into a Continuous Integration (CI) pipeline.

## 1. CI Job Stages

A typical CI workflow for running a QC test for a pipeline (e.g., `activity_chembl`) **SHOULD** consist of the following stages:

### Stage 1: Pre-run Setup

This stage prepares the environment to ensure a deterministic and isolated run.

- [ ] **Set Timezone**: The environment timezone **MUST** be set to `UTC` to ensure consistent timestamp generation.
  - `export TZ=UTC`
- [ ] **Prepare Environment**: Use a deterministic environment (e.g., a locked Docker image or a `requirements.txt` file) to prevent dependency-related fluctuations.
- [ ] **Clean Output Directory**: The output directory **MUST** be empty before the run to prevent contamination from previous runs.
  - `rm -rf data/output/activity_chembl/*`

### Stage 2: Pipeline Execution

This stage runs the ETL pipeline using a deterministic configuration profile.

- [ ] **Run Command**: Execute the pipeline using the `bioetl.cli.main` module. The run **MUST** use a configuration profile that enables determinism.

  - ```bash
    python -m bioetl.cli.main activity \
      --config configs/pipelines/activity/activity_chembl.yaml \
      --output-dir data/output/activity_chembl \
      --set determinism.enabled=true
    ```

- [ ] **Collect Artifacts**: The primary dataset (e.g., `.parquet`), the `meta.yaml`, and the `manifest.txt` **MUST** be collected for the next stage.

### Stage 3: Comparison and Validation

This stage compares the newly generated artifacts against the golden versions.

- [ ] **Compare Artifacts**: Use a dedicated comparison script or command to perform the comparison logic defined in the [Golden Test Policy](02-golden-tests.md).

  - ```bash
    # Pseudocode for a comparison script
    python tools/compare_artifacts.py \
      --new-dir data/output/activity_chembl/run_123 \
      --golden-dir tests/golden/activity_chembl
    ```

- [ ] **Check Exit Code**: The comparison script **MUST** exit with a non-zero exit code if any discrepancy is found.

### Stage 4: Reporting and Decision

This final stage reports the results and determines whether the CI job should pass or fail.

- [ ] **Publish Reports**: Any generated diff reports or metric summaries **SHOULD** be published as CI artifacts for review.
- [ ] **Fail the Build**: The CI job **MUST** fail if:
  - The pipeline execution itself fails (non-zero exit code).
  - The comparison script fails (non-zero exit code).
  - Any critical metric threshold defined in the [Metrics Catalog](01-metrics-catalog.md) is breached.

## 2. Exit Code Policy

- **`0` (Success)**: The pipeline run and all QC checks completed successfully.
- **`1` (Application Error)**: A critical error occurred during pipeline execution (e.g., a Python exception, a schema validation failure).
- **`2` (QC Failure)**: The pipeline ran successfully, but a critical QC check failed (e.g., a golden test mismatch, a metric threshold was breached).

## 3. Reporting

The results of the QC checks **SHOULD** be reported in a clear, machine-readable format.

- **Metrics**: Metrics **SHOULD** be written to a `qc_metrics.json` file in the output directory.
- **Diffs**: Golden test failures **SHOULD** generate a `diff.txt` or similar report detailing the exact discrepancies found.
- **CI Summary**: The CI job summary **SHOULD** display a high-level overview of the QC results in a tabular format for quick review.

**Example CI Summary Output:**

```markdown
| QC Check | Status | Details |
|---|---|---|
| **Pipeline Execution** | ✅ PASS | |
| **Pandera Validation** | ✅ PASS | |
| **Golden Test (Dataset)** | ❌ FAIL | `sha256` mismatch |
| **Golden Test (Metadata)**| ✅ PASS | |
| **Metric: `duplicate_rate`**| ✅ PASS | `0.0 <= 0.01` |
| **Metric: `http_error_rate`** | ⚠️ WARN | `0.03 > 0.02` |
```
