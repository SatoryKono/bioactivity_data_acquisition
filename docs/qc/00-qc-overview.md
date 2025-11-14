# Quality Control and Assurance Framework

This document provides an overview of the Quality Control (QC) and Quality
Assurance (QA) methodology used in the `bioetl` framework. Its purpose is to
ensure that all data processed by the pipelines is reliable, reproducible, and
meets a high standard of quality.

## 1. Core Principles

The QC/QA framework is built on four pillars of data quality control, which are
checked at different stages of the ETL process.

### 1.1. Structural Integrity (Schema Validation)

This is the first and most critical level of control. It ensures that the data
conforms to its expected physical structure.

- **Mechanism**: [Pandera schemas][pandera-docs] are used to define the strict
  structure of a dataset, including column names, data types, and order.
- **Enforcement**: The `validate` stage of the `PipelineBase` orchestrator
  performs a rigorous check against the defined Pandera schema. Any violation
  results in an immediate pipeline failure.
- **Invariant**: All schemas **MUST** be defined with `strict=True`,
  `ordered=True`, and `coerce=True` to prevent unexpected columns, enforce a
  canonical order, and ensure type safety.

### 1.2. Content Quality (Expectations)

This level of control validates the semantic correctness and plausibility of the
data *within* the columns.

- **Mechanism**: The framework uses a system of "Expectations," which are
  verifiable assertions about the data content, analogous to the concepts in
  [Great Expectations][ge-docs]. These include checks for uniqueness,
  non-nullability, value ranges, and set membership.
- **Enforcement**: Content quality checks are performed during the `transform`
  and `validate` stages. Metrics are collected, and failures can be configured
  to either raise warnings or cause the pipeline to fail.

### 1.3. Determinism and Lineage (Reproducibility)

This level ensures that a pipeline run is repeatable and that its outputs are
verifiable.

- **Mechanism**: Every pipeline run generates a `meta.yaml` file containing a
  complete lineage of the process. This includes input parameters, configuration
  hashes, the schema version, and cryptographic hashes of the output data.
- **Enforcement**: The `write` stage is responsible for sorting the data by a
  stable key and calculating `hash_row` and `hash_business_key` values. These
  hashes provide a cryptographic guarantee of the data's integrity and are used
  in [Golden Tests](02-golden-tests.md) to detect regressions.

### 1.4. Extraction and Network Reliability

This level of control monitors the health and reliability of the data extraction
process itself.

- **Mechanism**: The `UnifiedAPIClient` and other source clients are
  instrumented to produce structured logs and metrics for all network
  operations.
- **Enforcement**: The framework tracks metrics such as HTTP error rates,
  retries, timeouts, and parsing errors. These are aggregated and reported at
  the end of a run, and can be used to trigger alerts if they exceed predefined
  thresholds.

## 2. Integration with the Pipeline Lifecycle

The QC checks are not a separate process but are deeply integrated into the
`PipelineBase` lifecycle to ensure continuous validation.

- **`extract`**: During this stage, the framework collects metrics on network
  reliability and data source health (e.g., `http_error_rate`,
  `retry_count_total`).
- **`transform`**: This stage is where content quality checks are often
  performed. Custom validation rules and expectations can be applied as data is
  normalized and cleaned.
- **`validate`**: This is the primary gate for data quality. The framework
  performs a mandatory, strict validation of the transformed data against its
  Pandera schema. No data can proceed to the `write` stage without passing this
  check.
- **`write`**: In this final stage, the framework ensures determinism by sorting
  the data, calculating integrity hashes, and generating the final `meta.yaml`
  artifact.

This integrated approach guarantees that all data produced by the framework has
been rigorously vetted at every stage of its lifecycle.

## 3. QC/QA Metrics Enforced by the Test Suite

Quality gates in the `test_refactoring_32` branch are organised around dedicated
`pytest` suites (markers: `qc`, `golden`, `determinism`) that interrogate the
`meta.yaml`, per-stage logs, and on-disk artifacts. The table below consolidates
the hard thresholds and acceptance criteria that these tests assert.

| Category                    | Metric / Expectation                                                                                               | Data Source                                 | Threshold or Comparison                                                                                                                           | CI Outcome                                    |
| --------------------------- | ------------------------------------------------------------------------------------------------------------------ | ------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------- |
| **Row volume**              | `row_count` recorded for every run plus `delta_row_count_vs_prev_run` to guard regressions.                        | `meta.yaml`                                 | Drift **warns** when the delta exceeds 25%.                                                                                                       | Warn ≥ 25%, otherwise pass.                   |
| **Duplicate control**       | `duplicate_count`, `duplicate_rate`, `deduplicated_count`, and `delta_duplicate_rate_vs_prev_run`.                 | DataFrame aggregations & `meta.yaml`        | Any duplicate count triggers a **fail**; duplicate rate drifts above 1 % (absolute delta 5 %) issue warnings.                                     | Fail when count > 0; warn on elevated rates.  |
| **Uniqueness**              | `unique_violation_count(<cols>)` across declared business keys.                                                    | DataFrame aggregation                       | Any non-zero result is a **fail**.                                                                                                                | Fail when > 0.                                |
| **Not-null & completeness** | `missing_count_by_column`, `null_rate_by_column`, `invalid_enum_count(<col>)`.                                     | DataFrame aggregation                       | Pipeline configs encode per-column tolerances; exceeding them surfaces a warning, while invalid enums or schema-required columns enforce failure. | Warn on soft limits, fail on hard violations. |
| **Schema drift**            | `dtype_mismatch_count`, strict Pandera validation, and column-order freeze checks.                                 | Pandera validation & golden comparison      | Any mismatch or reordering is a **fail**; schemas are `strict=True`, `ordered=True`, `coerce=True`.                                               | Fail on mismatch.                             |
| **Hash integrity**          | `hash_row` / `hash_business_key` equality and `hash_algo_is_valid`.                                                | Artifact comparison & `meta.yaml`           | Hash algorithm must stay `sha256`; dataset/manifests undergo byte-for-byte equality.                                                              | Fail if algorithm changes or hashes differ.   |
| **Golden parity**           | Primary dataset, `meta.yaml` (with volatile fields masked), and `*_run_manifest.json`.                             | `tests/golden/<pipeline>/<version>/` snapshots | Byte-identical comparison for datasets & manifests; structural equality for masked metadata.                                                      | Fail on any divergence.                       |
| **Network health**          | `http_error_rate`, `retry_count_total`, `429_count`, `timeout_count`, `parse_error_count`, `pagination_gap_count`. | Structured client logs                      | Warn when error/retry rates exceed configured percentages; parsing or pagination gaps are fatal.                                                  | Warn/Fail per metric.                         |

These expectations combine to cover the categories requested by compliance
reviews—row counts and drifts, hash comparison, schema drift, not-null
completeness, and uniqueness. Whenever a pipeline introduces intentional changes
(e.g., a new feature causing extra rows), the accompanying PR must document the
rationale and adjust the golden baseline as described below.

## 4. Golden Snapshot Layout and Maintenance Workflow

Golden (snapshot) artifacts live under `tests/golden/<pipeline_name>/<version>/`
with deterministic subdirectories:

1. **Primary dataset** – `dataset/<stem>.csv`, sorted by the pipeline's business keys.
1. **`meta.yaml`** – `meta/<stem>_meta.yaml` with schema version, row counts, hashes,
   and quality summary (volatile fields masked during comparison).
1. **QC bundle** – `qc/<stem>_quality_report.csv`, optional correlation report,
   and the aggregated `*_qc.csv`.
1. **Run manifest** – `manifest/<stem>_run_manifest.json` containing checksums for every artifact.

Updates follow a strict review → regeneration → approval loop:

1. **Review the change** – confirm why the current snapshot is expected to
   diverge (e.g., schema evolution, intentional logic change).
1. **Regenerate artifacts** – run the pipeline with deterministic profiles,
   producing fresh dataset, metadata, and manifest.
1. **Verify locally** – execute the golden/qc pytest suites; ensure
   `hash_row`/`hash_business_key` and schema validations still pass.
1. **Submit for approval** – include the regenerated files in a PR together with
   rationale, diff summaries, and updated QC metrics. Reviewers approve only
   after confirming the deltas are intentional.

No golden file is updated outside this process; byte-wise parity and metadata
integrity remain the default acceptance target.

## 5. CI Integration Flow

Continuous Integration pipelines orchestrate QC in four deterministic stages
that mirror the manual checklist:

1. **Environment preparation** – enforce `TZ=UTC`, install pinned dependencies,
   and clear any prior outputs (e.g., `rm -rf data/output/<pipeline>/*`).

1. **Pipeline execution** – run the CLI with determinism toggles, for example:

   ```bash
   python -m bioetl.cli.cli_app activity \
     --config configs/pipelines/activity/activity_chembl.yaml \
     --output-dir data/output/activity_chembl \
     --set determinism.enabled=true
   ```

1. **Artifact comparison & metric export** – feed the new run into the
   comparison tooling:

   ```bash
  python tools/compare_artifacts.py \
    --new-dir data/output/activity_chembl/latest \
    --golden-dir tests/golden/activity_chembl/v1
   ```

   The job publishes the dataset diff (if any), refreshed `qc_metrics.json`, and
   supporting manifests as CI artifacts.

1. **Decision & reporting** – non-zero exit codes break the build: `1` for
   pipeline/runtime failures, `2` for QC/golden mismatches. Summary tables list
   each metric with PASS/WARN/FAIL states so reviewers can spot regressions
   quickly.

This process ensures that the QC checks run automatically on every commit,
enforcing both functional correctness (schema validation) and regression control
(golden parity and metric thresholds).

## 6. Declarative QC Plan

Starting with the `QCPlan` model (`bioetl.qc.plan.QCPlan`), every pipeline
expresses which QC steps **MUST** run for its outputs. The plan is evaluated by
`QCMetricsExecutor`, which yields a `QCMetricsBundle` consumed by all report
builders.

| Field                  | Type    | Default | Description                                                                    |
| ---------------------- | ------- | ------- | ------------------------------------------------------------------------------ |
| `duplicates`           | bool    | `true`  | Include duplicate statistics (business-key aware).                             |
| `missingness`          | bool    | `true`  | Emit per-column missing counts/ratios.                                         |
| `units_distribution`   | bool    | `true`  | Use `QCUnits` helpers for `*_units` suffix distributions.                      |
| `relation_distribution`| bool    | `true`  | Same for `*_relation` suffixes.                                                |
| `correlation`          | bool    | `true`  | Compute numeric correlation matrix.                                            |
| `outliers`             | bool    | `true`  | Run deterministic IQR-based outlier detection.                                 |
| `outlier_iqr_multiplier` | float | `1.5`   | Tunable IQR multiplier (MUST be > 0).                                          |
| `outlier_min_count`    | int     | `1`     | Minimum outlier count required to log a row.                                   |
| `custom_metrics`       | tuple[str, ...] | `()` | Names resolved via `QCMetricRegistry` for pipeline-specific metrics.          |

Plans are immutable (Pydantic `ConfigDict(frozen=True)`) and pipelines override
`PipelineBase.qc_plan` to toggle metrics or register per-domain hooks. Pipelines
register bespoke QC metrics by calling `register_qc_metric("provider::metric",
callable)` and adding the registry key to `custom_metrics`. Each callable
receives the raw `DataFrame` plus a `QCExecutionContext` exposing the current
plan, business-key fields, and precomputed bundle for reuse.

`QCUnits.for_suffixes(df, column_suffixes=(...))` offers the shared
implementation used by `QCUnits.for_units` and `QCUnits.for_relation` when
pipelines must report distributions for additional suffix families.

## 7. QC Artifact Contracts

All downstream consumers rely on stable QC artifacts:

### 7.1. `*_quality_report.csv`

- Columns are fixed and ordered: `section`, `metric`, `column`, `value`, `count`,
  `ratio`, `lower_bound`, `upper_bound`.
- Allowed sections:
  - `summary`: duplicates (`row_count`, `deduplicated_count`, `duplicate_count`,
    `duplicate_ratio`).
  - `missing`: per-column missing stats (`metric == "missing_count"`).
  - `distribution`: `units_distribution` and `relation_distribution` rows with
    canonical sorting.
  - `outliers`: `metric == "iqr_outlier_count"` showing IQR bounds.
  - `custom:*`: optional rows emitted by registry-backed metrics; they MUST set
    `section` to a namespaced value (`custom:<domain>`) and a deterministic
    `metric`.
- When a plan disables a section, the report simply omits that portion while
  preserving the column layout. Pipelines **MUST NOT** mutate the format.

### 7.2. `*_qc.json`

`build_qc_metrics_payload` serialises the bundle into canonical JSON:

- Always includes numeric totals when enabled: `row_count`,
  `deduplicated_count`, `duplicate_count`, `duplicate_ratio`,
  `total_missing_values`, `columns_with_missing`.
- Distributions are deterministic dictionaries keyed by column name with sorted
  inner keys.
- Outliers render as `{column: {count, lower_bound, upper_bound}}`.
- `business_key_fields` lists the exact keys that were enforced.
- Custom metrics populate `custom_metrics` as
  `{metric_name: <callable-specific payload>}`; consumers should treat payloads
  as opaque but deterministic JSON-serialisable structures.

### 7.3. `*_correlation_report.csv`

Correlation reports now wrap the Pandas matrix in a dataclass; the persisted CSV
starts with a `feature` column followed by alphabetically sorted numeric feature
names. When `QCPlan.correlation` is `false` or insufficient numeric columns are
present, the artifact is skipped (the path is planned but left empty).

[ge-docs]: https://docs.greatexpectations.io/docs/
[pandera-docs]: https://pandera.readthedocs.io/en/stable/
