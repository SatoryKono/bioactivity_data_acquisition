# QC artifacts overview

Every dataset produced by BioETL is accompanied by quality control
artifacts that capture metadata, validation results, and summary
statistics.

These artifacts are essential for enforcing determinism and blocking
regressions in CI.

## meta.yaml

Each output dataset has a `meta.yaml` file stored alongside it. It
includes at least the following fields:

- `pipeline_version`
- `git_commit`
- `config_hash`
- `schema_version`
- `row_count`
- `blake2_checksum`
- `business_key_hash`
- `generated_at_utc`

These fields allow runs to be traced back to exact code, configuration,
input data, and business keys.

## Quality report CSV

For each dataset, a `*_quality_report.csv` file summarizes data quality
metrics.

The format and column order are fixed:

- `section`
- `metric`
- `column`
- `value`
- `count`
- `ratio`
- `lower_bound`
- `upper_bound`

Sections include at least `summary`, `missing`, `distribution`,
`outliers`, and any custom sections using the `custom:*` prefix.

## JSON QC file

A `*_qc.json` file captures a machine-readable summary of QC metrics.
Keys are serialized in a canonical, deterministic order to support
byte-for-byte comparisons.

Typical fields include:

- `row_count`
- `deduplicated_count`
- `duplicate_ratio`
- `business_key_fields`
- `custom_metrics`

## Correlation reports

Some datasets may also have `*_correlation_report.csv` files that
capture feature correlations.

The first column is always `feature`, and the remaining columns list
other features in alphabetical order.

## Golden checks and CI

QC artifacts are used together with golden datasets in CI:

- Golden jobs compare current outputs and QC files byte-for-byte against
  stored golden artifacts when the `--golden` flag is used.
- Any deviation in data, metadata, or QC summaries causes a non-zero
  exit code and blocks merging.

This mechanism ensures that schema, data quality, and determinism
invariants remain enforced over time.
