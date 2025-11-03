# QC/QA Metrics Catalog

This document provides a comprehensive catalog of the metrics used to measure data quality and pipeline health. Each metric is a quantifiable measure that serves as the basis for an "Expectation," which is a verifiable assertion about the data.

## 1. Integrity and Uniqueness Metrics

These metrics ensure that the primary keys and relationships within the data are sound.

| Code | Name | Level | Formula | Aggregation | Source | Threshold | Action on Fail |
|---|---|---|---|---|---|---|---|
| `DUPE_CNT` | `duplicate_count` | Content | `COUNT(rows) - COUNT(DISTINCT business_key)` | `SUM` | DataFrame Aggregation | `> 0` | **FAIL** |
| `DUPE_RATE` | `duplicate_rate` | Content | `duplicate_count / COUNT(rows)` | `AVG` | DataFrame Aggregation | `> 0.01` | WARN |
| `REF_INTEGRITY_VIOLATIONS` | `referential_integrity_violations` | Content | `COUNT(rows WHERE foreign_key IS NOT NULL AND foreign_key NOT IN target_table)` | `SUM` | DataFrame Join/Lookup | `> 0` | WARN |
| `UNIQUE_VIOLATION_CNT` | `unique_violation_count(<cols>)` | Content | `COUNT(rows) - COUNT(DISTINCT <cols>)` | `SUM` | DataFrame Aggregation | `> 0` | **FAIL** |

## 2. Completeness and Type Metrics

These metrics measure the presence of data and its conformance to the expected data types.

| Code | Name | Level | Formula | Aggregation | Source | Threshold | Action on Fail |
|---|---|---|---|---|---|---|---|
| `MISSING_CNT` | `missing_count_by_column` | Content | `COUNT(rows WHERE column IS NULL)` | `SUM` | DataFrame Aggregation | Varies | WARN |
| `NULL_RATE` | `null_rate_by_column` | Content | `missing_count_by_column / COUNT(rows)` | `AVG` | DataFrame Aggregation | Varies | WARN |
| `DTYPE_MISMATCH_CNT` | `dtype_mismatch_count` | Structure | `COUNT(rows WHERE TYPE(column) != expected_dtype)` | `SUM` | Pandera Validation | `> 0` | **FAIL** |
| `INVALID_ENUM_CNT` | `invalid_enum_count(<col>)` | Content | `COUNT(rows WHERE column NOT IN allowed_values)` | `SUM` | DataFrame Aggregation | `> 0` | **FAIL** |

## 3. Range and Distribution Metrics

These metrics validate that data values fall within expected ranges and distributions.

| Code | Name | Level | Formula | Aggregation | Source | Threshold | Action on Fail |
|---|---|---|---|---|---|---|---|
| `OUT_OF_RANGE_CNT` | `out_of_range_count(<col>, min, max)` | Content | `COUNT(rows WHERE column < min OR column > max)` | `SUM` | DataFrame Aggregation | `> 0` | WARN |
| `QUANTILE_SHIFT_P95` | `quantile_shift_p95(abs)` | Content | `ABS(p95(current_run) - p95(golden_run))` | `LATEST` | Golden Snapshot Comparison | `> 10%` | WARN |
| `NEGATIVE_VAL_CNT` | `negative_value_count(<col>)` | Content | `COUNT(rows WHERE column < 0)` | `SUM` | DataFrame Aggregation | `> 0` | WARN |
| `NON_MONOTONIC_CNT` | `non_monotonic_count(<col>)` | Content | `COUNT(rows WHERE column < LAG(column))` | `SUM` | DataFrame Aggregation | `> 0` | WARN |

## 4. Volume and Delta Metrics

These metrics track the size of the dataset and how it changes over time.

| Code | Name | Level | Formula | Aggregation | Source | Threshold | Action on Fail |
|---|---|---|---|---|---|---|---|
| `ROW_CNT` | `row_count` | Volume | `COUNT(rows)` | `SUM` | `meta.yaml` | - | INFO |
| `DEDUPE_CNT` | `deduplicated_count` | Volume | `COUNT(DISTINCT business_key)` | `SUM` | `meta.yaml` | - | INFO |
| `DELTA_ROW_CNT` | `delta_row_count_vs_prev_run` | Volume | `row_count(current) - row_count(previous)` | `LATEST` | `meta.yaml` Comparison | `> 25%` | WARN |
| `DELTA_DUPE_RATE` | `delta_duplicate_rate_vs_prev_run` | Volume | `duplicate_rate(current) - duplicate_rate(previous)` | `LATEST` | `meta.yaml` Comparison | `> 5%` | WARN |

## 5. Extraction and Network Metrics

These metrics monitor the health of the data extraction process.

| Code | Name | Level | Formula | Aggregation | Source | Threshold | Action on Fail |
|---|---|---|---|---|---|---|---|
| `HTTP_ERROR_RATE` | `http_error_rate` | Network | `COUNT(http_errors) / COUNT(total_requests)` | `AVG` | Client Logs | `> 2%` | WARN |
| `RETRY_CNT_TOTAL` | `retry_count_total` | Network | `SUM(request_retries)` | `SUM` | Client Logs | `> 5%` | WARN |
| `HTTP_429_CNT` | `429_count` | Network | `COUNT(http_status == 429)` | `SUM` | Client Logs | `> 10` | WARN |
| `TIMEOUT_CNT` | `timeout_count` | Network | `COUNT(request_timeouts)` | `SUM` | Client Logs | `> 1%` | WARN |
| `PARSE_ERROR_CNT` | `parse_error_count` | Extraction | `COUNT(parsing_failures)` | `SUM` | Client Logs | `> 0` | **FAIL** |
| `PAGINATION_GAP_CNT` | `pagination_gap_count` | Extraction | `COUNT(missing_pages)` | `SUM` | Client Logic | `> 0` | **FAIL** |

## 6. Write and Artifact Metrics

These metrics validate the final output artifacts.

| Code | Name | Level | Formula | Aggregation | Source | Threshold | Action on Fail |
|---|---|---|---|---|---|---|---|
| `WRITE_ERROR_CNT` | `write_error_count` | System | `COUNT(write_failures)` | `SUM` | Pipeline Logs | `> 0` | **FAIL** |
| `META_VALID` | `meta.yaml_is_valid` | Determinism | `VALIDATE(meta.yaml against schema)` | `BOOL` | Artifact Validation | `is False` | **FAIL** |
| `HASH_ALGO_VALID` | `hash_algo_is_valid` | Determinism | `meta.yaml.hash_algo == "sha256"` | `BOOL` | `meta.yaml` | `is False` | **FAIL** |
| `COLUMN_ORDER_FROZEN` | `column_order_is_frozen` | Determinism | `DIFF(current_columns, golden_columns)` | `DIFF` | Golden Snapshot Comparison | `is not empty` | **FAIL** |

---
*Note on Expectations: The metrics listed above form the quantitative basis for a set of verifiable "Expectations" about the data. In this framework, an Expectation is a rule that is checked during a pipeline run, and its outcome (pass, fail, or warn) is determined by comparing a metric against its defined threshold. This approach is conceptually aligned with frameworks like [Great Expectations][ge-docs].*

[ge-docs]: https://docs.greatexpectations.io/docs/
