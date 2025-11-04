# Determinism Policy Specification

> **Note**: Implementation status: **planned**. All file paths referencing `src/bioetl/` in this document describe the intended architecture and are not yet implemented in the codebase.

## 1. Scope and Invariants

A "deterministic output" in the `bioetl` project guarantees that two pipeline runs with identical inputs and configurations will produce bit-for-bit identical output artifacts. This policy is the cornerstone of data integrity, reproducibility, and automated quality control.

The following invariants **must** be upheld for every deterministic run:

-   **Stable Row Order**: Rows in the output dataset must be in a consistent, predictable order.
-   **Stable Column Order**: Columns must appear in a fixed, predefined order.
-   **Consistent Serialization**: Data types (numbers, dates, booleans) must be serialized to strings in a uniform format.
-   **Fixed Environment**: All operations must behave as if they are in a fixed locale (`C`) and timezone (`UTC`).
-   **Frozen Schema**: The data must conform to a versioned Pandera schema.
-   **Reproducible Hashes**: Integrity hashes (`hash_row`, `hash_business_key`) must be identical across identical runs.
-   **Atomic Writes**: Output artifacts are written atomically to prevent partial or corrupt files.

This policy is enforced by the `PipelineBase` orchestrator, which applies settings from standard configuration profiles. The CLI automatically includes `base.yaml` and `determinism.yaml` for every run, ensuring these invariants are applied consistently.

[ref: repo:src/bioetl/pipelines/base.py@refactoring_001]
[ref: repo:src/bioetl/cli/app.py@refactoring_001]

## 2. Stable Sort Keys by Pipeline

To guarantee a stable row order, every pipeline **must** define a `determinism.sort.by` key in its configuration. The following table documents the established sort keys for the primary ChEMBL pipelines.

| Pipeline          | Sort Key(s)                               | Justification & Tie-Breaker Policy                                                                                                                                      | Source References                                                                                                |
| ----------------- | ----------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------- |
| **`activity`**    | `["assay_id", "testitem_id", "activity_id"]` | `activity_id` is the primary business key and is unique. `assay_id` and `testitem_id` are included for locality and creating a more human-readable sort. Nulls are not expected in these key fields. | [ref: repo:src/bioetl/configs/pipelines/chembl/activity.yaml@refactoring_001]                                |
| **`assay`**       | `["assay_id"]`                            | `assay_id` is the primary business key and unique identifier for an assay. No tie-breaker is needed. Nulls are not permitted.                                              | [ref: repo:src/bioetl/configs/pipelines/chembl/assay.yaml@refactoring_001]                                   |
| **`target`**      | `["target_id"]`                           | `target_id` is the primary business key and unique identifier for a target. No tie-breaker is needed. Nulls are not permitted.                                              | [ref: repo:src/bioetl/configs/pipelines/chembl/target.yaml@refactoring_001]                                  |
| **`document`**    | `["year", "document_id"]`                 | `document_id` is the primary business key. `year` is included as the primary sort key for chronological grouping, with `document_id` acting as the tie-breaker. Nulls are not expected. | [ref: repo:src/bioetl/configs/pipelines/chembl/document.yaml@refactoring_001]                              |
| **`testitem`**    | `["testitem_id"]`                         | `testitem_id` (the molecule's ChEMBL ID) is the primary business key and is unique. No tie-breaker is needed. Nulls are not permitted.                                     | [ref: repo:src/bioetl/configs/pipelines/chembl/testitem.yaml@refactoring_001]                              |

## 3. Value Canonicalization

Before any sorting or hashing operations, all data values must be brought to a standard, "canonical" form. This prevents inconsistencies arising from different data representations.

-   **Whitespace**: All strings must be trimmed of leading and trailing whitespace.
-   **Case Normalization**: Identifiers and controlled vocabulary fields should be normalized to a consistent case (typically lowercase).
-   **Null Values**: All forms of nulls (`None`, `np.nan`, `pd.NA`) must be unified to a single representation (e.g., an empty string `""` in CSVs, as defined in `na_rep`).
-   **Numbers**: Floating-point numbers must be serialized to a fixed precision (e.g., 6 decimal places).
-   **Dates and Times**: All datetime values must be converted to UTC and serialized in ISO-8601 format (e.g., `YYYY-MM-DDTHH:MM:SS.ffffffZ`).
-   **Complex Types**: Lists and dictionaries must be serialized to JSON with `sort_keys=True` to ensure a stable string representation.

## 4. Hash Policy

The framework uses hashes to verify data integrity. The current implementation uses the **SHA256** algorithm.

[ref: repo:src/bioetl/core/hashing.py@refactoring_001]

-   **`hash_row`**:
    -   **Purpose**: To verify the integrity of an entire data row.
    -   **Process**:
        1.  Select the subset of columns defined in the `determinism.hashing.row_fields` configuration key.
        2.  For each row, create a dictionary of these fields.
        3.  Apply the canonicalization rules from the previous section to the values.
        4.  Remove any keys where the value is null.
        5.  Serialize the dictionary to a compact JSON string with sorted keys.
        6.  The `hash_row` is the SHA256 hexdigest of this UTF-8 encoded JSON string.
    -   **Exclusions**: Fields that are non-deterministic by nature, such as `generated_at` or `run_id`, **must not** be included in the `hash_row` calculation.

-   **`hash_business_key`**:
    -   **Purpose**: To provide a stable, unique identifier for a business entity across pipeline runs.
    -   **Process**:
        1.  Concatenate the canonicalized values of the business key columns.
        2.  The `hash_business_key` is the SHA256 hexdigest of this UTF-8 encoded string.
    -   **Salting**: No salt is used.

-   **Schema Migration**: If a schema change alters the data type or content of a field included in a hash, the `hash_row` for affected rows **will change**. This is expected behavior and a key feature of integrity checking. Any such change must be accompanied by a schema version bump and regeneration of golden test files.

## 5. `meta.yaml` Structure

The `meta.yaml` file is the definitive record of a pipeline run. It is a YAML file with alphabetically sorted keys, containing the following structure:

```yaml
# Full meta.yaml Example
artifacts:
  dataset: path/to/activity.parquet
  quality_report: path/to/qc/activity_quality_report.csv
column_order:
  - activity_id
  - assay_id
  # ... all other columns in fixed order
column_order_source: schema
config_fingerprint: "sha256:abcde12345..."
config_snapshot:
  path: configs/pipelines/chembl/activity.yaml
  sha256: "sha256:fghij67890..."
deduplicated_count: 0
generated_at_utc: "2025-11-03T01:15:00.123456Z"
hash_algo: sha256
hash_business_key: "sha256:9f86d081884c7d65..."
hash_policy_version: '1.0'
inputs:
  # Paths and hashes of input files, if applicable
  - path: data/input/activity.csv
    sha256: "sha256:..."
outputs:
  # Paths and file hashes of all generated artifacts
  - path: activity.parquet
    sha256: "sha256:..."
  - path: activity_quality_report.csv
    sha256: "sha256:..."
pipeline:
  name: "activity"
  version: "1.0.0"
qc:
  duplicates: 0
  missing_values: 125
  referential_integrity_violations: 0
row_count: 10000
run_id: "20251103-011500-abcdef"
sample_hash_row: "sha256:ab0c12de34..."
schema_version: "1.2.0"
source_lineage:
  chembl_release: "33"
```

## 6. `determinism.yaml` Template

This profile provides the baseline settings for ensuring deterministic runs. It is located at `configs/profiles/determinism.yaml`.

[ref: repo:configs/profiles/determinism.yaml@refactoring_001]

```yaml
# /configs/profiles/determinism.yaml
determinism:
  serialization:
    csv: { separator: ",", quoting: "ALL", na_rep: "" }
    booleans: ["True", "False"]
    nan_rep: "NaN"
  sorting:
    direction: "ascending"
    na_position: "last"
  hashing:
    algorithm: "sha256"
    exclude_fields: ["generated_at", "run_id"]
  environment:
    timezone: "UTC"
    locale: "C"
  write:
    strategy: "atomic"
  meta:
    location: "sibling"
    include_fields: []
    exclude_fields: []
```

## 7. Golden Testing

The primary strategy for testing determinism is "golden testing."

-   **Location**: Golden files are stored in `tests/golden/<pipeline_name>/`.
-   **Process**:
    1.  A test runs a pipeline against a fixed input dataset.
    2.  The output dataset file and `meta.yaml` file are captured.
    3.  The test compares the SHA256 hash of the new outputs against the hashes of the "golden" files stored in the repository.
    4.  A secondary check verifies that the column order in the output matches the expected order.
-   **Exclusions**: For `meta.yaml`, fields that are expected to change with every run (`generated_at_utc`, `run_id`) must be excluded from the comparison.

## 8. Schema Migration Rules

-   **Versioning**: All Pandera schemas must be versioned. The version is recorded in `meta.yaml`.
-   **Compatibility**: Changes to a schema (e.g., adding a nullable column) are considered backward-compatible. Breaking changes (e.g., changing a data type, removing a column, changing a sort key) require a major version bump.
-   **Golden Artifacts**: Any schema change that alters the output artifacts **must** be accompanied by a regeneration of the corresponding golden test files.

## 9. `write()` Finalization Pseudocode

This pseudocode details the sequence of operations within the `write()` stage to ensure determinism. The public API method is `write()`, though some implementations may use `export()` as an internal method name.

```python
# Pseudocode within the framework's UnifiedOutputWriter class

def write_final_artifacts(df: pd.DataFrame, config: PipelineConfig) -> "WriteResult":

    log.info("Finalization started.")

    # 1. Canonicalization (assumed to be done during transform)
    # The DataFrame arriving here is expected to be fully canonicalized.

    # 2. Sorting
    sort_keys = config.determinism.sort.by
    df = df.sort_values(by=sort_keys, kind="stable", na_position="last").reset_index(drop=True)
    log.info("DataFrame sorted.", sort_keys=sort_keys)

    # 3. Freeze Column Order
    # The Pandera schema validation already enforced this, but we can re-apply
    # from a definitive list to be certain.
    final_column_order = get_column_order_from_schema(config.validate.schema_out)
    df = df[final_column_order]
    log.info("Column order frozen.")

    # 4. Calculate Hashes (if not already present)
    # This step is typically done at the end of the transform stage row-by-row.
    if "hash_row" not in df.columns:
        df["hash_row"] = df.apply(
            lambda row: generate_hash_row(row[config.determinism.hashing.row_fields]),
            axis=1
        )
    if "hash_business_key" not in df.columns:
        df["hash_business_key"] = df[config.determinism.hashing.business_key_field].apply(generate_hash_business_key)
    log.info("Integrity hashes calculated.")

    # 5. Atomic Write of Dataset
    dataset_path = build_output_path(...)
    atomic_writer = AtomicWriter(run_id)
    atomic_writer.write(df, dataset_path, format=config.write.format)
    log.info("Dataset written atomically.", path=dataset_path)

    # 6. Generate meta.yaml
    meta_path = dataset_path.with_name("meta.yaml")
    metadata = generate_metadata(df, config, dataset_path) # Gathers all fields
    atomic_writer.write_json_as_yaml(metadata, meta_path, sort_keys=True)
    log.info("Metadata file generated.", path=meta_path)

    # 7. Post-Write Validation (optional but recommended)
    # For example, read back the file and check row count.
    log.info("Post-write validation completed.")

    log.info("Finalization complete.")
    return WriteResult(...)
```

## 10. CLI Integration

The determinism policy is automatically enforced by the CLI.

[ref: repo:src/bioetl/cli/app.py@refactoring_001]

-   **Profile Injection**: The `README.md` and CLI implementation confirm that the `determinism.yaml` profile is automatically loaded for every pipeline run, establishing its settings as the default.
-   **Priority**: Parameters defined directly in a pipeline's specific YAML file (e.g., `activity.yaml`) will override the defaults set by `determinism.yaml`.
-   **Error Codes**: Failures related to determinism (e.g., a golden test mismatch) result in a non-zero exit code.
