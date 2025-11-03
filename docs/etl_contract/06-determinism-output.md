# 6. Determinism, Reproducibility, and Output

## Overview

Two of the core principles of the `bioetl` framework are **determinism** and **reproducibility**.
-   **Determinism** means that running the same pipeline with the same configuration and input data will always produce a bit-for-bit identical output file.
-   **Reproducibility** means that every output artifact is accompanied by rich metadata that allows anyone to understand its full lineage: how, when, and from what it was created.

These guarantees are primarily enforced during the framework-managed `write` stage, which handles the final materialization of the data and its associated metadata.

## Achieving Determinism

Determinism is achieved through a series of strict, automated steps performed by the `write` stage:

### 1. Stable Sorting

Before being written to a file, the final DataFrame is **always** sorted. The columns to sort by are defined in the `write.sort_by` section of the pipeline's configuration. This step is critical; without a stable sort order, the row order in the output file could change between runs, leading to different file hashes even if the data content is identical.

```yaml
# config.yaml
write:
  sort_by: ["assay_id", "testitem_id", "activity_id"]
```

### 2. Fixed Column Order

The `validate` stage, which runs just before the `write` stage, enforces a strict column order as defined in the Pandera schema (`class Config: ordered = True`). This ensures that the columns in the output file are always in the same sequence.

### 3. Atomic Writes

The `write` stage performs atomic file operations. The dataset is first written to a temporary file in a staging directory. Only after the write operation has completed successfully is the file moved to its final destination. This prevents consumers from ever accessing partial or corrupt files in the case of a pipeline failure during the write process.

## Achieving Reproducibility: The `meta.yaml` File

Every dataset produced by the framework is accompanied by a `meta.yaml` file. This file is the dataset's "birth certificate," containing a complete record of its origin and properties.

The metadata file is crucial for:
-   **Auditing**: Verifying the source and parameters used to generate a dataset.
-   **Debugging**: Comparing the configurations of two different runs to identify changes.
-   **Lineage Tracking**: Understanding the full data flow from source to final artifact.

### Full `meta.yaml` Example

Below is a complete example of a `meta.yaml` file, with explanations for each section.

```yaml
# -----------------------------------------------------------------------------
# Section: pipeline
# Information about the pipeline that generated this artifact.
# -----------------------------------------------------------------------------
pipeline:
  name: "chembl_activity"
  version: "1.0.0"
  run_id: "20231027-143000-abcdef"
  # A cryptographic hash of the exact YAML configuration used for this run.
  config_hash: "sha256:abcde12345f67890..."

# -----------------------------------------------------------------------------
# Section: source
# Details about the external data source.
# -----------------------------------------------------------------------------
source:
  system: "ChEMBL API"
  # The specific version of the source data (e.g., ChEMBL release number).
  version: "ChEMBL_33"
  endpoint: "https://www.ebi.ac.uk/chembl/api/data/activity"
  # The exact parameters sent to the source API for this run.
  request_params:
    format: "json"
    pchembl_value__isnull: false

# -----------------------------------------------------------------------------
# Section: execution
# Timings and metrics for the pipeline run.
# -----------------------------------------------------------------------------
execution:
  start_time_utc: "2023-10-27T14:30:00.123Z"
  end_time_utc: "2023-10-27T14:35:10.456Z"
  duration_seconds: 310.333
  # Millisecond timings for each stage, useful for performance analysis.
  stage_durations_ms:
    extract: 180123.45
    transform: 60234.56
    validate: 15123.78
    write: 54851.21

# -----------------------------------------------------------------------------
# Section: output
# Information about the generated dataset itself.
# -----------------------------------------------------------------------------
output:
  # The total number of rows in the dataset.
  row_count: 123456
  dataset_format: "parquet"
  determinism:
    sort_by: ["assay_id", "activity_id"]
    # The version of the hashing algorithm used.
    hash_policy_version: "1"
  # The core of data integrity verification.
  hashes:
    # A hash of the columns defined in `write.hash_business_key`.
    # This hash identifies a unique business record.
    business_key: "sha256:fedcba9876..."
    # A hash of all columns defined in `write.hash_row`.
    # This hash verifies the bit-for-bit integrity of the entire dataset.
    row: "sha256:98765fedcba..."
```

### Integrity Hashes

The `output.hashes` section provides two critical checksums for data validation:

-   `hash_business_key`: This hash is calculated from the columns that form the unique business identifier of a record (e.g., `activity_id`). It is useful for tracking specific business entities across different versions of a dataset.
-   `hash_row`: This hash is calculated from all columns in the dataset. If this hash matches between two datasets, it provides a cryptographic guarantee that their content is identical. This is the ultimate check for determinism.
