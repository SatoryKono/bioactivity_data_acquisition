# 6. Determinism, Reproducibility, and Output

## Overview

Two of the core principles of the `bioetl` framework are **determinism** and **reproducibility**.
- **Determinism** means that running the same pipeline with the same configuration and input data will always produce a bit-for-bit identical output file.
- **Reproducibility** means that every output artifact is accompanied by rich metadata that allows anyone to understand its full lineage: how, when, and from what it was created.

These guarantees are primarily enforced during the framework-managed `write` stage, which handles the final materialization of the data and its associated metadata.

## Achieving Determinism

Determinism is achieved through a series of strict, automated steps performed by the `write` stage:

### 1. Stable Sorting

Before being written to a file, the final DataFrame is **always** sorted. The columns to sort by are defined in the `determinism.sort.by` section of the pipeline's configuration. This step is critical; without a stable sort order, the row order in the output file could change between runs, leading to different file hashes even if the data content is identical.

```yaml
# config.yaml
determinism:
  sort:
    by: ["assay_id", "testitem_id", "activity_id"]
```

### 2. Fixed Column Order

The `validate` stage, which runs just before the `write` stage, enforces a strict column order as defined in the Pandera schema (`class Config: ordered = True`). This ensures that the columns in the output file are always in the same sequence.

### 3. Atomic Writes

The `write` stage performs atomic file operations. The dataset is first written to a temporary file in a staging directory. Only after the write operation has completed successfully is the file moved to its final destination. This prevents consumers from ever accessing partial or corrupt files in the case of a pipeline failure during the write process.

## Achieving Reproducibility: The `meta.yaml` File

Every dataset produced by the framework is accompanied by a `meta.yaml` file. This file is the dataset's "birth certificate," containing a complete record of its origin and properties.

The metadata file is crucial for:
- **Auditing**: Verifying the source and parameters used to generate a dataset.
- **Debugging**: Comparing the configurations of two different runs to identify changes.
- **Lineage Tracking**: Understanding the full data flow from source to final artifact.

### Full `meta.yaml` Example

Below is a complete example of a `meta.yaml` file. The structure is standardized across all pipelines. For the authoritative specification, see [Determinism Policy](../determinism/00-determinism-policy.md#5-metayaml-structure).

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

### Integrity Hashes

The `meta.yaml` file includes two critical integrity hashes:

- `hash_business_key`: A SHA256 hash of the canonicalized business key columns. This hash identifies a unique business entity and is stable across pipeline runs with identical data.
- `sample_hash_row`: A SHA256 hash of a sample row (based on the first row after sorting) that verifies the integrity of row-level data. The full `hash_row` is calculated for every row in the dataset during the write stage.

The `hash_algo` field specifies the hashing algorithm used (currently `sha256`), and `hash_policy_version` tracks the version of the hashing policy for compatibility checks.
