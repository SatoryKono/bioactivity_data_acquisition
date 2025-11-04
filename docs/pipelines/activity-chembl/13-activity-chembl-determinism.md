# Activity ChEMBL: Determinism

This document describes the pipeline-specific determinism implementation for the ChEMBL Activity pipeline. While the pipeline relies on the core determinism framework provided by `PipelineBase`, it includes a crucial override to ensure that the output is always sorted correctly.

## 1. General Determinism Policy

The `activity_chembl` pipeline adheres to the standard determinism policy defined in the `[ref: repo:docs/pipelines/00-pipeline-base.md#6-determinism-and-artifacts]`. This includes:
-   **Atomic Writes**: All output files are written to temporary files and then atomically moved to their final destination.
-   **Row Hashing**: A `hash_row` is calculated for each record to ensure data integrity.
-   **Metadata**: A comprehensive `meta.yaml` file is generated for each run, capturing the configuration, source versions, and integrity hashes.

## 2. Overridden `write` Method

To guarantee byte-for-byte reproducibility, the final dataset must be sorted in a consistent order before being written to disk. The `ChemblActivityPipeline` overrides the `write()` method to enforce a specific set of sort keys.

### 2.1. Sort Key Enforcement

The overridden method ensures that the DataFrame is sorted by the following keys in this specific order:
1.  `assay_chembl_id`
2.  `molecule_chembl_id`
3.  `activity_id`

### 2.2. Implementation Details

The logic in the `write` method performs the following steps:
1.  It checks if the sort keys defined in the pipeline's configuration (`determinism.sort.by`) match the required keys listed above.
2.  If they do not match, it dynamically creates a temporary, modified `PipelineConfig` object with the correct sort keys.
3.  It temporarily replaces `self.config` with this modified config.
4.  It then calls `super().write(payload, artifacts)`, which uses the modified config to perform the sorting and writing.
5.  Finally, in a `finally` block, it restores the original configuration object.

This implementation ensures that the `activity_chembl` pipeline is always sorted correctly, regardless of the configuration provided by the user. This is a critical feature for maintaining the deterministic integrity of the pipeline's output.
