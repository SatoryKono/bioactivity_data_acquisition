# Activity ChEMBL: Quality Control

This document describes the custom Quality Control (QC) metrics generated for the ChEMBL Activity pipeline. The `ChemblActivityPipeline` overrides the default `build_quality_report` method to produce a richer, more context-specific quality report.

## 1. Overview

The QC report for the `activity_chembl` pipeline is a CSV file that provides a detailed summary of the data quality of the generated dataset. It is created by the `build_quality_report()` method, which combines the base QC metrics with a set of pipeline-specific checks.

## 2. Base Quality Report

The method first calls the default `build_default_quality_report` function, which provides a standard set of metrics for each column in the dataset, including:

-   `null_count`: The number of missing values.
-   `distinct_count`: The number of unique values.
-   `duplicate_count`: The number of duplicate values, using `activity_id` as the business key.

## 3. Custom QC Metrics

After generating the base report, the method adds several custom metrics that are specific to the ChEMBL activity data. These are added as new rows to the quality report DataFrame.

### 3.1. Foreign Key Integrity

For each of the main ChEMBL foreign key fields, a set of metrics is calculated to assess referential integrity.

-   **Fields**: `assay_chembl_id`, `molecule_chembl_id`, `target_chembl_id`, `document_chembl_id`.
-   **Metrics per field**:
    -   `integrity_ratio`: The ratio of validly formatted identifiers to the total number of non-null identifiers.
    -   `valid_count`: The number of identifiers that match the `^CHEMBL\d+$` format.
    -   `invalid_count`: The number of identifiers that do not match the format.
    -   `total_count`: The total number of non-null identifiers.

### 3.2. Measurement Distribution

To provide insight into the composition of the activity data, the report includes distribution counts for key measurement fields.

-   **`standard_type_count`**: For each unique value in the `standard_type` column (e.g., "IC50", "Ki"), the report includes a row with the total count of records of that type.
-   **`standard_units_count`**: Similarly, for each unique value in the `standard_units` column (e.g., "nM", "μM"), the report includes a row with the total count.

### 3.3. ChEMBL Validity Flags

The pipeline also reports on the presence of several boolean flags that ChEMBL provides to indicate data quality or context.

-   **Fields**: `is_citation`, `high_citation_rate`, `exact_data_citation`, `rounded_data_citation`.
-   **Metric**: For each flag, the report includes a `{flag_name}_count` metric, which is the total count of records where the flag is `True`.

## 4. Output

The final output is a single quality report CSV file containing both the base and custom metrics. This report provides a comprehensive overview of the dataset's quality, enabling downstream users to understand its characteristics and limitations.
