# Activity ChEMBL: Validation

This document describes the `validate` stage of the ChEMBL Activity pipeline, which is implemented in the `ChemblActivityPipeline.validate()` method. This stage is the final gatekeeper that ensures data integrity before it is written to disk.

## 1. Overview

The validation stage leverages the base functionality of `PipelineBase.validate()` but extends it with pipeline-specific pre-validation checks and enhanced error logging. Its primary responsibilities are:

-   Performing critical pre-validation checks to catch common data integrity issues early.
-   Invoking the standard Pandera schema validation.
-   Catching `pandera.errors.SchemaErrors` to provide detailed, structured logging of validation failures.

## 2. Pre-Validation Checks

Before submitting the DataFrame to the standard Pandera validation, the `activity_chembl` pipeline performs two crucial checks:

### 2.1. `activity_id` Uniqueness (`_check_activity_id_uniqueness`)

This check ensures that the primary business key, `activity_id`, is unique across the entire dataset. If any duplicate `activity_id` values are found, the pipeline fails immediately with an error, logging the count of duplicates and a sample of the duplicate IDs. This prevents the writing of ambiguous or incorrect data.

### 2.2. Foreign Key Integrity (`_check_foreign_key_integrity`)

This function verifies the format of all ChEMBL foreign key fields (`assay_chembl_id`, `molecule_chembl_id`, etc.). It checks all non-null values against the canonical `^CHEMBL\d+$` regular expression. If any malformed identifiers are found, the pipeline fails with an error, reporting which fields contain invalid formats and how many.

## 3. Pandera Schema Validation

After the pre-validation checks pass, the DataFrame is passed to the `super().validate(payload)` method. This invokes the standard `PipelineBase` validation logic, which:

1.  Loads the Pandera schema specified in the `validation.schema_out` key of the pipeline's configuration.
2.  Applies the `strict` and `coerce` settings from the configuration.
3.  Validates the DataFrame against the schema.

## 4. Enhanced Error Handling

The `ChemblActivityPipeline` wraps the call to `super().validate()` in a `try...except` block to catch `pandera.errors.SchemaErrors`. If this exception occurs, the pipeline performs the following actions:

1.  **Extracts Structured Error Information**: It calls an internal helper (`_extract_validation_errors`) to parse the `SchemaErrors` exception and extract a structured summary, including the number of affected rows and the types of errors.
2.  **Logs a Detailed Summary**: It logs a high-level error message containing this structured summary.
3.  **Logs Individual Failure Cases**: It iterates through the first 20 failure cases provided by Pandera and logs each one as a separate, structured error message. Each message includes the row index, the `activity_id` of the failing record, the affected column, and the specific validation that failed. This level of detail is invaluable for debugging data quality issues.
4.  **Raises a `ValueError`**: Finally, it raises a `ValueError` to ensure the pipeline run is marked as a failure.

This enhanced error handling turns a generic validation failure into a detailed, actionable report, making it much easier to identify and fix problems in the source data or the transformation logic.
