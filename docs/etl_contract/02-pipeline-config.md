# 2. Pipeline Configuration

## Overview

Every pipeline in the `bioetl` framework is driven by a declarative YAML configuration file. This approach separates the pipeline's logic (defined in its Python class) from its behavior (defined in the YAML), making pipelines flexible, reusable, and easy to manage.

All configuration files are validated at runtime against a set of strongly-typed Pydantic models located in `src/bioetl/configs/models.py`. This ensures that all configurations are well-formed and contain all necessary parameters before the pipeline begins execution.

## Configuration Skeleton

Below is a complete skeleton of a pipeline configuration file. It includes all the primary sections and common parameters that a developer will need.

```yaml
# src/bioetl/configs/pipelines/<source>/<pipeline>.yaml

# (Optional) Inherit from base configuration profiles to reduce duplication.
profile:
  - base.yaml          # Common settings for all pipelines
  - determinism.yaml   # Standard settings for ensuring deterministic output

# -----------------------------------------------------------------------------
# Section: source
# Defines the connection details for the primary data source.
# -----------------------------------------------------------------------------
source:
  # The base URL or endpoint for the data source (e.g., an API).
  endpoint: "<required>"

  # (Optional) Rate limiting to avoid overwhelming the source.
  rps_limit: 10 # Requests per second

  # (Optional) Retry policy for transient network errors.
  retries:
    max: 5
    backoff: "exponential-jitter"
    base_seconds: 1.0

# -----------------------------------------------------------------------------
# Section: extract
# Parameters that control the data extraction process.
# -----------------------------------------------------------------------------
extract:
  # (Optional) Pagination strategy for sources that return data in pages.
  pagination:
    type: "cursor"        # "cursor" or "offset"
    cursor_key: "next"    # JSON key in the response that contains the next page URL/token
    page_size: 100        # The number of records to request per page

  # (Optional) Static query parameters to include in every request.
  params: {}

  # (Optional) Filters to apply to the data request.
  filters: {}

# -----------------------------------------------------------------------------
# Section: transform
# Rules for transforming the raw data into the target schema.
# -----------------------------------------------------------------------------
transform:
  # (Optional) Declarative normalization rules to be applied.
  normalizers: []

  # (Optional) A mapping of column names to their target pandas dtypes.
  dtypes: {}

# -----------------------------------------------------------------------------
# Section: validate
# Defines the schemas and validation rules for the pipeline.
# -----------------------------------------------------------------------------
validate:
  # (Optional) Path to the Pandera schema for validating the raw, extracted data.
  schema_in: "schemas/<source>/<pipeline>_in.py"

  # (Required) Path to the Pandera schema for validating the final, transformed data.
  schema_out: "schemas/<source>/<pipeline>_out.py"

  # (Required) Enforces that the output DataFrame's column order matches the schema.
  enforce_column_order: true

# -----------------------------------------------------------------------------
# Section: write
# Controls the format and properties of the output artifacts.
# -----------------------------------------------------------------------------
write:
  # The format for the output dataset. "parquet" is recommended.
  format: "parquet"

  # (Required) A list of columns to sort the final dataset by. This is critical
  # for ensuring deterministic output.
  sort_by: ["<stable_key1>", "<stable_key2>"]

  # (Required) A list of all columns to be included in the `hash_row` calculation.
  # This hash verifies the integrity of the entire row.
  hash_row: ["<ordered_cols_for_row_hash>"]

  # (Required) A list of columns that form the unique business key for a record.
  # This is used to calculate `hash_business_key`.
  hash_business_key: ["<ordered_cols_for_bk_hash>"]

  # The directory where the output artifacts will be written.
  output_dir: "data/output/<pipeline>"

# -----------------------------------------------------------------------------
# Section: runtime
# Parameters that control the execution environment of the pipeline.
# -----------------------------------------------------------------------------
runtime:
  # (Optional) The level of parallelism for tasks that can be multi-threaded.
  parallelism: 4

  # (Optional) The size of data chunks to process at a time.
  chunk_size: 1000

  # The logging level for the pipeline run.
  log_level: "INFO"
```

## Section Details

- **`profile`**: Allows for the composition of configurations. Common settings can be placed in base files (like `base.yaml`) and included in multiple pipeline configurations to avoid repetition.
- **`source`**: Contains everything needed to connect and communicate with the data source, including its location and policies for safe and reliable interaction (rate limiting, retries).
- **`extract`**: Governs how data is retrieved. This includes defining the pagination method, which is crucial for handling large datasets from APIs.
- **`transform`**: Provides a declarative way to perform common data transformation tasks. `normalizers` can be used for simple, reusable cleaning operations, while `dtypes` ensures that data is cast to the correct types before validation.
- **`validate`**: The cornerstone of data quality. This section links the pipeline to its Pandera schemas. `schema_out` is mandatory, as all data must be validated before it is written.
- **`write`**: Defines the properties of the final output. The `sort_by`, `hash_row`, and `hash_business_key` parameters are essential for the framework's determinism and reproducibility guarantees.
- **`runtime`**: Configures the execution environment. This allows for tuning performance (`parallelism`, `chunk_size`) and setting the verbosity of logs.
