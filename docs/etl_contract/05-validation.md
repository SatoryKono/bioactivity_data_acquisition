# 5. The Validation Stage

> **Note**: Implementation status: **planned**. All file paths referencing `src/bioetl/` in this document describe the intended architecture and are not yet implemented in the codebase.

## Overview

The `validate` stage is the gatekeeper of data quality in the `bioetl` framework. It is a non-negotiable, framework-managed step that occurs immediately after the `transform` stage. Its purpose is to ensure that every single record in the DataFrame conforms to a strict, predefined schema before it can be written to disk.

This stage is managed entirely by the `PipelineBase` class and **should not be overridden** by developers. It automatically uses the Pandera schema specified in the `validate.schema_out` section of the pipeline's YAML configuration.

If validation succeeds, the pipeline proceeds to the `write` stage. If even a single record fails to validate, the pipeline halts immediately, logs the specific errors, and exits. This fail-fast approach prevents data corruption and ensures that only clean, compliant data is ever produced.

## Pandera Schemas

The `bioetl` framework uses [Pandera](https://pandera.readthedocs.io/en/stable/) to define and enforce data schemas. A Pandera schema is a Python class that declaratively defines the expected structure of a DataFrame, including:

-   **Column Names and Order**: Ensures all required columns are present and in the correct order.
-   **Data Types**: Enforces strict data types for each column (e.g., `int64`, `string`, `datetime64[ns, UTC]`).
-   **Constraints**: Defines rules that values must adhere to (e.g., `nullable=False`, `unique=True`, value ranges).
-   **Custom Checks**: Allows for complex, custom validation logic to be applied to one or more columns.

### Example Pandera Schema

Below is an example of a Pandera schema for a hypothetical `activity` pipeline. This file would be saved at a path like `src/bioetl/schemas/chembl/activity_out.py` and referenced in the YAML configuration.

```python
# file: src/bioetl/schemas/chembl/activity_out.py

import pandera as pa
from pandera.typing import Series, DateTime, String, Int64, Float64

class ActivitySchema(pa.SchemaModel):
    """
    Pandera schema for the final, transformed ChEMBL activity data.
    """
    activity_id: Series[Int64] = pa.Field(nullable=False, unique=True, coerce=True)
    assay_id: Series[Int64] = pa.Field(nullable=False, coerce=True)
    testitem_id: Series[Int64] = pa.Field(nullable=False, coerce=True)
    record_id: Series[Int64] = pa.Field(nullable=False, coerce=True)

    standard_type: Series[String] = pa.Field(nullable=False)
    standard_relation: Series[String] = pa.Field(nullable=True)
    standard_value: Series[Float64] = pa.Field(nullable=True)
    standard_units: Series[String] = pa.Field(nullable=True)

    pchembl_value: Series[Float64] = pa.Field(nullable=True, ge=0, le=14)

    # Business Rule: If standard_value is present, standard_units must also be present.
    @pa.dataframe_check
    def check_units_exist_if_value_exists(cls, df: pd.DataFrame) -> bool:
        value_present = df['standard_value'].notna()
        units_missing = df['standard_units'].isna()
        # The check fails if there are any rows where a value is present but units are missing.
        return not (value_present & units_missing).any()

    class Config:
        # This is critical for determinism. It forces the DataFrame columns to
        # match the order defined in this schema.
        ordered = True
        # This will cause validation to collect all errors rather than failing on the first one.
        lazy = True
```

## How It Works

1.  **Configuration**: In your pipeline's YAML file, you specify the path to your schema:
    ```yaml
    validate:
      schema_out: "schemas/chembl/activity_out.py"
      enforce_column_order: true
    ```
2.  **Execution**: After your `transform()` method finishes, the `PipelineBase.run()` method takes the resulting DataFrame and passes it to the `validate()` stage.
3.  **Validation**: The `validate()` method dynamically loads the `ActivitySchema` class from the specified file and calls its `validate()` method on the DataFrame.
4.  **Outcome**:
    -   **Success**: If the DataFrame is valid, it is returned and passed to the `write` stage.
    -   **Failure**: If validation fails, Pandera raises a `SchemaErrors` exception. The framework catches this, logs the detailed failure cases (e.g., which rows failed which checks), and terminates the pipeline run with a non-zero exit code.

By centralizing and automating the validation process, the framework ensures that data quality is a consistent, non-negotiable standard across all pipelines, rather than an ad-hoc check left to individual developers.
