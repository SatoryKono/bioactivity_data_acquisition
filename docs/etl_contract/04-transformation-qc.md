# 4. The Transformation and QC Stage

## Overview

The `transform` stage is where the raw data from the `extract` stage is
converted into its final, clean, and structured form. This is where the primary
business logic of the pipeline resides. The developer implements this stage as
the `transform()` method in their `PipelineBase` subclass.

The goals of this stage are to:

1. **Clean and Normalize Data**: Apply cleaning functions and standardize data
   formats.
1. **Enrich Data**: (Optional) Combine the source data with other data sources.
1. **Shape Data**: Rename columns, create new ones, and ensure the DataFrame
   conforms to the target schema.
1. **Perform Quality Control (QC)**: Execute in-line quality checks to identify
   and flag potential data issues.

## Implementing the `transform()` Method

The `transform()` method receives the raw pandas DataFrame from the `extract`
stage and must return a new, transformed DataFrame.

```python
import pandas as pd
from bioetl.pipelines.base import PipelineBase


class MyPipeline(PipelineBase):
    def extract(self) -> pd.DataFrame:
        # ...
        pass

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df

        # Apply a sequence of transformation steps
        df = self._rename_columns(df)
        df = self._normalize_values(df)
        df = self._cast_data_types(df)
        df = self._perform_qc_checks(df)

        return df
```

It is a best practice to break down the transformation logic into a series of
smaller, private methods. This makes the process more readable, testable, and
maintainable.

### Common Transformation Steps

#### 1. Column Renaming and Selection

The first step is often to select the columns you need and rename them to match
the canonical names in the output schema.

```python
def _rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
    rename_map = {
        "api_response_id": "activity_id",
        "compound_identifier": "testitem_id",
        "assay_protocol": "assay_id",
    }
    # Keep only the columns we are renaming, plus any others we need
    required_cols = list(rename_map.keys())
    return df[required_cols].rename(columns=rename_map)
```

#### 2. Normalization

Normalization involves standardizing the values within columns. This can be
driven by the `transform.normalizers` section in the YAML configuration for
simple cases, or handled with custom code for more complex logic.

```python
def _normalize_values(self, df: pd.DataFrame) -> pd.DataFrame:
    # Example of a declarative normalizer from config
    # config.yaml -> transform.normalizers: [{ field: 'status', type: 'lowercase' }]
    df["status"] = df["status"].str.lower()

    # Example of custom normalization logic
    # Convert activity units to a standard format (e.g., nanomolar)
    df["standard_units"] = df["standard_units"].replace(
        {"nM": "nanomolar", "uM": "micromolar"}
    )

    return df
```

#### 3. Data Type Casting

Before validation, it is crucial to cast all columns to their correct data
types. This can be partially automated using the `transform.dtypes`
configuration. For more complex types like datetimes, explicit casting is
required.

```python
def _cast_data_types(self, df: pd.DataFrame) -> pd.DataFrame:
    # Cast using the dtypes defined in the YAML config
    df = df.astype(self.config.transform.dtypes)

    # Explicitly handle complex types like datetimes
    df["updated_at"] = pd.to_datetime(df["updated_at"], utc=True, errors="coerce")

    return df
```

Note the use of `errors='coerce'`, which will turn any unparseable dates into
`NaT` (Not a Time). This prevents the pipeline from failing and allows the
`validate` stage to catch the issue.

## In-line Quality Control (QC)

The `transform` stage is the ideal place to perform mid-flight quality checks.
The `bioetl` framework provides helpers to record validation issues that can be
reviewed later, without immediately halting the pipeline. This is useful for
flagging warnings or recoverable errors.

The `record_validation_issue()` method allows you to log custom QC issues.

```python
def _perform_qc_checks(self, df: pd.DataFrame) -> pd.DataFrame:
    # Example QC Check: Flag rows where pchembl_value seems implausible
    implausible_pchembl = df[df["pchembl_value"] > 12]

    for index, row in implausible_pchembl.iterrows():
        self.record_validation_issue(
            {
                "metric": "plausibility_check",
                "issue_type": "implausible_pchembl_value",
                "severity": "warning",  # This will not fail the pipeline by default
                "business_key": row["activity_id"],
                "details": f"pchembl_value of {row['pchembl_value']} is unusually high.",
            }
        )

    return df
```

By recording issues with a `severity` of `warning` or `info`, you can gather
valuable quality metrics on the data without prematurely terminating the
pipeline. The final `validate` stage will still enforce the critical,
non-negotiable rules defined in the Pandera schema.
