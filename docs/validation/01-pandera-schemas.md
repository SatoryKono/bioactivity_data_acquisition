# Specification: Pandera Schemas and Strict Data Validation

## 1. Overview and Goals

In the `bioetl` project, data integrity is enforced through a strict data contract implemented with Pandera.

As defined in `[ref: repo:src/bioetl/pipelines/base.py@test_refactoring_32]`, every pipeline executes a `validate` stage before writing any data. This stage ensures that the transformed data conforms to a predefined schema, guaranteeing the structural integrity and type safety of all outputs.

The goals of this validation process are:
-   **Type Safety**: Ensure columns contain the correct data types.
-   **Structural Integrity**: Ensure the exact set and order of columns.
-   **Value Constraints**: Ensure data values adhere to business rules.
-   **Determinism**: A strict, ordered schema is a prerequisite for producing bit-for-bit identical outputs.

## 2. Base Schema Contract

Based on the implementation in `base.py`, every Pandera schema in the project **MUST** provide the following capabilities, though the exact implementation (e.g., `strict=True` in the schema vs. external checks) is a project convention.

-   **Strict Column Checks**: The validation process must reject dataframes with columns not explicitly defined in the schema.
-   **Column Order Enforcement**: The validation process must enforce a specific column order. The `validate` method in `base.py` contains logic to reorder columns based on a `canonical_order` list retrieved from the schema, indicating this is a critical requirement.
-   **Type Coercion**: The validation should coerce data into the specified `dtype` for each column.
-   **Explicit Nullability**: Every column must have a clearly defined nullability status.

## 3. Schema Catalog by Pipeline

This section defines the illustrative schema standards for the core pipelines. These examples are based on the data fields present in the ChEMBL configurations and pipeline files.

-   **Source Configurations**: `[ref: repo:src/bioetl/configs/pipelines/chembl/activity.yaml@test_refactoring_32]` (and others for assay, target, etc.)

### Activity Schema
| name | dtype | required | nullable | checks | comment |
|---|---|---|---|---|---|
| `activity_id` | `pa.Int64` | True | False | `unique=True` | Primary key. |
| `assay_id` | `pa.Int64` | True | False | - | Foreign key to assay. |
| `standard_type`| `pa.String` | False | True | - | Type of measurement. |
| `standard_value`|`pa.Float64`| False | True | `ge=0`| Value of measurement.|

### Assay Schema
| name | dtype | required | nullable | checks | comment |
|---|---|---|---|---|---|
| `assay_id` | `pa.Int64` | True | False | `unique=True` | Primary key. |
| `assay_type` | `pa.String`| True | False | - | Type of assay. |
| `organism` | `pa.String` | False | True | - | Test organism. |

### Target Schema
| name | dtype | required | nullable | checks | comment |
|---|---|---|---|---|---|
| `target_id` | `pa.Int64` | True | False | `unique=True` | Primary key. |
| `pref_name` | `pa.String`| True | False | - | Preferred name. |
| `target_type`| `pa.String`| True | False | - | Type of target. |

### Document Schema
| name | dtype | required | nullable | checks | comment |
|---|---|---|---|---|---|
| `document_id` | `pa.Int64` | True | False | `unique=True` | Primary key. |
| `pubmed_id` | `pa.Int64` | False | True | `ge=0` | PubMed ID. |
| `year` | `pa.Int64` | False | True | `ge=1900` | Publication year. |

### Test Item Schema
| name | dtype | required | nullable | checks | comment |
|---|---|---|---|---|---|
| `testitem_id` | `pa.Int64` | True | False | `unique=True` | Primary key. |
| `pref_name` | `pa.String`| True | False | - | Preferred name. |
| `molecule_type`| `pa.String`| False | True | - | Type of molecule. |

## 4. Types and Coercion

The project should standardize on a core set of Pandera `dtypes` (`pa.String`, `pa.Int64`, `pa.Float64`, etc.) to ensure consistency. The use of `coerce=True` in Pandera schemas is the recommended way to enforce these types.

## 5. DataFrame-Level Checks

For validations spanning multiple columns (e.g., composite key uniqueness), Pandera's DataFrame-level `Check`s are the standard mechanism.

**Example Pattern:**
```python
import pandera as pa
uniqueness_check = pa.Check(lambda df: ~df.duplicated(subset=["key_part1", "key_part2"]).any())
MySchema = pa.DataFrameSchema(checks=[uniqueness_check])
```

## 6. Strict Column Discipline

The `validate` method in `base.py` demonstrates a clear intent for strict column discipline. It attempts to reorder columns to a `canonical_order` and add any missing columns as `pd.NA`. This implies that the final written data must have a fixed set and order of columns. Using `strict=True` and `ordered=True` in Pandera schemas is the most direct way to enforce this.

## 7. Schema Versioning and Evolution

The `validate` method retrieves a schema using `get_schema(entity, schema_version)`, where the version is derived from `get_schema_metadata`. This indicates a project-level versioning policy.
- The version for a schema is managed by a central registry (accessed via `get_schema_metadata`).
- This version is recorded in the `meta.yaml` file for each output, as seen in the `set_export_metadata_from_dataframe` method in `base.py`.
- When a schema changes, its version in the registry must be incremented.

## 8. Integration with CLI and Configs

- **Invocation Point**: The `PipelineBase.run()` method calls `self.validate(df)`. `[ref: repo:src/bioetl/pipelines/base.py@test_refactoring_32]`
- **Configuration**: The `pipeline.entity` key in the pipeline's YAML config determines which schema is retrieved from the registry. Example: `[ref: repo:src/bioetl/configs/pipelines/chembl/activity.yaml@test_refactoring_32]`
- **CLI**: The CLI's role is to parse the config and instantiate the correct pipeline. The `create_pipeline_command` function (imported in `[ref: repo:src/bioetl/cli/app.py@test_refactoring_32]`) is responsible for this, eventually triggering the pipeline's `.run()` method.

## 9. Examples

**Mini-Schema (`SchemaModel` style):**
```python
import pandera as pa
from pandera.typing import Series

class ActivitySchema(pa.SchemaModel):
    activity_id: Series[int] = pa.Field(unique=True)
    assay_id: Series[int]
    standard_value: Series[float] = pa.Field(nullable=True)

    class Config:
        strict = True
        ordered = True
        coerce = True
```

**YAML Export:**
```python
yaml_schema = ActivitySchema.to_yaml()
```

## 10. Test Plan

- **Unit Tests**: Each schema should have tests that:
    - Verify a valid dataframe passes.
    - Verify a dataframe with extra/missing columns fails.
    - Verify a dataframe with incorrect column order fails.
    - Verify a dataframe with incorrect types fails.
    - Verify violation of any DataFrame-level checks fails.
- **Integration Tests**:
    - A pipeline run should fail at the `validate` stage if the transformed data is invalid.
    - The error message should clearly indicate a schema validation failure.
- **Golden Tests**: The schema of golden test files should remain stable between versions unless a schema change is intended.
