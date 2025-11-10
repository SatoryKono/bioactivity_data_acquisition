# Data Schemas and Validation

This document defines the standards for data schema definitions and validation using Pandera in the `bioetl` project.

## Principles

- **Mandatory Schemas**: All output tables **MUST** have Pandera schemas defined.
- **Validate Before Write**: Data **MUST** be validated before writing to files.
- **Schema Versioning**: Schemas **MUST** be versioned to track changes over time.
- **Fixed Column Order**: Column order **MUST** be fixed and enforced via `column_order`.
- **Controlled Vocabularies**: Valid values **SHOULD** reference controlled dictionaries and ontologies (e.g., BAO).

## Pandera Schema Requirements

### Schema Definition

All schemas **MUST** be defined in the schema registry (typically `src/bioetl/schemas/`) using Pandera's `DataFrameSchema` or `SchemaModel`:

```python
from pandera.pandas import DataFrameSchema, Column, Check, Index

ActivitySchema = DataFrameSchema(
    columns={
        "activity_id": Column(str, nullable=False),
        "assay_id": Column(str, nullable=False),
        "value": Column(float, nullable=False),
        "unit": Column(str, nullable=False, checks=Check.isin(["nM", "uM", "mM"])),
    },
    index=Index(int),
    strict=True,
    coerce=True,
    ordered=True,  # Enforces column order
)
```

### Column Order

Schemas **MUST** enforce fixed column order via `ordered=True` in the schema configuration:

```python
class Config:
    ordered = True  # Required for deterministic output
```

Column order **MUST** match the order in the pipeline's `column_order` configuration.

### Valid Examples

```python
from pandera.pandas import DataFrameSchema, Column, Check
from pandera.typing import DataFrame

ActivitySchema = DataFrameSchema(
    columns={
        "activity_id": Column(str, nullable=False, unique=True),
        "assay_id": Column(str, nullable=False),
        "value": Column(float, nullable=False, checks=Check.greater_than(0)),
        "unit": Column(str, nullable=False),
    },
    strict=True,
    coerce=True,
    ordered=True,
)

def validate_activity_data(df: DataFrame) -> DataFrame[ActivitySchema]:
    """Validate activity data against schema."""
    return ActivitySchema.validate(df)
```

### Invalid Examples

```python
# Invalid: missing ordered=True
ActivitySchema = DataFrameSchema(
    columns={...},
    ordered=False  # SHALL NOT omit ordered=True
)

# Invalid: no validation before write
def write_data(df: pd.DataFrame, path: Path):
    df.to_csv(path)  # SHALL NOT write without validation
```

## Validation Workflow

### Pre-Write Validation

Data **MUST** be validated before writing:

```python
from pandera.errors import SchemaError

def write_validated_data(df: pd.DataFrame, schema: DataFrameSchema, path: Path):
    try:
        validated_df = schema.validate(df)
        # Only write if validation passes
        write_to_file(validated_df, path)
    except SchemaError as e:
        log.error("Schema validation failed", error=str(e))
        raise
```

### Validation in Pipeline

The pipeline's `validate` stage **MUST** run before the `write` stage:

1. Load data from transformation stage
2. Apply Pandera schema validation
3. Ensure column order matches schema
4. Raise errors on validation failure (never write invalid data)

## Schema Versioning

Schemas **MUST** be versioned to track changes:

- Version number in schema name or metadata
- Changes documented in CHANGELOG.md
- Migration scripts for schema updates
- Backward compatibility considerations

### Valid Examples

```python
# Schema with explicit version
ActivitySchemaV1 = DataFrameSchema(...)
ActivitySchemaV2 = DataFrameSchema(...)  # Updated schema

# Schema with version in metadata
ActivitySchema = DataFrameSchema(
    ...,
    metadata={"version": "1.0.0", "created": "2024-01-01"}
)
```

## Controlled Vocabularies

Valid values **SHOULD** reference controlled dictionaries:

- BAO (BioAssay Ontology) for assay types
- ChEBI for chemical entities
- UniProt for protein identifiers
- IUPAC for units

### Valid Examples

```python
from bioetl.normalizers import BAO_ASSAY_TYPES

AssaySchema = DataFrameSchema(
    columns={
        "assay_type": Column(
            str,
            nullable=False,
            checks=Check.isin(BAO_ASSAY_TYPES)  # Controlled vocabulary
        ),
    }
)
```

## Schema Migration

When changing schemas:

1. **Document Changes**: Update CHANGELOG.md with migration notes
2. **Update QC**: Regenerate QC reports for affected tables
3. **Update Docs**: Synchronize documentation with new schema
4. **Version Bump**: Increment schema version

### Breaking Changes

Breaking schema changes **MUST** include:

- Migration path for existing data
- Deprecation notice in DEPRECATIONS.md
- Version compatibility matrix

## Nullability Policy

Nullability **MUST** be explicitly defined for all columns:

- `nullable=False`: Required field (default for primary keys)
- `nullable=True`: Optional field (must be justified)

### Valid Examples

```python
ActivitySchema = DataFrameSchema(
    columns={
        "activity_id": Column(str, nullable=False),  # Required
        "comments": Column(str, nullable=True),  # Optional, justified
    }
)
```

## Data Type Coercion

Schemas **SHOULD** use `coerce=True` to automatically convert compatible types:

- String to numeric where appropriate
- Float to int where applicable
- String normalization (trimming, case)

### Valid Examples

```python
ActivitySchema = DataFrameSchema(
    columns={
        "value": Column(float, coerce=True),  # Auto-convert strings to float
        "activity_id": Column(str, coerce=True),  # Trim whitespace
    },
    coerce=True,  # Global coercion
)
```

## Schema Registry

All schemas **SHOULD** be registered in a central schema registry:

- Location: `src/bioetl/schemas/`
- Naming: `{entity}_schema.py` (e.g., `activity_schema.py`)
- Export: `__all__` lists public schemas

## References

- Pandera documentation: https://pandera.readthedocs.io/
- Schema policy: [`docs/schemas/00-pandera-policy.md`](../schemas/00-pandera-policy.md)
- Determinism guidelines: [`docs/styleguide/04-deterministic-io.md`](./04-deterministic-io.md)
