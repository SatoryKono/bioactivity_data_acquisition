# 11 Activity ChEMBL Validation

**Version:** 1.0.0 **Date:** 2025-01-29 **Author:** Data Acquisition Team

## Purpose

This document describes the validation stage of the Activity (ChEMBL) pipeline,
covering Pandera schema definitions, field constraints, and validation
workflows.

## Pandera Schemas

The pipeline uses the `ActivitySchema` for validation:

- **Required Fields**: `activity_id`, `assay_id`, `molecule_chembl_id`
- **Measurement Fields**: `standard_type`, `standard_value`, `standard_units`,
  `standard_relation`
- **Optional Fields**: `pchembl_value`, `data_validity_comment`,
  `potential_duplicate`

## Validation Workflow

1. **Schema Validation**: Validates against ActivitySchema with strict=True
1. **Duplicate Detection**: Ensures duplicate-free `activity_id` values
1. **Foreign Key Integrity**: Validates references to assays and molecules
1. **Measurement Validity**: Checks numeric ranges and unit consistency

## Constraints

- `activity_id`: Unique, required, ChEMBL ID format
- `standard_value`: Numeric, nullable, within reasonable bounds
- `standard_units`: Must match controlled vocabulary
- Foreign key references must exist in source tables

## Error Handling

Validation failures are logged with detailed context:

- Failed field name and value
- Expected vs actual value
- Row index and activity ID

## Related Documentation

- [12-activity-chembl-io.md](12-activity-chembl-io.md) — Output format
- [13-activity-chembl-determinism.md](13-activity-chembl-determinism.md) —
  Determinism
- [00-activity-chembl-overview.md](00-activity-chembl-overview.md) — Pipeline
  overview
