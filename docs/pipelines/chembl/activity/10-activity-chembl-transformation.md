# 10 Activity ChEMBL Transformation

**Version:** 1.0.0 **Date:** 2025-01-29 **Author:** Data Acquisition Team

## Purpose

This document describes the transformation stage of the Activity (ChEMBL)
pipeline, covering measurement field normalization, value cleaning, and data
type conversions.

## Transformation Workflow

The transformation stage receives raw activity data from the extraction stage
and applies the following steps:

1. **Measurement Field Normalization**: Standardizes numeric values, units, and
   measurement types
1. **Identifier Mapping**: Ensures consistent format for assay and molecule
   identifiers
1. **Value Cleaning**: Handles missing values, invalid measurements, and edge
   cases
1. **Type Conversion**: Converts fields to appropriate data types

## Key Transformations

### Measurement Values

- Normalization of numeric values (removal of non-numeric characters, handling
  of ranges)
- Unit standardization to canonical forms
- Handling of comparison operators (`>`, `<`, `~`)

### Identifiers

- Validation and normalization of ChEMBL IDs (`activity_id`, `assay_id`,
  `molecule_chembl_id`)
- Foreign key integrity checks

### Data Types

- Conversion of string representations to appropriate numeric types
- Date parsing and normalization
- Boolean flag handling

## Related Documentation

- [09-activity-chembl-extraction.md](09-activity-chembl-extraction.md) —
  Extraction stage
- [11-activity-chembl-validation.md](11-activity-chembl-validation.md) —
  Validation stage
- [00-activity-chembl-overview.md](00-activity-chembl-overview.md) — Pipeline
  overview
