# 11 Assay ChEMBL Validation

**Version:** 1.0.0 **Date:** 2025-01-29 **Author:** Data Acquisition Team

## Purpose

This document describes the validation stage of the Assay (ChEMBL) pipeline,
covering Pandera schema definitions, field constraints, and validation
workflows.

## Pandera Schemas

The pipeline uses `AssaySchema` with strict validation:

- **Required Fields**: `assay_chembl_id`, `row_subtype`, `row_index`
- **Enriched Fields**: `target_chembl_id`, `assay_class_id`
- **Optional Fields**: Various assay metadata fields

## Validation Workflow

1. **Schema Validation**: Validates against AssaySchema with strict=True
1. **Referential Integrity**: Checks enriched target and assay class references
1. **Duplicate Detection**: Ensures no duplicate combinations of
   `assay_chembl_id`, `row_subtype`, `row_index`
1. **BAO Compliance**: Validates assay class mappings against BAO ontology

## Constraints

- `assay_chembl_id`: Required, ChEMBL ID format
- `row_subtype`: Required for expanded rows
- Enriched references must exist or be marked as fallback

## Related Documentation

- [12-assay-chembl-io.md](12-assay-chembl-io.md) — Output format
- [14-assay-chembl-qc.md](14-assay-chembl-qc.md) — QC metrics
- [00-assay-chembl-overview.md](00-assay-chembl-overview.md) — Pipeline overview
