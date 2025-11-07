# 11 Target ChEMBL Validation

**Version:** 1.0.0
**Date:** 2025-01-29
**Author:** Data Acquisition Team

## Purpose

This document describes the validation stage of the Target (ChEMBL) pipeline, covering Pandera schema definitions, field constraints, and validation workflows.

## Pandera Schemas

The pipeline uses `TargetSchema` for validation:

- **Required Fields**: `target_chembl_id`
- **Identifier Fields**: `uniprot_accession`, `target_type`
- **Metadata Fields**: `pref_name`, `organism`, `component_type`
- **Optional Fields**: Various target metadata fields

## Validation Workflow

1. **Schema Validation**: Validates against TargetSchema with strict=True
2. **Duplicate Detection**: Ensures duplicate-free `target_chembl_id` values
3. **UniProt Validation**: Validates UniProt accession formats
4. **Referential Integrity**: Checks target references in related tables

## Constraints

- `target_chembl_id`: Unique, required, ChEMBL ID format
- `uniprot_accession`: Valid UniProt accession format (if present)
- `target_type`: Must match controlled vocabulary

## Related Documentation

- [12-target-chembl-io.md](12-target-chembl-io.md) — Output format
- [14-target-chembl-qc.md](14-target-chembl-qc.md) — QC metrics
- [00-target-chembl-overview.md](00-target-chembl-overview.md) — Pipeline overview
