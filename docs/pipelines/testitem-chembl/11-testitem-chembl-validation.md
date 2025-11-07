# 11 TestItem ChEMBL Validation

**Version:** 1.0.0
**Date:** 2025-01-29
**Author:** Data Acquisition Team

## Purpose

This document describes the validation stage of the TestItem (ChEMBL) pipeline.

## Pandera Schemas

The pipeline uses `TestItemSchema` for validation:

- **Required Fields**: `molecule_chembl_id`
- **Identifier Fields**: `pubchem_cid`, `inchi`, `canonical_smiles`
- **Property Fields**: `molecular_weight`, `molecular_formula`
- **Optional Fields**: Various molecular metadata fields

## Validation Workflow

1. **Schema Validation**: Validates against TestItemSchema with strict=True
2. **Duplicate Detection**: Ensures duplicate-free `molecule_chembl_id` values
3. **Structure Validation**: Validates InChI/SMILES formats
4. **Referential Integrity**: Checks molecule references in related tables

## Constraints

- `molecule_chembl_id`: Unique, required, ChEMBL ID format
- Structure formats must be valid chemical representations

## Related Documentation

- [12-testitem-chembl-io.md](12-testitem-chembl-io.md)
- [14-testitem-chembl-qc.md](14-testitem-chembl-qc.md)
- [00-testitem-chembl-overview.md](00-testitem-chembl-overview.md)
