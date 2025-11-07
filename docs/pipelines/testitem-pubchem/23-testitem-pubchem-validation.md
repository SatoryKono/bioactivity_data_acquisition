# 23 TestItem PubChem Validation

**Version:** 1.0.0
**Date:** 2025-01-29
**Author:** Data Acquisition Team

## Purpose

This document describes the validation stage of the TestItem (PubChem) pipeline.

## Pandera Schemas

The pipeline uses `TestItemPubChemSchema` for validation:

- **Required Fields**: `pubchem_cid`
- **Identifier Fields**: `inchi`, `canonical_smiles`
- **Property Fields**: `molecular_weight`, `molecular_formula`
- **Optional Fields**: Various molecular metadata fields

## Validation Workflow

1. **Schema Validation**: Validates against TestItemPubChemSchema with strict=True
2. **Duplicate Detection**: Ensures duplicate-free `pubchem_cid` values
3. **Structure Validation**: Validates InChI/SMILES formats
4. **Property Validation**: Validates numeric property ranges

## Constraints

- `pubchem_cid`: Unique, required, positive integer
- Structure formats must be valid chemical representations

## Related Documentation

- [24-testitem-pubchem-io.md](24-testitem-pubchem-io.md)
- [26-testitem-pubchem-qc.md](26-testitem-pubchem-qc.md)
- [00-testitem-pubchem-overview.md](00-testitem-pubchem-overview.md)
