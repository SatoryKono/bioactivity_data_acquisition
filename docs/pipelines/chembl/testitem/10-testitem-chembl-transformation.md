# 10 TestItem ChEMBL Transformation

**Version:** 1.0.0 **Date:** 2025-01-29 **Author:** Data Acquisition Team

## Purpose

This document describes the transformation stage of the TestItem (ChEMBL)
pipeline, covering molecule metadata normalization, identifier mapping, and
structure handling.

## Transformation Workflow

1. **Metadata Normalization**: Standardizes molecule metadata fields
1. **Identifier Mapping**: Ensures consistent format for molecule identifiers
1. **Structure Normalization**: Handles molecular structure representations
1. **Type Conversion**: Converts fields to appropriate data types

## Key Transformations

### Molecule Identifiers

- Normalization of `molecule_chembl_id` format
- PubChem CID mapping and validation
- InChI/SMILES standardization

### Molecular Properties

- Molecular weight and formula normalization
- Property value standardization
- Classification and type assignment

## Related Documentation

- [09-testitem-chembl-extraction.md](09-testitem-chembl-extraction.md) —
  Extraction stage
- [11-testitem-chembl-validation.md](11-testitem-chembl-validation.md) —
  Validation stage
- [00-testitem-chembl-overview.md](00-testitem-chembl-overview.md) — Pipeline
  overview
