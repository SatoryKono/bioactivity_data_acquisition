# 22 TestItem PubChem Transformation

**Version:** 1.0.0  
**Date:** 2025-01-29  
**Author:** Data Acquisition Team

## Purpose

This document describes the transformation stage of the TestItem (PubChem) pipeline, covering molecular structure normalization, identifier mapping, and property standardization.

## Transformation Workflow

1. **Structure Normalization**: Standardizes InChI/SMILES representations
2. **Identifier Mapping**: Ensures consistent format for PubChem CID and related identifiers
3. **Property Normalization**: Standardizes molecular property values
4. **Type Conversion**: Converts fields to appropriate data types

## Key Transformations

### Molecular Identifiers

- Normalization of `pubchem_cid` format
- InChI/SMILES standardization and validation
- Cross-reference identifier mapping

### Molecular Properties

- Molecular weight and formula normalization
- Property value standardization
- Descriptor calculation and validation

## Related Documentation

- [21-testitem-pubchem-extraction.md](21-testitem-pubchem-extraction.md) — Extraction stage
- [23-testitem-pubchem-validation.md](23-testitem-pubchem-validation.md) — Validation stage
- [00-testitem-pubchem-overview.md](00-testitem-pubchem-overview.md) — Pipeline overview
