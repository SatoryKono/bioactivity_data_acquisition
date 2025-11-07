# 10 TestItem PubChem Transformation

**Version:** 1.0.0
**Date:** 2025-01-29
**Author:** Data Acquisition Team

## Purpose

This document describes the transformation stage of the TestItem (PubChem) pipeline, covering molecule property normalization and structure data processing.

## Transformation Overview

The transformation stage normalizes raw PubChem molecule data, processes structure information (SMILES, InChI), and standardizes molecular properties.

## Field Normalization

### Structure Normalization

- **SMILES**: Canonicalization and validation
- **InChI**: InChI key generation and validation
- **Molecular Formula**: Standardized format

### Property Normalization

- **Molecular Weight**: Numeric conversion with unit consistency
- **LogP**: Numeric conversion for lipophilicity
- **HBD/HBA**: Hydrogen bond donor/acceptor counts
- **Rotatable Bonds**: Numeric conversion

## NA Policy

- Empty strings → `pd.NA`
- CSV serialization: `na_rep=""`
- Invalid numeric values → `pd.NA`

## Related Documentation

- [09-testitem-pubchem-extraction.md](09-testitem-pubchem-extraction.md) — Extraction stage
- [23-testitem-pubchem-validation.md](23-testitem-pubchem-validation.md) — Validation stage
- [00-testitem-pubchem-overview.md](00-testitem-pubchem-overview.md) — Pipeline overview
