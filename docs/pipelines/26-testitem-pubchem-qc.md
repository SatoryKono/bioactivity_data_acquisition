# 26 TestItem PubChem QC

**Version:** 1.0.0  
**Date:** 2025-01-29  
**Author:** Data Acquisition Team

## Purpose

This document describes QC metrics and thresholds for the TestItem (PubChem) pipeline.

## QC Metrics

- Duplicate detection
- Structure representation coverage (InChI/SMILES)
- Property value validation
- Identifier integrity

## QC Thresholds

- `duplicate_ratio`: 0.0 (critical)
- `structure_coverage`: Configurable threshold

## Related Documentation

- [23-testitem-pubchem-validation.md](23-testitem-pubchem-validation.md)
- [00-testitem-pubchem-overview.md](00-testitem-pubchem-overview.md)
