# 14 TestItem ChEMBL QC

**Version:** 1.0.0 **Date:** 2025-01-29 **Author:** Data Acquisition Team

## Purpose

This document describes QC metrics and thresholds for the TestItem (ChEMBL)
pipeline.

## QC Metrics

- Duplicate detection
- Structure representation coverage (InChI/SMILES)
- PubChem CID mapping coverage
- Molecular property validation

## QC Thresholds

- `duplicate_ratio`: 0.0 (critical)
- `structure_coverage`: Configurable threshold

## Related Documentation

- [11-testitem-chembl-validation.md](11-testitem-chembl-validation.md)
- [00-testitem-chembl-overview.md](00-testitem-chembl-overview.md)
