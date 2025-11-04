# 12 TestItem ChEMBL I/O

**Version:** 1.0.0  
**Date:** 2025-01-29  
**Author:** Data Acquisition Team

## Purpose

This document describes the I/O stage of the TestItem (ChEMBL) pipeline.

## Output Files

- `testitem_{date}.csv` — Main dataset
- `testitem_{date}_quality_report.csv` — QC report
- `testitem_{date}_meta.yaml` — Metadata

## Sort Keys

Stable sorting by: `['molecule_chembl_id']`

## Related Documentation

- [13-testitem-chembl-determinism.md](13-testitem-chembl-determinism.md)
- [00-testitem-chembl-overview.md](00-testitem-chembl-overview.md)
