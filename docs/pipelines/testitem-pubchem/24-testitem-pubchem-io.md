# 24 TestItem PubChem I/O

**Version:** 1.0.0  
**Date:** 2025-01-29  
**Author:** Data Acquisition Team

## Purpose

This document describes the I/O stage of the TestItem (PubChem) pipeline.

## Output Files

- `testitem_{date}.csv` — Main dataset
- `testitem_{date}_quality_report.csv` — QC report
- `testitem_{date}_meta.yaml` — Metadata

## Sort Keys

Stable sorting by: `['pubchem_cid']`

## Related Documentation

- [25-testitem-pubchem-determinism.md](25-testitem-pubchem-determinism.md)
- [00-testitem-pubchem-overview.md](00-testitem-pubchem-overview.md)
