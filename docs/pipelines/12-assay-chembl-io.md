# 12 Assay ChEMBL I/O

**Version:** 1.0.0  
**Date:** 2025-01-29  
**Author:** Data Acquisition Team

## Purpose

This document describes the I/O stage of the Assay (ChEMBL) pipeline.

## Output Files

- `assay_{date}.csv` — Main dataset
- `assay_{date}_quality_report.csv` — QC report
- `assay_{date}_meta.yaml` — Metadata

## Sort Keys

Stable sorting by: `['assay_chembl_id', 'row_subtype', 'row_index']`

## Related Documentation

- [13-assay-chembl-determinism.md](13-assay-chembl-determinism.md) — Determinism
- [00-assay-chembl-overview.md](00-assay-chembl-overview.md) — Pipeline overview
