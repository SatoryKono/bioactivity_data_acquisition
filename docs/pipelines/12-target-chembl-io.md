# 12 Target ChEMBL I/O

**Version:** 1.0.0  
**Date:** 2025-01-29  
**Author:** Data Acquisition Team

## Purpose

This document describes the I/O stage of the Target (ChEMBL) pipeline.

## Output Files

- `target_{date}.csv` — Main dataset
- `target_{date}_quality_report.csv` — QC report
- `target_{date}_meta.yaml` — Metadata

## Sort Keys

Stable sorting by: `['target_chembl_id']`

## Related Documentation

- [13-target-chembl-determinism.md](13-target-chembl-determinism.md) — Determinism
- [00-target-chembl-overview.md](00-target-chembl-overview.md) — Pipeline overview
