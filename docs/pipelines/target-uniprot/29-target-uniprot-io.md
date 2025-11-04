# 29 Target UniProt I/O

**Version:** 1.0.0  
**Date:** 2025-01-29  
**Author:** Data Acquisition Team

## Purpose

This document describes the I/O stage of the Target (UniProt) pipeline.

## Output Files

- `target_{date}.csv` — Main dataset
- `target_{date}_quality_report.csv` — QC report
- `target_{date}_meta.yaml` — Metadata

## Sort Keys

Stable sorting by: `['uniprot_accession']`

## Related Documentation

- [30-target-uniprot-determinism.md](30-target-uniprot-determinism.md)
- [00-target-uniprot-overview.md](00-target-uniprot-overview.md)
