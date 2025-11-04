# 12 Assay ChEMBL I/O

**Version:** 1.0.0  
**Date:** 2025-01-29  
**Author:** Data Acquisition Team

## Purpose

This document describes the pipeline-specific I/O configuration for the Assay (ChEMBL) pipeline.

## Pipeline-Specific Output Files

- `assay_{date}.csv` — Main dataset
- `assay_{date}_quality_report.csv` — QC report
- `assay_{date}_meta.yaml` — Metadata

## Pipeline-Specific Sort Keys

Stable sorting by: `['assay_chembl_id', 'row_subtype', 'row_index']`

## Related Documentation

- [PipelineBase I/O and Artifacts](../00-pipeline-base.md#61-io-and-artifacts) — General I/O format, atomic writing, metadata structure
- [13-assay-chembl-determinism.md](13-assay-chembl-determinism.md) — Determinism policy
- [00-assay-chembl-overview.md](00-assay-chembl-overview.md) — Pipeline overview
