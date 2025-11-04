# 12 Document ChEMBL I/O

**Version:** 1.0.0  
**Date:** 2025-01-29  
**Author:** Data Acquisition Team

## Purpose

This document describes the pipeline-specific I/O configuration for the Document (ChEMBL) pipeline.

## Pipeline-Specific Output Files

- `document_{date}.csv` — Main dataset with all document records
- `document_{date}_quality_report.csv` — QC metrics and statistics
- `document_{date}_meta.yaml` — Metadata and lineage information

## Pipeline-Specific Sort Keys

Stable sorting by: `['document_chembl_id']`

## Related Documentation

- [PipelineBase I/O and Artifacts](../00-pipeline-base.md#61-io-and-artifacts) — General I/O format, atomic writing, metadata structure
- [13-document-chembl-determinism.md](13-document-chembl-determinism.md) — Determinism policy
- [14-document-chembl-qc.md](14-document-chembl-qc.md) — QC metrics
- [00-document-chembl-overview.md](00-document-chembl-overview.md) — Pipeline overview 