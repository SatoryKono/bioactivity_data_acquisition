# 12 Activity ChEMBL I/O

**Version:** 1.0.0
**Date:** 2025-01-29
**Author:** Data Acquisition Team

## Purpose

This document describes the pipeline-specific I/O configuration for the Activity (ChEMBL) pipeline.

## Pipeline-Specific Output Files

- `activity_{date}.csv` — Main dataset with all activity records
- `activity_{date}_quality_report.csv` — QC metrics and statistics
- `activity_{date}_meta.yaml` — Metadata and lineage information

## Pipeline-Specific Sort Keys

Stable sorting by: `['assay_id', 'testitem_id', 'activity_id']`

## Related Documentation

- [PipelineBase I/O and Artifacts](../00-pipeline-base.md#61-io-and-artifacts) — General I/O format, atomic writing, metadata structure
- [13-activity-chembl-determinism.md](13-activity-chembl-determinism.md) — Determinism policy
- [14-activity-chembl-qc.md](14-activity-chembl-qc.md) — QC metrics
- [00-activity-chembl-overview.md](00-activity-chembl-overview.md) — Pipeline overview
