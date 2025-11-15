# 24 TestItem PubChem I/O

**Version:** 1.0.0 **Date:** 2025-01-29 **Author:** Data Acquisition Team

## Purpose

This document describes the pipeline-specific I/O configuration for the TestItem
(PubChem) pipeline.

## Pipeline-Specific Output Files

- `testitem_{date}.csv` — Main dataset
- `testitem_{date}_quality_report.csv` — QC report
- `testitem_{date}_meta.yaml` — Metadata

## Pipeline-Specific Sort Keys

Stable sorting by: `['pubchem_cid']`

## Related Documentation

- [PipelineBase I/O and Artifacts](../00-pipeline-base.md#61-io-and-artifacts) —
  General I/O format, atomic writing, metadata structure
- [25-testitem-pubchem-determinism.md](25-testitem-pubchem-determinism.md) —
  Determinism policy
- [00-testitem-pubchem-overview.md](00-testitem-pubchem-overview.md) — Pipeline
  overview
