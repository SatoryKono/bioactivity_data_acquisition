# 29 Target UniProt I/O

**Version:** 1.0.0 **Date:** 2025-01-29 **Author:** Data Acquisition Team

## Purpose

This document describes the pipeline-specific I/O configuration for the Target
(UniProt) pipeline.

## Pipeline-Specific Output Files

- `target_{date}.csv` — Main dataset
- `target_{date}_quality_report.csv` — QC report
- `target_{date}_meta.yaml` — Metadata

## Pipeline-Specific Sort Keys

Stable sorting by: `['uniprot_accession']`

## Related Documentation

- [PipelineBase I/O and Artifacts](../00-pipeline-base.md#61-io-and-artifacts) —
  General I/O format, atomic writing, metadata structure
- [30-target-uniprot-determinism.md](30-target-uniprot-determinism.md) —
  Determinism policy
- [00-target-uniprot-overview.md](00-target-uniprot-overview.md) — Pipeline
  overview
