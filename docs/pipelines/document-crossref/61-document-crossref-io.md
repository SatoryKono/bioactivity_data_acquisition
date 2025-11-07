# 61 Document Crossref I/O

**Version:** 1.0.0
**Date:** 2025-01-29
**Author:** Data Acquisition Team

## Purpose

This document describes the pipeline-specific I/O configuration for the Document (Crossref) pipeline.

## Pipeline-Specific Output Files

- `document_{date}.csv` — Main dataset
- `document_{date}_quality_report.csv` — QC report
- `document_{date}_meta.yaml` — Metadata

## Pipeline-Specific Sort Keys

Stable sorting by: `['doi']`

## Related Documentation

- [PipelineBase I/O and Artifacts](../00-pipeline-base.md#61-io-and-artifacts) — General I/O format, atomic writing, metadata structure
- [62-document-crossref-determinism.md](62-document-crossref-determinism.md) — Determinism policy
- [00-document-crossref-overview.md](00-document-crossref-overview.md) — Pipeline overview
