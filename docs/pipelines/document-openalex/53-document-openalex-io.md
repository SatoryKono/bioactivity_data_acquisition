# 53 Document OpenAlex I/O

**Version:** 1.0.0 **Date:** 2025-01-29 **Author:** Data Acquisition Team

## Purpose

This document describes the pipeline-specific I/O configuration for the Document
(OpenAlex) pipeline.

## Pipeline-Specific Output Files

- `document_{date}.csv` — Main dataset
- `document_{date}_quality_report.csv` — QC report
- `document_{date}_meta.yaml` — Metadata

## Pipeline-Specific Sort Keys

Stable sorting by: `['openalex_id']`

## Related Documentation

- [PipelineBase I/O and Artifacts](../00-pipeline-base.md#61-io-and-artifacts) —
  General I/O format, atomic writing, metadata structure
- [54-document-openalex-determinism.md](54-document-openalex-determinism.md) —
  Determinism policy
- [00-document-openalex-overview.md](00-document-openalex-overview.md) —
  Pipeline overview
