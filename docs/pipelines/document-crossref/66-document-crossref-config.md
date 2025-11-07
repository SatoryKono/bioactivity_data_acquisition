# 66 Document Crossref Config

**Version:** 1.0.0
**Date:** 2025-01-29
**Author:** Data Acquisition Team

## Purpose

This document describes configuration for the Document (Crossref) pipeline.

## Configuration File

`src/bioetl/configs/pipelines/crossref/document.yaml`

## Key Settings

- `sources.crossref.batch_size`: Batch size for API calls
- `sources.crossref.base_url`: Crossref API base URL
- `determinism.sort.by`: `['doi']`
- `qc.thresholds.document.duplicate_ratio`: 0.0

## Related Documentation

- [Typed Configurations](../configs/00-typed-configs-and-profiles.md)
- [00-document-crossref-overview.md](00-document-crossref-overview.md)
