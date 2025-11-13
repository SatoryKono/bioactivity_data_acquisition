# 58 Document OpenAlex Config

**Version:** 1.0.0 **Date:** 2025-01-29 **Author:** Data Acquisition Team

## Purpose

This document describes configuration for the Document (OpenAlex) pipeline.

## Configuration File

`src/bioetl/configs/pipelines/openalex/document.yaml`

## Key Settings

- `sources.openalex.batch_size`: Batch size for API calls
- `sources.openalex.base_url`: OpenAlex API base URL
- `determinism.sort.by`: `['openalex_id']`
- `qc.thresholds.document.duplicate_ratio`: 0.0

## Related Documentation

- [Typed Configurations](../configs/00-typed-configs-and-profiles.md)
- [00-document-openalex-overview.md](00-document-openalex-overview.md)
