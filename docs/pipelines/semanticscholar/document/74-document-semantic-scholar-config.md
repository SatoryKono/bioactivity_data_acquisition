# 74 Document Semantic Scholar Config

> **Status:** pipeline not yet implemented (CLI command missing in `COMMAND_REGISTRY`).

**Version:** 1.0.0 **Date:** 2025-01-29 **Author:** Data Acquisition Team

## Purpose

This document describes configuration for the Document (Semantic Scholar)
pipeline.

## Configuration File

`src/bioetl/configs/pipelines/semantic-scholar/document.yaml`

## Key Settings

- `sources.semantic_scholar.batch_size`: Batch size for API calls
- `sources.semantic_scholar.base_url`: Semantic Scholar API base URL
- `determinism.sort.by`: `['semantic_scholar_id']`
- `qc.thresholds.document.duplicate_ratio`: 0.0

## Related Documentation

- [Typed Configurations](../configs/00-typed-configs-and-profiles.md)
- [00-document-semantic-scholar-overview.md](00-document-semantic-scholar-overview.md)
