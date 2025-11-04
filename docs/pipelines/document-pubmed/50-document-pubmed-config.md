# 50 Document PubMed Config

**Version:** 1.0.0  
**Date:** 2025-01-29  
**Author:** Data Acquisition Team

## Purpose

This document describes configuration for the Document (PubMed) pipeline.

## Configuration File

`src/bioetl/configs/pipelines/pubmed/document.yaml`

## Key Settings

- `sources.pubmed.batch_size`: Batch size for API calls
- `sources.pubmed.base_url`: PubMed API base URL
- `determinism.sort.by`: `['pmid']`
- `qc.thresholds.document.duplicate_ratio`: 0.0

## Related Documentation

- [Typed Configurations](../configs/00-typed-configs-and-profiles.md)
- [00-document-pubmed-overview.md](00-document-pubmed-overview.md)
