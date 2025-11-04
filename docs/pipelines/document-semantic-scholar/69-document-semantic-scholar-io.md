# 69 Document Semantic Scholar I/O

**Version:** 1.0.0  
**Date:** 2025-01-29  
**Author:** Data Acquisition Team

## Purpose

This document describes the I/O stage of the Document (Semantic Scholar) pipeline.

## Output Files

- `document_{date}.csv` — Main dataset
- `document_{date}_quality_report.csv` — QC report
- `document_{date}_meta.yaml` — Metadata

## Sort Keys

Stable sorting by: `['semantic_scholar_id']`

## Related Documentation

- [70-document-semantic-scholar-determinism.md](70-document-semantic-scholar-determinism.md)
- [00-document-semantic-scholar-overview.md](00-document-semantic-scholar-overview.md)
