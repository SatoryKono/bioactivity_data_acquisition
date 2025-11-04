# 53 Document OpenAlex I/O

**Version:** 1.0.0  
**Date:** 2025-01-29  
**Author:** Data Acquisition Team

## Purpose

This document describes the I/O stage of the Document (OpenAlex) pipeline.

## Output Files

- `document_{date}.csv` — Main dataset
- `document_{date}_quality_report.csv` — QC report
- `document_{date}_meta.yaml` — Metadata

## Sort Keys

Stable sorting by: `['openalex_id']`

## Related Documentation

- [54-document-openalex-determinism.md](54-document-openalex-determinism.md)
- [00-document-openalex-overview.md](00-document-openalex-overview.md)
