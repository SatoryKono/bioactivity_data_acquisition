# 61 Document Crossref I/O

**Version:** 1.0.0  
**Date:** 2025-01-29  
**Author:** Data Acquisition Team

## Purpose

This document describes the I/O stage of the Document (Crossref) pipeline.

## Output Files

- `document_{date}.csv` — Main dataset
- `document_{date}_quality_report.csv` — QC report
- `document_{date}_meta.yaml` — Metadata

## Sort Keys

Stable sorting by: `['doi']`

## Related Documentation

- [62-document-crossref-determinism.md](62-document-crossref-determinism.md)
- [00-document-crossref-overview.md](00-document-crossref-overview.md)
