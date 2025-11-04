# 45 Document PubMed I/O

**Version:** 1.0.0  
**Date:** 2025-01-29  
**Author:** Data Acquisition Team

## Purpose

This document describes the I/O stage of the Document (PubMed) pipeline.

## Output Files

- `document_{date}.csv` — Main dataset
- `document_{date}_quality_report.csv` — QC report
- `document_{date}_meta.yaml` — Metadata

## Sort Keys

Stable sorting by: `['pmid']`

## Related Documentation

- [46-document-pubmed-determinism.md](46-document-pubmed-determinism.md)
- [00-document-pubmed-overview.md](00-document-pubmed-overview.md)
