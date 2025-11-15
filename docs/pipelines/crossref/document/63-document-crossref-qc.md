# 63 Document Crossref QC

> **Status:** pipeline not yet implemented (CLI command missing in `COMMAND_REGISTRY`).

**Version:** 1.0.0 **Date:** 2025-01-29 **Author:** Data Acquisition Team

## Purpose

This document describes QC metrics and thresholds for the Document (Crossref)
pipeline.

## QC Metrics

- Duplicate detection
- DOI format validation
- Publisher coverage
- Funding information coverage

## QC Thresholds

- `duplicate_ratio`: 0.0 (critical)
- `doi_validity`: 1.0 (all must be valid)

## Related Documentation

- [60-document-crossref-validation.md](60-document-crossref-validation.md)
- [00-document-crossref-overview.md](00-document-crossref-overview.md)
