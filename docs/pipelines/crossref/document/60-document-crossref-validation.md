# 60 Document Crossref Validation

> **Status:** pipeline not yet implemented (CLI command missing in `COMMAND_REGISTRY`).

**Version:** 1.0.0 **Date:** 2025-01-29 **Author:** Data Acquisition Team

## Purpose

This document describes the validation stage of the Document (Crossref)
pipeline.

## Pandera Schemas

The pipeline uses `DocumentCrossrefSchema` for validation:

- **Required Fields**: `doi`
- **Identifier Fields**: `crossref_id`, `issn`, `isbn`
- **Metadata Fields**: `title`, `publisher`, `year`, `authors`
- **Optional Fields**: Various document metadata fields

## Validation Workflow

1. **Schema Validation**: Validates against DocumentCrossrefSchema with
   strict=True
1. **Duplicate Detection**: Ensures duplicate-free `doi` values
1. **Identifier Validation**: Validates DOI/ISSN/ISBN formats
1. **Publisher Validation**: Validates publisher references

## Constraints

- `doi`: Unique, required, valid DOI format
- Identifier fields must match controlled vocabulary formats

## Related Documentation

- [61-document-crossref-io.md](61-document-crossref-io.md)
- [63-document-crossref-qc.md](63-document-crossref-qc.md)
- [00-document-crossref-overview.md](00-document-crossref-overview.md)
