# 52 Document OpenAlex Validation

> **Status:** pipeline not yet implemented (CLI command missing in `COMMAND_REGISTRY`).

**Version:** 1.0.0 **Date:** 2025-01-29 **Author:** Data Acquisition Team

## Purpose

This document describes the validation stage of the Document (OpenAlex)
pipeline.

## Pandera Schemas

The pipeline uses `DocumentOpenAlexSchema` for validation:

- **Required Fields**: `openalex_id`
- **Identifier Fields**: `doi`, `pmid`, `mag_id`
- **Metadata Fields**: `title`, `venue`, `year`, `authors`
- **Optional Fields**: Various document metadata fields

## Validation Workflow

1. **Schema Validation**: Validates against DocumentOpenAlexSchema with
   strict=True
1. **Duplicate Detection**: Ensures duplicate-free `openalex_id` values
1. **Identifier Validation**: Validates OpenAlex ID/DOI/PMID formats
1. **Concept Validation**: Validates concept ID references

## Constraints

- `openalex_id`: Unique, required, valid OpenAlex ID format
- Identifier fields must match controlled vocabulary formats

## Related Documentation

- [53-document-openalex-io.md](53-document-openalex-io.md)
- [55-document-openalex-qc.md](55-document-openalex-qc.md)
- [00-document-openalex-overview.md](00-document-openalex-overview.md)
