# 11 Document ChEMBL Validation

**Version:** 1.0.0
**Date:** 2025-01-29
**Author:** Data Acquisition Team

## Purpose

This document describes the validation stage of the Document (ChEMBL) pipeline, covering Pandera schema definitions, field constraints, and validation workflows.

## Pandera Schemas

The pipeline uses `DocumentSchema` for validation:

- **Required Fields**: `document_chembl_id`
- **Identifier Fields**: `doi`, `pubmed_id`, `pmc_id` (normalized)
- **Metadata Fields**: `title`, `journal`, `year`, `authors`
- **Optional Fields**: Various document metadata fields

## Validation Workflow

1. **Schema Validation**: Validates against DocumentSchema with strict=True
2. **Duplicate Detection**: Ensures duplicate-free `document_chembl_id` values
3. **Identifier Validation**: Validates DOI/PMID/PMCID formats
4. **Referential Integrity**: Checks document references in related tables

## Constraints

- `document_chembl_id`: Unique, required, ChEMBL ID format
- Identifier fields must match controlled vocabulary formats
- Date fields must be valid ISO-8601 dates

## Related Documentation

- [12-document-chembl-io.md](12-document-chembl-io.md) — Output format
- [14-document-chembl-qc.md](14-document-chembl-qc.md) — QC metrics
- [00-document-chembl-overview.md](00-document-chembl-overview.md) — Pipeline overview
