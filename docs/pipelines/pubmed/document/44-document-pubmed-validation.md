# 44 Document PubMed Validation

> **Status:** pipeline not yet implemented (CLI command missing in `COMMAND_REGISTRY`).

**Version:** 1.0.0 **Date:** 2025-01-29 **Author:** Data Acquisition Team

## Purpose

This document describes the validation stage of the Document (PubMed) pipeline.

## Pandera Schemas

The pipeline uses `DocumentPubMedSchema` for validation:

- **Required Fields**: `pmid`
- **Identifier Fields**: `doi`, `pmc_id`
- **Metadata Fields**: `title`, `journal`, `year`, `authors`
- **Optional Fields**: Various document metadata fields

## Validation Workflow

1. **Schema Validation**: Validates against DocumentPubMedSchema with
   strict=True
1. **Duplicate Detection**: Ensures duplicate-free `pmid` values
1. **Identifier Validation**: Validates PMID/DOI/PMCID formats
1. **Date Validation**: Validates publication date ranges

## Constraints

- `pmid`: Unique, required, positive integer
- Identifier fields must match controlled vocabulary formats

## Related Documentation

- [45-document-pubmed-io.md](45-document-pubmed-io.md)
- [47-document-pubmed-qc.md](47-document-pubmed-qc.md)
- [00-document-pubmed-overview.md](00-document-pubmed-overview.md)
