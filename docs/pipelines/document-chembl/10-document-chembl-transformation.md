# 10 Document ChEMBL Transformation

**Version:** 1.0.0
**Date:** 2025-01-29
**Author:** Data Acquisition Team

## Purpose

This document describes the transformation stage of the Document (ChEMBL) pipeline, covering metadata normalization, identifier mapping, and field standardization.

## Transformation Workflow

1. **Metadata Normalization**: Standardizes document metadata fields
2. **Identifier Mapping**: Ensures consistent format for document identifiers
3. **Field Standardization**: Applies canonical formats for dates, identifiers, and metadata
4. **Type Conversion**: Converts fields to appropriate data types

## Key Transformations

### Document Identifiers

- Normalization of `document_chembl_id` format
- DOI/PMID/PMCID normalization to canonical forms
- Validation of identifier formats

### Metadata Fields

- Date parsing and normalization to ISO-8601 UTC
- Journal name standardization
- Author list normalization

### Data Types

- Conversion of string representations to appropriate numeric types
- Boolean flag handling
- Null value handling per schema policy

## Related Documentation

- [09-document-chembl-extraction.md](09-document-chembl-extraction.md) — Extraction stage
- [11-document-chembl-validation.md](11-document-chembl-validation.md) — Validation stage
- [00-document-chembl-overview.md](00-document-chembl-overview.md) — Pipeline overview
