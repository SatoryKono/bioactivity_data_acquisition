# 14 Document ChEMBL QC

**Version:** 1.0.0  
**Date:** 2025-01-29  
**Author:** Data Acquisition Team

## Purpose

This document describes the quality control (QC) metrics and thresholds for the Document (ChEMBL) pipeline.

## QC Metrics

### Mandatory Metrics

- **Total Records**: Count of document records processed
- **Duplicate Ratio**: Percentage of duplicate `document_chembl_id` values (must be 0.0)
- **Missing Values**: Coverage statistics for key fields
- **Identifier Coverage**: Statistics for DOI/PMID/PMCID presence
- **Date Validation**: Count of valid vs invalid date fields
- **Referential Integrity**: Validation of document references in related tables

## QC Thresholds

- `duplicate_ratio`: 0.0 (critical: no duplicates allowed)
- `missing_value_ratio`: Configurable per field
- `identifier_coverage`: Configurable thresholds for identifier fields

## QC Report

The QC report includes:
- Summary statistics
- Distribution of identifiers
- Date range analysis
- Threshold violations
- Recommendations

## Related Documentation

- [11-document-chembl-validation.md](11-document-chembl-validation.md) — Validation stage
- [QC Overview](../qc/00-qc-overview.md) — QC framework
- [00-document-chembl-overview.md](00-document-chembl-overview.md) — Pipeline overview
