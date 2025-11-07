# 14 Activity ChEMBL QC

**Version:** 1.0.0
**Date:** 2025-01-29
**Author:** Data Acquisition Team

## Purpose

This document describes the quality control (QC) metrics and thresholds for the Activity (ChEMBL) pipeline.

## QC Metrics

### Mandatory Metrics

- **Total Records**: Count of activity records processed
- **Duplicate Ratio**: Percentage of duplicate `activity_id` values (must be 0.0)
- **Missing Values**: Coverage statistics for key fields
- **Measurement Type Distribution**: Counts by `standard_type`
- **Unit Distribution**: Counts by `standard_units`
- **Foreign Key Integrity**: Validation of `assay_id` and `molecule_chembl_id` references
- **ChEMBL Validity Flags**: Counts of records with validity issues

### Optional Metrics

- **Correlation Analysis**: Pairwise correlations between measurement types (when enabled)

## QC Thresholds

- `duplicate_ratio`: 0.0 (critical: no duplicates allowed)
- `missing_value_ratio`: Configurable per field
- `foreign_key_integrity`: 1.0 (all references must exist)

## QC Report

The QC report includes:

- Summary statistics
- Distribution plots (when applicable)
- Threshold violations
- Recommendations

## Related Documentation

- [11-activity-chembl-validation.md](11-activity-chembl-validation.md) — Validation stage
- [QC Overview](../qc/00-qc-overview.md) — QC framework
- [00-activity-chembl-overview.md](00-activity-chembl-overview.md) — Pipeline overview
