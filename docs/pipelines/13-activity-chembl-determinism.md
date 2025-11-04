# 13 Activity ChEMBL Determinism

**Version:** 1.0.0  
**Date:** 2025-01-29  
**Author:** Data Acquisition Team

## Purpose

This document describes the determinism requirements for the Activity (ChEMBL) pipeline, ensuring byte-for-byte reproducible outputs.

## Determinism Requirements

### Stable Sorting

Rows are sorted by a fixed set of keys before writing:

- Primary: `assay_id`
- Secondary: `testitem_id`  
- Tertiary: `activity_id`

This ensures identical input data produces identical output file ordering.

### Hash Generation

The pipeline generates two types of hashes:

1. **Row Hash** (`hash_row`): SHA256 hash of canonicalized row values
2. **Business Key Hash** (`hash_business_key`): SHA256 hash of the business key combination

### Canonicalization

- All string values are normalized (trimmed, case handling)
- Numeric values use fixed precision
- Dates use ISO-8601 UTC format
- Null values are represented consistently

### Timestamps

All timestamps use UTC timezone and ISO-8601 format.

## Related Documentation

- [Determinism Policy](../determinism/00-determinism-policy.md) — Global determinism policy
- [12-activity-chembl-io.md](12-activity-chembl-io.md) — I/O implementation
- [00-activity-chembl-overview.md](00-activity-chembl-overview.md) — Pipeline overview
