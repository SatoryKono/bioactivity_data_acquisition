# 10 Assay ChEMBL Transformation

**Version:** 1.0.0  
**Date:** 2025-01-29  
**Author:** Data Acquisition Team

## Purpose

This document describes the transformation stage of the Assay (ChEMBL) pipeline, covering nested structure expansion, enrichment operations, and data normalization.

## Transformation Workflow

1. **Nested Structure Expansion**: Converts nested JSON structures to long format
2. **Enrichment**: Adds target and assay class information via lookups
3. **Normalization**: Applies strict NA policy and field standardization
4. **Row Subtype Assignment**: Creates `row_subtype` and `row_index` for expanded records

## Key Transformations

### Structure Expansion

- Expansion of nested assay properties to separate rows
- Creation of `row_subtype` and `row_index` for tracking
- Preservation of `assay_chembl_id` as primary key

### Enrichment

- Target lookup and enrichment
- Assay class mapping via BAO ontology
- Fallback handling for missing enrichments

## Related Documentation

- [09-assay-chembl-extraction.md](09-assay-chembl-extraction.md) — Extraction stage
- [11-assay-chembl-validation.md](11-assay-chembl-validation.md) — Validation stage
- [00-assay-chembl-overview.md](00-assay-chembl-overview.md) — Pipeline overview
