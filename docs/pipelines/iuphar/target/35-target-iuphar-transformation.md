# 35 Target IUPHAR Transformation

**Version:** 1.0.0 **Date:** 2025-01-29 **Author:** Data Acquisition Team

## Purpose

This document describes the transformation stage of the Target (IUPHAR)
pipeline, covering receptor metadata normalization, identifier mapping, and
classification standardization.

## Transformation Workflow

1. **Metadata Normalization**: Standardizes receptor metadata fields
1. **Identifier Mapping**: Ensures consistent format for IUPHAR object IDs and
   related identifiers
1. **Classification Standardization**: Normalizes receptor classifications and
   families
1. **Type Conversion**: Converts fields to appropriate data types

## Key Transformations

### Receptor Identifiers

- Normalization of IUPHAR object ID format
- UniProt accession mapping
- Cross-reference identifier handling

### Receptor Metadata

- Receptor name standardization
- Family and subfamily classification
- Ligand interaction normalization

## Related Documentation

- [09-target-iuphar-extraction.md](09-target-iuphar-extraction.md) — Extraction
  stage
- [36-target-iuphar-validation.md](36-target-iuphar-validation.md) — Validation
  stage
- [00-target-iuphar-overview.md](00-target-iuphar-overview.md) — Pipeline
  overview
