# 27 Target UniProt Transformation

**Version:** 1.0.0
**Date:** 2025-01-29
**Author:** Data Acquisition Team

## Purpose

This document describes the transformation stage of the Target (UniProt) pipeline, covering protein metadata normalization, identifier mapping, and annotation standardization.

## Transformation Workflow

1. **Metadata Normalization**: Standardizes protein metadata fields
2. **Identifier Mapping**: Ensures consistent format for UniProt accession and related identifiers
3. **Annotation Standardization**: Normalizes functional annotations and classifications
4. **Type Conversion**: Converts fields to appropriate data types

## Key Transformations

### Protein Identifiers

- Normalization of `uniprot_accession` format
- Cross-reference identifier mapping (ChEMBL, PDB, etc.)
- Isoform and variant handling

### Protein Metadata

- Protein name standardization
- Organism taxonomy normalization
- Functional annotation normalization
- Gene ontology mapping

## Related Documentation

- [09-target-uniprot-extraction.md](09-target-uniprot-extraction.md) — Extraction stage
- [28-target-uniprot-validation.md](28-target-uniprot-validation.md) — Validation stage
- [00-target-uniprot-overview.md](00-target-uniprot-overview.md) — Pipeline overview
