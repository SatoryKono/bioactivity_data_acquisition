# 36 Target IUPHAR Validation

**Version:** 1.0.0
**Date:** 2025-01-29
**Author:** Data Acquisition Team

## Purpose

This document describes the validation stage of the Target (IUPHAR) pipeline.

## Pandera Schemas

The pipeline uses `TargetIUPHARSchema` for validation:

- **Required Fields**: `iuphar_object_id`
- **Identifier Fields**: `uniprot_accession`, `receptor_name`
- **Metadata Fields**: `family`, `subfamily`, `organism`
- **Optional Fields**: Various receptor metadata fields

## Validation Workflow

1. **Schema Validation**: Validates against TargetIUPHARSchema with strict=True
2. **Duplicate Detection**: Ensures duplicate-free `iuphar_object_id` values
3. **ID Validation**: Validates IUPHAR object ID format
4. **UniProt Validation**: Validates UniProt accession references

## Constraints

- `iuphar_object_id`: Unique, required, valid IUPHAR ID format
- UniProt accessions must be valid if present

## Related Documentation

- [37-target-iuphar-io.md](37-target-iuphar-io.md)
- [39-target-iuphar-qc.md](39-target-iuphar-qc.md)
- [00-target-iuphar-overview.md](00-target-iuphar-overview.md)
