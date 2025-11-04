# 28 Target UniProt Validation

**Version:** 1.0.0  
**Date:** 2025-01-29  
**Author:** Data Acquisition Team

## Purpose

This document describes the validation stage of the Target (UniProt) pipeline.

## Pandera Schemas

The pipeline uses `TargetUniProtSchema` for validation:

- **Required Fields**: `uniprot_accession`
- **Identifier Fields**: `entry_name`, `protein_name`
- **Metadata Fields**: `organism`, `function`, `subcellular_location`
- **Optional Fields**: Various protein annotation fields

## Validation Workflow

1. **Schema Validation**: Validates against TargetUniProtSchema with strict=True
2. **Duplicate Detection**: Ensures duplicate-free `uniprot_accession` values
3. **Accession Validation**: Validates UniProt accession format
4. **Taxonomy Validation**: Validates organism taxonomy IDs

## Constraints

- `uniprot_accession`: Unique, required, valid UniProt accession format
- Taxonomy IDs must match NCBI taxonomy database

## Related Documentation

- [29-target-uniprot-io.md](29-target-uniprot-io.md)
- [31-target-uniprot-qc.md](31-target-uniprot-qc.md)
- [00-target-uniprot-overview.md](00-target-uniprot-overview.md)
