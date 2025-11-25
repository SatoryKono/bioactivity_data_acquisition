# Target ChEMBL Pipeline

The target_chembl pipeline normalizes target entities from ChEMBL,
including proteins, complexes, and target families.

## Purpose

- Represent targets in a consistent, normalized form.
- Capture relationships between targets and their components.
- Provide stable identifiers used across activity and assay datasets.

## Inputs and sources

- Primary source: ChEMBL target tables and related components.
- Supplementary information from protein and classification tables.

## Outputs and schemas

The pipeline produces target-centric datasets that are validated using
Pandera schemas from the bioetl.schemas registry.

Field-level descriptions for target-related tables are documented in
../../datatypes/target.md and related datatype files.

## Notes

Integration details with external protein or classification systems
should be documented here once corresponding enrichment steps are
implemented in the codebase.
