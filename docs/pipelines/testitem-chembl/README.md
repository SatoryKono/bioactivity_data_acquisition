# Test Item ChEMBL Pipeline

The testitem_chembl pipeline describes tested items in ChEMBL, such as
compounds or batches used in experiments.

## Purpose

- Represent test items that participate in assays and activities.
- Link test items to underlying molecules or molecule forms.
- Provide additional experimental context where available.

## Inputs and sources

- Primary source: ChEMBL test item or related tables.
- Cross-links to molecule, assay, and activity records.

## Outputs and schemas

The pipeline produces test item datasets that are validated using
Pandera schemas from the bioetl.schemas registry.

Field-level descriptions for related entities can be found in
../../datatypes/molecule.md and other datatype files under
../../datatypes/.

## Notes

This documentation focuses on the domain model. CLI commands and config
options for running this pipeline should be documented centrally in the
CLI and configuration documentation.
