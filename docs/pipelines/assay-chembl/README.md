# Assay ChEMBL Pipeline

The assay_chembl pipeline focuses on assay-level metadata derived from
ChEMBL.

## Purpose

- Capture assay identifiers and basic metadata.
- Describe assay types, formats, and biological context.
- Provide a link between activities, targets, and test items.

## Inputs and sources

- Primary source: ChEMBL assay tables and related REST endpoints.
- Cross-links to activity, target, and test item records.

## Outputs and schemas

The pipeline writes one or more assay datasets that are validated using
Pandera schemas from the bioetl.schemas registry.

Field-level descriptions for the assay table are documented in
../../datatypes/assay.md.

## Notes

CLI examples and configuration options for this pipeline are intended to
follow the unified CLI documentation, so they are not duplicated here.
