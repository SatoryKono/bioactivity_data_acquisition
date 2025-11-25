# Activity ChEMBL Pipeline

The activity_chembl pipeline extracts and normalizes bioactivity
measurements from ChEMBL.

## Purpose

- Collect activity records such as IC50, EC50, Ki, and related
  endpoints.
- Normalize units, relations, and activity types.
- Link activities to assays, targets, molecules, and supporting
  documents.

## Inputs and sources

- Primary source: ChEMBL activity tables and associated REST endpoints.
- Supporting metadata from assays, targets, and documents.

All HTTP access is performed via unified API clients with timeouts,
retries, throttling, and caching.

## Outputs and schemas

The pipeline produces one or more activity-focused datasets that are
validated using Pandera schemas in the bioetl.schemas registry.

Field-level descriptions for the main activity table are documented in
../../datatypes/activity.md.

## Notes

Detailed CLI usage and configuration examples for this pipeline should
be documented together with the central CLI and config documentation
once the CLI registry is fully described.
