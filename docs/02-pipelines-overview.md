# Pipelines Overview

This document provides a high-level overview of the main BioETL
pipelines and the datasets they produce.

## Summary table

- activity_chembl
  - Entity: Activity
  - Source: ChEMBL
  - Purpose: extract and normalize bioactivity measurements such as
    IC50, EC50, Ki, and related endpoints.

- assay_chembl
  - Entity: Assay
  - Source: ChEMBL
  - Purpose: capture assay-level metadata, including assay type, target
    information, and conditions.

- target_chembl
  - Entity: Target
  - Source: ChEMBL
  - Purpose: normalize target definitions and related components such as
    proteins and complexes.

- document_chembl
  - Entity: Document
  - Source: ChEMBL and literature metadata
  - Purpose: represent publications and other documents that support
    activity and assay records.

- testitem_chembl
  - Entity: Test item
  - Source: ChEMBL
  - Purpose: describe tested substances, linking molecules, batches, and
    experimental context.

## Relation to data schemas

Each pipeline writes one or more datasets that are validated using
Pandera schemas registered in the bioetl.schemas package.

Field-level details for the resulting datasets can be found in the
markdown files under the datatypes directory (for example
../datatypes/activity.md or ../datatypes/assay.md), and in the dedicated
schema documentation that will live under docs/schemas.
