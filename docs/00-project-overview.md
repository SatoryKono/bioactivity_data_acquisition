# Project Overview

BioETL is a data processing framework for acquiring, normalizing, and
validating bioactivity-related datasets from multiple external sources.

## Goals

- Provide deterministic, reproducible ETL pipelines for bioactivity
  data.
- Enforce strict data validation using Pandera schemas before any
  dataset is written.
- Produce high-quality, well-documented datasets with accompanying
  quality control artifacts.

## Core entities

The main business entities handled by BioETL include:

- Activity
- Assay
- Target
- Molecules and molecule forms
- Documents and document-derived entities (terms, similarities)

See the datatypes directory for detailed field-level descriptions:

- [activity](../datatypes/activity.md)
- [assay](../datatypes/assay.md)
- [target](../datatypes/target.md)
- [molecule](../datatypes/molecule.md)
- [document](../datatypes/document.md)

## Data sources

Typical external data sources include:

- ChEMBL REST API and downloadable datasets.
- Scientific literature and metadata providers such as OpenAlex or
  Semantic Scholar (depending on pipeline).

All HTTP access is performed through unified API clients that implement
timeouts, retries, throttling, and caching.

## High-level features

- Unified pipeline architecture with a shared lifecycle and components.
- Deterministic I/O: stable row and column ordering, canonical
  serialization, atomic writes.
- Rich QC artifacts for every dataset, including meta.yaml, quality
  reports, and golden files where applicable.
