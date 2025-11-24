# 29 TestItem PubChem Config

**Version:** 1.0.0 **Date:** 2025-01-29 **Author:** Data Acquisition Team

## Purpose

This document describes configuration for the TestItem (PubChem) pipeline.

## Configuration File

`src/bioetl/configs/pipelines/pubchem/testitem.yaml`

## Key Settings

- `sources.pubchem.batch_size`: Batch size for API calls
- `sources.pubchem.base_url`: PubChem API base URL
- `determinism.sort.by`: `['pubchem_cid']`
- `qc.thresholds.testitem.duplicate_ratio`: 0.0

## Related Documentation

- [Typed Configurations](../configs/00-typed-configs-and-profiles.md)
- [00-testitem-pubchem-overview.md](00-testitem-pubchem-overview.md)
