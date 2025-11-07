# 17 TestItem ChEMBL Config

**Version:** 1.0.0
**Date:** 2025-01-29
**Author:** Data Acquisition Team

## Purpose

This document describes configuration for the TestItem (ChEMBL) pipeline.

## Configuration File

`configs/pipelines/testitem/testitem_chembl.yaml`

## Key Settings

- `sources.chembl.batch_size`: ≤ 25 (required)
- `sources.chembl.max_url_length`: ≤ 2000
- `determinism.sort.by`: `['molecule_chembl_id']`
- `qc.thresholds.testitem.duplicate_ratio`: 0.0

## Related Documentation

- [Typed Configurations](../configs/00-typed-configs-and-profiles.md)
- [00-testitem-chembl-overview.md](00-testitem-chembl-overview.md)
