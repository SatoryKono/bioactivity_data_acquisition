# 17 Target ChEMBL Config

**Version:** 1.0.0  
**Date:** 2025-01-29  
**Author:** Data Acquisition Team

## Purpose

This document describes configuration for the Target (ChEMBL) pipeline.

## Configuration File

`src/bioetl/configs/pipelines/chembl/target.yaml`

## Key Settings

- `sources.chembl.batch_size`: ≤ 25 (required)
- `sources.chembl.max_url_length`: ≤ 2000
- `determinism.sort.by`: `['target_chembl_id']`
- `qc.thresholds.target.duplicate_ratio`: 0.0

## Related Documentation

- [Typed Configurations](../configs/00-typed-configs-and-profiles.md)
- [00-target-chembl-overview.md](00-target-chembl-overview.md)
