# 17 Assay ChEMBL Config

**Version:** 1.0.0  
**Date:** 2025-01-29  
**Author:** Data Acquisition Team

## Purpose

This document describes configuration for the Assay (ChEMBL) pipeline.

## Configuration File

`configs/pipelines/chembl/assay_chembl.yaml`

## Key Settings

- `sources.chembl.batch_size`: ≤ 25 (required)
- `sources.chembl.max_url_length`: ≤ 2000
- `cache.namespace`: Release-scoped cache
- `determinism.sort.by`: `['assay_chembl_id', 'row_subtype', 'row_index']`
- `qc.thresholds.assay.fallback_usage_rate`: Configurable

## Related Documentation

- [Typed Configurations](../configs/00-typed-configs-and-profiles.md)
- [00-assay-chembl-overview.md](00-assay-chembl-overview.md)
