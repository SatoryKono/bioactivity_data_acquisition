# 17 Activity ChEMBL Config

**Version:** 1.0.0  
**Date:** 2025-01-29  
**Author:** Data Acquisition Team

## Purpose

This document describes the configuration keys and profiles for the Activity (ChEMBL) pipeline.

## Configuration File

The pipeline configuration is defined in `src/bioetl/configs/pipelines/chembl/activity.yaml`.

## Profile Inheritance

The configuration extends the following profiles:

- `profiles/base.yaml`: Base logging and I/O settings
- `profiles/determinism.yaml`: Determinism and hashing policies

## Key Configuration Keys

### ChEMBL Source

- `sources.chembl.batch_size`: Batch size for API calls (≤ 25, required)
- `sources.chembl.base_url`: ChEMBL API base URL
- `sources.chembl.max_url_length`: Maximum URL length for requests (≤ 2000)

### Determinism

- `determinism.sort.by`: Sort keys `['assay_id', 'testitem_id', 'activity_id']`
- `determinism.hash_policy`: Hash generation policy (SHA256)

### Post-processing

- `postprocess.correlation.enabled`: Enable correlation report generation (default: false)

### QC Thresholds

- `qc.thresholds.activity.duplicate_ratio`: Maximum duplicate ratio (0.0)
- `qc.thresholds.activity.missing_value_ratio`: Configurable per field

## Configuration Override

Configuration can be overridden via:

1. Environment variables (highest priority)
2. `--set` CLI flags
3. Configuration file values (lowest priority)

## Related Documentation

- [Typed Configurations](../configs/00-typed-configs-and-profiles.md) — Configuration system
- [16-activity-chembl-cli.md](16-activity-chembl-cli.md) — CLI usage
- [00-activity-chembl-overview.md](00-activity-chembl-overview.md) — Pipeline overview
