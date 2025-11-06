# 17 Document ChEMBL Config

**Version:** 1.0.0  
**Date:** 2025-01-29  
**Author:** Data Acquisition Team

## Purpose

This document describes the configuration keys and profiles for the Document (ChEMBL) pipeline.

## Configuration File

The pipeline configuration is defined in `configs/pipelines/document/document_chembl.yaml`.

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

- `determinism.sort.by`: Sort keys `['document_chembl_id']`
- `determinism.hash_policy`: Hash generation policy (SHA256)

### QC Thresholds

- `qc.thresholds.document.duplicate_ratio`: Maximum duplicate ratio (0.0)
- `qc.thresholds.document.missing_value_ratio`: Configurable per field
- `qc.thresholds.document.identifier_coverage`: Configurable thresholds

## Configuration Override

Configuration can be overridden via:

1. Environment variables (highest priority)
2. `--set` CLI flags
3. Configuration file values (lowest priority)

## Related Documentation

- [Typed Configurations](../configs/00-typed-configs-and-profiles.md) — Configuration system
- [16-document-chembl-cli.md](document-chembl/16-document-chembl-cli.md) — CLI usage
- [00-document-chembl-overview.md](document-chembl/00-document-chembl-overview.md) — Pipeline overview
