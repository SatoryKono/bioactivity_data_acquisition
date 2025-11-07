# 34 Target UniProt Config

**Version:** 1.0.0
**Date:** 2025-01-29
**Author:** Data Acquisition Team

## Purpose

This document describes configuration for the Target (UniProt) pipeline.

## Configuration File

`src/bioetl/configs/pipelines/uniprot/target.yaml`

## Key Settings

- `sources.uniprot.batch_size`: Batch size for API calls
- `sources.uniprot.base_url`: UniProt API base URL
- `determinism.sort.by`: `['uniprot_accession']`
- `qc.thresholds.target.duplicate_ratio`: 0.0

## Related Documentation

- [Typed Configurations](../configs/00-typed-configs-and-profiles.md)
- [00-target-uniprot-overview.md](00-target-uniprot-overview.md)
