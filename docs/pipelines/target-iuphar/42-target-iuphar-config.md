# 42 Target IUPHAR Config

**Version:** 1.0.0  
**Date:** 2025-01-29  
**Author:** Data Acquisition Team

## Purpose

This document describes configuration for the Target (IUPHAR) pipeline.

## Configuration File

`src/bioetl/configs/pipelines/iuphar/target.yaml`

## Key Settings

- `sources.iuphar.batch_size`: Batch size for API calls
- `sources.iuphar.base_url`: IUPHAR API base URL
- `determinism.sort.by`: `['iuphar_object_id']`
- `qc.thresholds.target.duplicate_ratio`: 0.0

## Related Documentation

- [Typed Configurations](../configs/00-typed-configs-and-profiles.md)
- [00-target-iuphar-overview.md](00-target-iuphar-overview.md)
