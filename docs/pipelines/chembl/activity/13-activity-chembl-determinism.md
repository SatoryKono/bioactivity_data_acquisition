# 13 Activity ChEMBL Determinism

**Version:** 1.0.0 **Date:** 2025-01-29 **Author:** Data Acquisition Team

## Purpose

This document describes the pipeline-specific determinism configuration for the
Activity (ChEMBL) pipeline.

## Pipeline-Specific Sort Keys

- Primary: `assay_id`
- Secondary: `testitem_id`
- Tertiary: `activity_id`

## Related Documentation

- [PipelineBase Determinism](../00-pipeline-base.md#6-determinism-and-artifacts)
  — General determinism policy, hash generation, canonicalization
- [12-activity-chembl-io.md](12-activity-chembl-io.md) — I/O implementation
- [00-activity-chembl-overview.md](00-activity-chembl-overview.md) — Pipeline
  overview
