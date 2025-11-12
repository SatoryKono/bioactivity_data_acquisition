# 13 Assay ChEMBL Determinism

**Version:** 1.0.0 **Date:** 2025-01-29 **Author:** Data Acquisition Team

## Purpose

This document describes the pipeline-specific determinism configuration for the
Assay (ChEMBL) pipeline.

## Pipeline-Specific Sort Keys

- Primary: `assay_chembl_id`
- Secondary: `row_subtype`
- Tertiary: `row_index`

## Related Documentation

- [PipelineBase Determinism](../00-pipeline-base.md#6-determinism-and-artifacts)
  — General determinism policy, hash generation, canonicalization
- [12-assay-chembl-io.md](12-assay-chembl-io.md) — I/O implementation
- [00-assay-chembl-overview.md](00-assay-chembl-overview.md) — Pipeline overview
