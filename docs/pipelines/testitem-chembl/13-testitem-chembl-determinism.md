# 13 TestItem ChEMBL Determinism

**Version:** 1.0.0  
**Date:** 2025-01-29  
**Author:** Data Acquisition Team

## Purpose

This document describes the pipeline-specific determinism configuration for the TestItem (ChEMBL) pipeline.

## Pipeline-Specific Sort Keys

- Primary: `molecule_chembl_id`

## Related Documentation

- [PipelineBase Determinism](../00-pipeline-base.md#6-determinism-and-artifacts) — General determinism policy, hash generation, canonicalization
- [12-testitem-chembl-io.md](12-testitem-chembl-io.md) — I/O implementation
- [00-testitem-chembl-overview.md](00-testitem-chembl-overview.md) — Pipeline overview
