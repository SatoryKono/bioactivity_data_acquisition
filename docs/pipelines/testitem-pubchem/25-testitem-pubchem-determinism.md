# 25 TestItem PubChem Determinism

**Version:** 1.0.0  
**Date:** 2025-01-29  
**Author:** Data Acquisition Team

## Purpose

This document describes the pipeline-specific determinism configuration for the TestItem (PubChem) pipeline.

## Pipeline-Specific Sort Keys

- Primary: `pubchem_cid`

## Related Documentation

- [PipelineBase Determinism](../00-pipeline-base.md#6-determinism-and-artifacts) — General determinism policy, hash generation, canonicalization
- [24-testitem-pubchem-io.md](24-testitem-pubchem-io.md) — I/O implementation
- [00-testitem-pubchem-overview.md](00-testitem-pubchem-overview.md) — Pipeline overview
