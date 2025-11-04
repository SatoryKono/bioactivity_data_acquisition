# 15 Assay ChEMBL Logging

**Version:** 1.0.0  
**Date:** 2025-01-29  
**Author:** Data Acquisition Team

## Purpose

This document describes structured logging for the Assay (ChEMBL) pipeline.

## Logging Format

Structured JSON via `structlog` with mandatory fields: `run_id`, `stage`, `actor`, `source`, `timestamp`.

Actor: `assay_chembl`

## Related Documentation

- [Logging Overview](../logging/00-overview.md)
- [00-assay-chembl-overview.md](00-assay-chembl-overview.md)
