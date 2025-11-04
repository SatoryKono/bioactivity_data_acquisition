# 32 Target UniProt Logging

**Version:** 1.0.0  
**Date:** 2025-01-29  
**Author:** Data Acquisition Team

## Purpose

This document describes structured logging for the Target (UniProt) pipeline.

## Logging Format

Structured JSON via `structlog` with mandatory fields: `run_id`, `stage`, `actor`, `source`, `timestamp`.

Actor: `target_uniprot`

## Related Documentation

- [Logging Overview](../logging/00-overview.md)
- [00-target-uniprot-overview.md](00-target-uniprot-overview.md)
