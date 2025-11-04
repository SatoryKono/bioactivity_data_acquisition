# 30 Target UniProt Determinism

**Version:** 1.0.0  
**Date:** 2025-01-29  
**Author:** Data Acquisition Team

## Purpose

This document describes determinism requirements for the Target (UniProt) pipeline.

## Sort Keys

`['uniprot_accession']`

## Hash Generation

SHA256-based row and business key hashing per global determinism policy.

## Related Documentation

- [Determinism Policy](../determinism/00-determinism-policy.md)
- [29-target-uniprot-io.md](29-target-uniprot-io.md)
- [00-target-uniprot-overview.md](00-target-uniprot-overview.md)
