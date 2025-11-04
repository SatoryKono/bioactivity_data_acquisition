# 13 TestItem ChEMBL Determinism

**Version:** 1.0.0  
**Date:** 2025-01-29  
**Author:** Data Acquisition Team

## Purpose

This document describes determinism requirements for the TestItem (ChEMBL) pipeline.

## Sort Keys

`['molecule_chembl_id']`

## Hash Generation

SHA256-based row and business key hashing per global determinism policy.

## Related Documentation

- [Determinism Policy](../determinism/00-determinism-policy.md)
- [12-testitem-chembl-io.md](12-testitem-chembl-io.md)
- [00-testitem-chembl-overview.md](00-testitem-chembl-overview.md)
