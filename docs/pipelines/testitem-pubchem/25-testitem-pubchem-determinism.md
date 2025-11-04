# 25 TestItem PubChem Determinism

**Version:** 1.0.0  
**Date:** 2025-01-29  
**Author:** Data Acquisition Team

## Purpose

This document describes determinism requirements for the TestItem (PubChem) pipeline.

## Sort Keys

`['pubchem_cid']`

## Hash Generation

SHA256-based row and business key hashing per global determinism policy.

## Related Documentation

- [Determinism Policy](../determinism/00-determinism-policy.md)
- [24-testitem-pubchem-io.md](24-testitem-pubchem-io.md)
- [00-testitem-pubchem-overview.md](00-testitem-pubchem-overview.md)
