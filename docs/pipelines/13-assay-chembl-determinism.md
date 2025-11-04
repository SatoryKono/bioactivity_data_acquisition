# 13 Assay ChEMBL Determinism

**Version:** 1.0.0  
**Date:** 2025-01-29  
**Author:** Data Acquisition Team

## Purpose

This document describes determinism requirements for the Assay (ChEMBL) pipeline.

## Sort Keys

`['assay_chembl_id', 'row_subtype', 'row_index']`

## Hash Generation

SHA256-based row and business key hashing per global determinism policy.

## Related Documentation

- [Determinism Policy](../determinism/00-determinism-policy.md)
- [12-assay-chembl-io.md](12-assay-chembl-io.md)
- [00-assay-chembl-overview.md](00-assay-chembl-overview.md)
