# 37 Target IUPHAR I/O

**Version:** 1.0.0  
**Date:** 2025-01-29  
**Author:** Data Acquisition Team

## Purpose

This document describes the I/O stage of the Target (IUPHAR) pipeline.

## Output Files

- `target_{date}.csv` — Main dataset
- `target_{date}_quality_report.csv` — QC report
- `target_{date}_meta.yaml` — Metadata

## Sort Keys

Stable sorting by: `['iuphar_object_id']`

## Related Documentation

- [38-target-iuphar-determinism.md](38-target-iuphar-determinism.md)
- [00-target-iuphar-overview.md](00-target-iuphar-overview.md)
