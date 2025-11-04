# 12 Activity ChEMBL I/O

**Version:** 1.0.0  
**Date:** 2025-01-29  
**Author:** Data Acquisition Team

## Purpose

This document describes the I/O stage of the Activity (ChEMBL) pipeline, covering output formats, atomic writing, and file structure.

## Output Files

The pipeline generates the following output files:

- `activity_{date}.csv` — Main dataset with all activity records
- `activity_{date}_quality_report.csv` — QC metrics and statistics
- `activity_{date}_meta.yaml` — Metadata and lineage information

## File Formats

### CSV Format

- UTF-8 encoding
- Comma-separated values
- Header row with column names matching Pandera schema order
- Stable row ordering by sort keys: `[ 'activity_id']`

### Metadata Format

- YAML format with deterministic key ordering
- Includes: pipeline_version, git_commit, config_hash, row_count, checksums, generated_at_utc

## Atomic Writing

All file writes use atomic operations:

1. Write to temporary file
2. Flush and sync to disk
3. Atomic rename to final filename

## Related Documentation

- [13-activity-chembl-determinism.md](13-activity-chembl-determinism.md) — Determinism policy
- [14-activity-chembl-qc.md](14-activity-chembl-qc.md) — QC metrics
- [00-activity-chembl-overview.md](00-activity-chembl-overview.md) — Pipeline overview
