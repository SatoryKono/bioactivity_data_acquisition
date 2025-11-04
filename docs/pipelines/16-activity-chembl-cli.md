# 16 Activity ChEMBL CLI

**Version:** 1.0.0  
**Date:** 2025-01-29  
**Author:** Data Acquisition Team

## Purpose

This document describes the CLI commands and flags for the Activity (ChEMBL) pipeline.

## Command

```bash
python -m bioetl.cli.main activity
```

## Required Flags

- `--config`: Path to pipeline configuration file (e.g., `configs/pipelines/chembl/activity.yaml`)
- `--output-dir`: Directory for output files

## Optional Flags

- `--input-file`: Path to input CSV file with activity IDs (if applicable)
- `--dry-run`: Validate configuration without executing pipeline
- `--limit`: Limit number of records to process (for testing)
- `--set`: Override configuration values (e.g., `--set sources.chembl.batch_size=10`)

## Exit Codes

- `0`: Success
- `1`: Configuration error
- `2`: Validation error
- `3`: Extraction error
- `4`: Transformation error
- `5`: Write error

## Examples

```bash
# Standard production run
python -m bioetl.cli.main activity \
  --config configs/pipelines/chembl/activity.yaml \
  --output-dir data/output/activity

# Dry run to validate configuration
python -m bioetl.cli.main activity \
  --config configs/pipelines/chembl/activity.yaml \
  --output-dir data/output/activity \
  --dry-run

# Override batch size for testing
python -m bioetl.cli.main activity \
  --config configs/pipelines/chembl/activity.yaml \
  --output-dir data/output/activity \
  --set sources.chembl.batch_size=10 \
  --limit 100
```

## Related Documentation

- [CLI Overview](../cli/00-cli-overview.md) — CLI architecture
- [17-activity-chembl-config.md](17-activity-chembl-config.md) — Configuration details
- [00-activity-chembl-overview.md](00-activity-chembl-overview.md) — Pipeline overview
