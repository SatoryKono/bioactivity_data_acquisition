# 16 Activity ChEMBL CLI

**Version:** 1.0.0  
**Date:** 2025-01-29  
**Author:** Data Acquisition Team

## Purpose

This document describes the pipeline-specific CLI configuration for the Activity (ChEMBL) pipeline.

## Pipeline-Specific Command Name

```bash
python -m bioetl.cli.main activity
```

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

- [PipelineBase Standard CLI Flags](../00-pipeline-base.md#81-standard-cli-flags) — General CLI flag descriptions and exit codes
- [17-activity-chembl-config.md](17-activity-chembl-config.md) — Configuration details
- [00-activity-chembl-overview.md](00-activity-chembl-overview.md) — Pipeline overview
