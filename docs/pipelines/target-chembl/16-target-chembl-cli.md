# 16 Target ChEMBL CLI

**Version:** 1.0.0 **Date:** 2025-01-29 **Author:** Data Acquisition Team

## Purpose

This document describes the pipeline-specific CLI configuration for the Target
(ChEMBL) pipeline.

## Pipeline-Specific Command Name

```bash
python -m bioetl.cli.cli_app target_chembl
```

## Examples

```bash
python -m bioetl.cli.cli_app target_chembl \
  --config configs/pipelines/target/target_chembl.yaml \
  --output-dir data/output/target
```

## Related Documentation

- [PipelineBase Standard CLI Flags](../00-pipeline-base.md#81-standard-cli-flags)
  — General CLI flag descriptions and exit codes
- [17-target-chembl-config.md](17-target-chembl-config.md) — Configuration
  details
- [00-target-chembl-overview.md](00-target-chembl-overview.md) — Pipeline
  overview
