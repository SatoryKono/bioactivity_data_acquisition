# 16 TestItem ChEMBL CLI

**Version:** 1.0.0  
**Date:** 2025-01-29  
**Author:** Data Acquisition Team

## Purpose

This document describes the pipeline-specific CLI configuration for the TestItem (ChEMBL) pipeline.

## Pipeline-Specific Command Name

```bash
python -m bioetl.cli.main testitem
```

## Examples

```bash
python -m bioetl.cli.main testitem \
  --config configs/pipelines/testitem/testitem_chembl.yaml \
  --output-dir data/output/testitem
```

## Related Documentation

- [PipelineBase Standard CLI Flags](../00-pipeline-base.md#81-standard-cli-flags) — General CLI flag descriptions and exit codes
- [17-testitem-chembl-config.md](17-testitem-chembl-config.md) — Configuration details
- [00-testitem-chembl-overview.md](00-testitem-chembl-overview.md) — Pipeline overview
