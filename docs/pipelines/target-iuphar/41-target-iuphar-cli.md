# 41 Target IUPHAR CLI

**Version:** 1.0.0
**Date:** 2025-01-29
**Author:** Data Acquisition Team

## Purpose

This document describes the pipeline-specific CLI configuration for the Target (IUPHAR) pipeline.

## Pipeline-Specific Command Name

```bash
# (not implemented)
python -m bioetl.cli.app target --source iuphar
```

## Examples

```bash
# (not implemented)
python -m bioetl.cli.app target --source iuphar \
  --config configs/pipelines/iuphar/target.yaml \
  --output-dir data/output/target
```

## Related Documentation

- [PipelineBase Standard CLI Flags](../00-pipeline-base.md#81-standard-cli-flags) — General CLI flag descriptions and exit codes
- [42-target-iuphar-config.md](42-target-iuphar-config.md) — Configuration details
- [00-target-iuphar-overview.md](00-target-iuphar-overview.md) — Pipeline overview
