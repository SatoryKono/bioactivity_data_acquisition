# 33 Target UniProt CLI

**Version:** 1.0.0 **Date:** 2025-01-29 **Author:** Data Acquisition Team

## Purpose

This document describes the pipeline-specific CLI configuration for the Target
(UniProt) pipeline.

## Pipeline-Specific Command Name

```bash
# (not implemented)
python -m bioetl.cli.cli_app target --source uniprot
```

## Examples

```bash
# (not implemented)
python -m bioetl.cli.cli_app target --source uniprot \
  --config configs/pipelines/uniprot/target.yaml \
  --output-dir data/output/target
```

## Related Documentation

- [PipelineBase Standard CLI Flags](../00-pipeline-base.md#81-standard-cli-flags)
  — General CLI flag descriptions and exit codes
- [34-target-uniprot-config.md](34-target-uniprot-config.md) — Configuration
  details
- [00-target-uniprot-overview.md](00-target-uniprot-overview.md) — Pipeline
  overview
