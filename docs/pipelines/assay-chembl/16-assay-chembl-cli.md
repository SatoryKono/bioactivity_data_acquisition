# 16 Assay ChEMBL CLI

**Version:** 1.0.0  
**Date:** 2025-01-29  
**Author:** Data Acquisition Team

## Purpose

This document describes the pipeline-specific CLI configuration for the Assay (ChEMBL) pipeline.

## Pipeline-Specific Command Name

```bash
python -m bioetl.cli.main assay_chembl
```

## Examples

```bash
# Standard production run
python -m bioetl.cli.main assay_chembl \
  --config configs/pipelines/assay/assay_chembl.yaml \
  --output-dir data/output/assay

# Dry run to validate configuration
python -m bioetl.cli.main assay_chembl \
  --config configs/pipelines/assay/assay_chembl.yaml \
  --output-dir data/output/assay \
  --dry-run
```

## Related Documentation

- [PipelineBase Standard CLI Flags](../00-pipeline-base.md#81-standard-cli-flags) — General CLI flag descriptions and exit codes
- [17-assay-chembl-config.md](17-assay-chembl-config.md) — Configuration details
- [00-assay-chembl-overview.md](00-assay-chembl-overview.md) — Pipeline overview
