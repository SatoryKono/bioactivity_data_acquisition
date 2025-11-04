# 16 Assay ChEMBL CLI

**Version:** 1.0.0  
**Date:** 2025-01-29  
**Author:** Data Acquisition Team

## Purpose

This document describes CLI commands for the Assay (ChEMBL) pipeline.

## Command

```bash
python -m bioetl.cli.main assay
```

## Required Flags

- `--config`: Configuration file path
- `--output-dir`: Output directory

## Examples

```bash
python -m bioetl.cli.main assay \
  --config configs/pipelines/chembl/assay.yaml \
  --output-dir data/output/assay
```

## Related Documentation

- [17-assay-chembl-config.md](17-assay-chembl-config.md)
- [00-assay-chembl-overview.md](00-assay-chembl-overview.md)
