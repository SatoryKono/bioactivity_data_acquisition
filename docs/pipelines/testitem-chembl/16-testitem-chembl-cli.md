# 16 TestItem ChEMBL CLI

**Version:** 1.0.0  
**Date:** 2025-01-29  
**Author:** Data Acquisition Team

## Purpose

This document describes CLI commands for the TestItem (ChEMBL) pipeline.

## Command

```bash
python -m bioetl.cli.main testitem
```

## Required Flags

- `--config`: Configuration file path
- `--output-dir`: Output directory

## Examples

```bash
python -m bioetl.cli.main testitem \
  --config configs/pipelines/chembl/testitem.yaml \
  --output-dir data/output/testitem
```

## Related Documentation

- [17-testitem-chembl-config.md](17-testitem-chembl-config.md)
- [00-testitem-chembl-overview.md](00-testitem-chembl-overview.md)
