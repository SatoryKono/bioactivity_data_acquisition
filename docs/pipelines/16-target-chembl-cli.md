# 16 Target ChEMBL CLI

**Version:** 1.0.0  
**Date:** 2025-01-29  
**Author:** Data Acquisition Team

## Purpose

This document describes CLI commands for the Target (ChEMBL) pipeline.

## Command

```bash
python -m bioetl.cli.main target
```

## Required Flags

- `--config`: Configuration file path
- `--output-dir`: Output directory

## Examples

```bash
python -m bioetl.cli.main target \
  --config configs/pipelines/chembl/target.yaml \
  --output-dir data/output/target
```

## Related Documentation

- [17-target-chembl-config.md](17-target-chembl-config.md)
- [00-target-chembl-overview.md](00-target-chembl-overview.md)
