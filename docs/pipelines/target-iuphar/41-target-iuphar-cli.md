# 41 Target IUPHAR CLI

**Version:** 1.0.0  
**Date:** 2025-01-29  
**Author:** Data Acquisition Team

## Purpose

This document describes CLI commands for the Target (IUPHAR) pipeline.

## Command

```bash
python -m bioetl.cli.main target --source iuphar
```

## Required Flags

- `--config`: Configuration file path
- `--output-dir`: Output directory

## Examples

```bash
python -m bioetl.cli.main target --source iuphar \
  --config configs/pipelines/iuphar/target.yaml \
  --output-dir data/output/target
```

## Related Documentation

- [42-target-iuphar-config.md](42-target-iuphar-config.md)
- [00-target-iuphar-overview.md](00-target-iuphar-overview.md)
