# 28 TestItem PubChem CLI

**Version:** 1.0.0  
**Date:** 2025-01-29  
**Author:** Data Acquisition Team

## Purpose

This document describes CLI commands for the TestItem (PubChem) pipeline.

## Command

```bash
python -m bioetl.cli.main testitem --source pubchem
```

## Required Flags

- `--config`: Configuration file path
- `--output-dir`: Output directory

## Examples

```bash
python -m bioetl.cli.main testitem --source pubchem \
  --config configs/pipelines/pubchem/testitem.yaml \
  --output-dir data/output/testitem
```

## Related Documentation

- [29-testitem-pubchem-config.md](29-testitem-pubchem-config.md)
- [00-testitem-pubchem-overview.md](00-testitem-pubchem-overview.md)
