# 33 Target UniProt CLI

**Version:** 1.0.0  
**Date:** 2025-01-29  
**Author:** Data Acquisition Team

## Purpose

This document describes CLI commands for the Target (UniProt) pipeline.

## Command

```bash
python -m bioetl.cli.main target --source uniprot
```

## Required Flags

- `--config`: Configuration file path
- `--output-dir`: Output directory

## Examples

```bash
python -m bioetl.cli.main target --source uniprot \
  --config configs/pipelines/uniprot/target.yaml \
  --output-dir data/output/target
```

## Related Documentation

- [34-target-uniprot-config.md](34-target-uniprot-config.md)
- [00-target-uniprot-overview.md](00-target-uniprot-overview.md)
