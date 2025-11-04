# 57 Document OpenAlex CLI

**Version:** 1.0.0  
**Date:** 2025-01-29  
**Author:** Data Acquisition Team

## Purpose

This document describes CLI commands for the Document (OpenAlex) pipeline.

## Command

```bash
python -m bioetl.cli.main document --source openalex
```

## Required Flags

- `--config`: Configuration file path
- `--output-dir`: Output directory

## Examples

```bash
python -m bioetl.cli.main document --source openalex \
  --config configs/pipelines/openalex/document.yaml \
  --output-dir data/output/document
```

## Related Documentation

- [58-document-openalex-config.md](58-document-openalex-config.md)
- [00-document-openalex-overview.md](00-document-openalex-overview.md)
