# 73 Document Semantic Scholar CLI

**Version:** 1.0.0  
**Date:** 2025-01-29  
**Author:** Data Acquisition Team

## Purpose

This document describes CLI commands for the Document (Semantic Scholar) pipeline.

## Command

```bash
python -m bioetl.cli.main document --source semantic-scholar
```

## Required Flags

- `--config`: Configuration file path
- `--output-dir`: Output directory

## Examples

```bash
python -m bioetl.cli.main document --source semantic-scholar \
  --config configs/pipelines/semantic-scholar/document.yaml \
  --output-dir data/output/document
```

## Related Documentation

- [74-document-semantic-scholar-config.md](74-document-semantic-scholar-config.md)
- [00-document-semantic-scholar-overview.md](00-document-semantic-scholar-overview.md)
