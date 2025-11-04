# 65 Document Crossref CLI

**Version:** 1.0.0  
**Date:** 2025-01-29  
**Author:** Data Acquisition Team

## Purpose

This document describes CLI commands for the Document (Crossref) pipeline.

## Command

```bash
python -m bioetl.cli.main document --source crossref
```

## Required Flags

- `--config`: Configuration file path
- `--output-dir`: Output directory

## Examples

```bash
python -m bioetl.cli.main document --source crossref \
  --config configs/pipelines/crossref/document.yaml \
  --output-dir data/output/document
```

## Related Documentation

- [66-document-crossref-config.md](66-document-crossref-config.md)
- [00-document-crossref-overview.md](00-document-crossref-overview.md)
