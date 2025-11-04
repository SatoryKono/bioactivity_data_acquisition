# 49 Document PubMed CLI

**Version:** 1.0.0  
**Date:** 2025-01-29  
**Author:** Data Acquisition Team

## Purpose

This document describes CLI commands for the Document (PubMed) pipeline.

## Command

```bash
python -m bioetl.cli.main document --source pubmed
```

## Required Flags

- `--config`: Configuration file path
- `--output-dir`: Output directory

## Examples

```bash
python -m bioetl.cli.main document --source pubmed \
  --config configs/pipelines/pubmed/document.yaml \
  --output-dir data/output/document
```

## Related Documentation

- [50-document-pubmed-config.md](50-document-pubmed-config.md)
- [00-document-pubmed-overview.md](00-document-pubmed-overview.md)
