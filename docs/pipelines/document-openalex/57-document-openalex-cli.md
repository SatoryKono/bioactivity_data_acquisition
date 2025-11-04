# 57 Document OpenAlex CLI

**Version:** 1.0.0  
**Date:** 2025-01-29  
**Author:** Data Acquisition Team

## Purpose

This document describes the pipeline-specific CLI configuration for the Document (OpenAlex) pipeline.

## Pipeline-Specific Command Name

```bash
python -m bioetl.cli.main document --source openalex
```

## Examples

```bash
python -m bioetl.cli.main document --source openalex \
  --config configs/pipelines/openalex/document.yaml \
  --output-dir data/output/document
```

## Related Documentation

- [PipelineBase Standard CLI Flags](../00-pipeline-base.md#81-standard-cli-flags) — General CLI flag descriptions and exit codes
- [58-document-openalex-config.md](58-document-openalex-config.md) — Configuration details
- [00-document-openalex-overview.md](00-document-openalex-overview.md) — Pipeline overview
