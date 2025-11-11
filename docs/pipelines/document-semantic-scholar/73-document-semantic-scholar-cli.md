# 73 Document Semantic Scholar CLI

**Version:** 1.0.0
**Date:** 2025-01-29
**Author:** Data Acquisition Team

## Purpose

This document describes the pipeline-specific CLI configuration for the Document (Semantic Scholar) pipeline.

## Pipeline-Specific Command Name

```bash
python -m bioetl.cli.app document --source semantic-scholar
```

## Examples

```bash
python -m bioetl.cli.app document --source semantic-scholar \
  --config configs/pipelines/semantic-scholar/document.yaml \
  --output-dir data/output/document
```

## Related Documentation

- [PipelineBase Standard CLI Flags](../00-pipeline-base.md#81-standard-cli-flags) — General CLI flag descriptions and exit codes
- [74-document-semantic-scholar-config.md](74-document-semantic-scholar-config.md) — Configuration details
- [00-document-semantic-scholar-overview.md](00-document-semantic-scholar-overview.md) — Pipeline overview
