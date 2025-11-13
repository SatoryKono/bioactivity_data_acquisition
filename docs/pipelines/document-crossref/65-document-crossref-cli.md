# 65 Document Crossref CLI

**Version:** 1.0.0 **Date:** 2025-01-29 **Author:** Data Acquisition Team

> **Статус:** not implemented (CLI команда отсутствует в `COMMAND_REGISTRY`).

## Purpose

This document describes the pipeline-specific CLI configuration for the Document
(Crossref) pipeline.

## Pipeline-Specific Command Name

```bash
# not implemented
python -m bioetl.cli.cli_app document --source crossref
```

## Examples

```bash
# not implemented
python -m bioetl.cli.cli_app document --source crossref \
  --config configs/pipelines/crossref/document.yaml \
  --output-dir data/output/document
```

## Related Documentation

- [PipelineBase Standard CLI Flags](../00-pipeline-base.md#81-standard-cli-flags)
  — General CLI flag descriptions and exit codes
- [66-document-crossref-config.md](66-document-crossref-config.md) —
  Configuration details
- [00-document-crossref-overview.md](00-document-crossref-overview.md) —
  Pipeline overview
