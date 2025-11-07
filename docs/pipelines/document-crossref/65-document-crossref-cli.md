# 65 Document Crossref CLI

**Version:** 1.0.0
**Date:** 2025-01-29
**Author:** Data Acquisition Team

## Purpose

This document describes the pipeline-specific CLI configuration for the Document (Crossref) pipeline.

## Pipeline-Specific Command Name

```bash
python -m bioetl.cli.main document --source crossref
```

## Examples

```bash
python -m bioetl.cli.main document --source crossref \
  --config configs/pipelines/crossref/document.yaml \
  --output-dir data/output/document
```

## Related Documentation

- [PipelineBase Standard CLI Flags](../00-pipeline-base.md#81-standard-cli-flags) — General CLI flag descriptions and exit codes
- [66-document-crossref-config.md](66-document-crossref-config.md) — Configuration details
- [00-document-crossref-overview.md](00-document-crossref-overview.md) — Pipeline overview
