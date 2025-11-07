# 49 Document PubMed CLI

**Version:** 1.0.0
**Date:** 2025-01-29
**Author:** Data Acquisition Team

## Purpose

This document describes the pipeline-specific CLI configuration for the Document (PubMed) pipeline.

## Pipeline-Specific Command Name

```bash
python -m bioetl.cli.main document --source pubmed
```

## Examples

```bash
python -m bioetl.cli.main document --source pubmed \
  --config configs/pipelines/pubmed/document.yaml \
  --output-dir data/output/document
```

## Related Documentation

- [PipelineBase Standard CLI Flags](../00-pipeline-base.md#81-standard-cli-flags) — General CLI flag descriptions and exit codes
- [50-document-pubmed-config.md](50-document-pubmed-config.md) — Configuration details
- [00-document-pubmed-overview.md](00-document-pubmed-overview.md) — Pipeline overview
