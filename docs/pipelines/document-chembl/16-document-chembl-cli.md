# 16 Document ChEMBL CLI

**Version:** 1.0.0  
**Date:** 2025-01-29  
**Author:** Data Acquisition Team

## Purpose

This document describes the pipeline-specific CLI configuration for the Document (ChEMBL) pipeline.

## Pipeline-Specific Command Name

```bash
python -m bioetl.cli.main document
```

## Examples

```bash
# Standard production run
python -m bioetl.cli.main document \
  --config configs/pipelines/chembl/document.yaml \
  --output-dir data/output/document

# Dry run to validate configuration
python -m bioetl.cli.main document \
  --config configs/pipelines/chembl/document.yaml \
  --output-dir data/output/document \
  --dry-run

# Override batch size for testing
python -m bioetl.cli.main document \
  --config configs/pipelines/chembl/document.yaml \
  --output-dir data/output/document \
  --set sources.chembl.batch_size=10 \
  --limit 100
```

## Related Documentation

- [PipelineBase Standard CLI Flags](../00-pipeline-base.md#81-standard-cli-flags) — General CLI flag descriptions and exit codes
- [17-document-chembl-config.md](17-document-chembl-config.md) — Configuration details
- [00-document-chembl-overview.md](00-document-chembl-overview.md) — Pipeline overview
