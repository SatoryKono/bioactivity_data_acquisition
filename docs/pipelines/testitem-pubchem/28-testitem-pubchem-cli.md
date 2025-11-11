# 28 TestItem PubChem CLI

**Version:** 1.0.0
**Date:** 2025-01-29
**Author:** Data Acquisition Team

## Purpose

This document describes the pipeline-specific CLI configuration for the TestItem (PubChem) pipeline.

## Pipeline-Specific Command Name

```bash
# (РЅРµ СЂРµР°Р»РёР·РѕРІР°РЅРѕ)
python -m bioetl.cli.app testitem --source pubchem
```

## Examples

```bash
# (РЅРµ СЂРµР°Р»РёР·РѕРІР°РЅРѕ)
python -m bioetl.cli.app testitem --source pubchem \
  --config configs/pipelines/pubchem/testitem.yaml \
  --output-dir data/output/testitem
```

## Related Documentation

- [PipelineBase Standard CLI Flags](../00-pipeline-base.md#81-standard-cli-flags) — General CLI flag descriptions and exit codes
- [29-testitem-pubchem-config.md](29-testitem-pubchem-config.md) — Configuration details
- [00-testitem-pubchem-overview.md](00-testitem-pubchem-overview.md) — Pipeline overview
