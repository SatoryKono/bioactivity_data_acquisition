# 00 TestItem ChEMBL Overview

**Version:** 1.0.0  
**Date:** 2025-01-29  
**Author:** Data Acquisition Team

## Purpose

The TestItem (ChEMBL) pipeline exports flattened molecule records from ChEMBL while preserving deterministic ordering by molecule ID. The pipeline flattens nested JSON structures from ChEMBL responses to create comprehensive, flat records for each molecule.

## Pipeline Architecture

The pipeline follows the standard ETL stages:

```
Extract → Transform → Validate → Write
```

### Components

- **Extract Stage**: ChEMBL `/molecule.json` endpoint with batch retrieval (batch_size ≤ 25), release-scoped caching, graceful degradation
- **Transform Stage**: Nested structure flattening, PubChem enrichment (optional)
- **Validate Stage**: Pandera schema validation (TestItemSchema), duplicate tracking, referential integrity checks
- **Write Stage**: Atomic writer, flattened molecule dataset, meta.yaml, QC reports

## Documentation Structure

This pipeline documentation is organized by stage and topic:

- [00-testitem-chembl-overview.md](00-testitem-chembl-overview.md) — Pipeline overview (this file)
- [09-testitem-chembl-extraction.md](09-testitem-chembl-extraction.md) — Extraction from ChEMBL API
- [10-testitem-chembl-transformation.md](10-testitem-chembl-transformation.md) — Structure flattening and transformations
- [11-testitem-chembl-validation.md](11-testitem-chembl-validation.md) — Pandera schemas and validation
- [12-testitem-chembl-io.md](12-testitem-chembl-io.md) — Output formats and atomic writing
- [13-testitem-chembl-determinism.md](13-testitem-chembl-determinism.md) — Determinism, stable sort, hashing
- [14-testitem-chembl-qc.md](14-testitem-chembl-qc.md) — QC metrics and thresholds
- [15-testitem-chembl-logging.md](15-testitem-chembl-logging.md) — Structured logging format
- [16-testitem-chembl-cli.md](16-testitem-chembl-cli.md) — CLI commands and flags
- [17-testitem-chembl-config.md](17-testitem-chembl-config.md) — Configuration keys and profiles

## CLI Usage Example

```bash
# Base molecule export
python -m bioetl.cli.main testitem \
  --config configs/pipelines/chembl/testitem_chembl.yaml \
  --output-dir data/output/testitem

# Override batch size for smoke test
python -m bioetl.cli.main testitem \
  --config configs/pipelines/chembl/testitem_chembl.yaml \
  --output-dir data/output/testitem \
  --set sources.chembl.batch_size=10 \
  --limit 100
```

## Configuration

Configuration is defined in `src/bioetl/configs/pipelines/chembl/testitem_chembl.yaml`. Key settings include:

- ChEMBL source configuration (batch_size ≤ 25, max_url_length ≤ 2000)
- Determinism sort keys: `['molecule_chembl_id']`
- PubChem enrichment (postprocess configuration)
- QC thresholds: duplicate_ratio (0.0), fallback_ratio (0.2), parent_missing_ratio (0.0)

See [17-testitem-chembl-config.md](17-testitem-chembl-config.md) for detailed configuration documentation.

## PubChem Enrichment

PubChem enrichment is described in a separate document:

- [09-testitem-pubchem-extraction.md](09-testitem-pubchem-extraction.md) — PubChem TestItem Pipeline

## Related Documentation

- [PipelineBase Specification](../00-pipeline-base.md) — General I/O, determinism, logging, and CLI documentation
- [Pipelines Catalog](../10-pipelines-catalog.md) — Overview of all pipelines
- [ETL Contract Overview](../etl_contract/00-etl-overview.md) — Core ETL principles
- [Determinism Policy](../determinism/00-determinism-policy.md) — Deterministic output requirements
