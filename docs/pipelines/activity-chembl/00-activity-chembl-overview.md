# 00 Activity ChEMBL Overview

**Version:** 1.0.0  
**Date:** 2025-01-29  
**Author:** Data Acquisition Team

## Purpose

The Activity (ChEMBL) pipeline extracts biological activity records from ChEMBL Data Web Services, normalizes measurement fields, validates records, and generates deterministic artifacts with lineage metadata.

## Pipeline Architecture

The pipeline follows the standard ETL stages:

```
Extract → Transform → Validate → Write
```

### Components

- **Extract Stage**: ChEMBL REST API client with batch retrieval (batch_size ≤ 25), release handshake, UnifiedAPIClient
- **Transform Stage**: Measurement field normalization (identifiers, numeric values, units)
- **Validate Stage**: Pandera schema validation (ActivitySchema)
- **Write Stage**: Atomic writer, QC reports, correlation analysis (optional), meta.yaml generation

## Documentation Structure

This pipeline documentation is organized by stage and topic:

- [00-activity-chembl-overview.md](00-activity-chembl-overview.md) — Pipeline overview (this file)
- [09-activity-chembl-extraction.md](09-activity-chembl-extraction.md) — Extraction from ChEMBL API
- [10-activity-chembl-transformation.md](10-activity-chembl-transformation.md) — Measurement field normalization
- [11-activity-chembl-validation.md](11-activity-chembl-validation.md) — Pandera schemas and validation
- [12-activity-chembl-io.md](12-activity-chembl-io.md) — Output formats and atomic writing
- [13-activity-chembl-determinism.md](13-activity-chembl-determinism.md) — Determinism, stable sort, hashing
- [14-activity-chembl-qc.md](14-activity-chembl-qc.md) — QC metrics and thresholds
- [15-activity-chembl-logging.md](15-activity-chembl-logging.md) — Structured logging format
- [16-activity-chembl-cli.md](16-activity-chembl-cli.md) — CLI commands and flags
- [17-activity-chembl-config.md](17-activity-chembl-config.md) — Configuration keys and profiles

## CLI Usage Example

```bash
# Deterministic run with the canonical config
python -m bioetl.cli.main activity_chembl \
  --config configs/pipelines/chembl/activity_chembl.yaml \
  --output-dir data/output/activity

# Override batch size for a smoke test
python -m bioetl.cli.main activity_chembl \
  --config configs/pipelines/chembl/activity_chembl.yaml \
  --output-dir data/output/activity \
  --set sources.chembl.batch_size=10
```

## Configuration

Configuration is defined in `src/bioetl/configs/pipelines/chembl/activity_chembl.yaml`. Key settings include:

- ChEMBL source configuration (batch_size ≤ 25, required)
- Correlation report generation (optional, disabled by default)
- Determinism sort keys: `['assay_id', 'testitem_id', 'activity_id']`
- QC thresholds for duplicates and measurement validity

See [17-activity-chembl-config.md](17-activity-chembl-config.md) for detailed configuration documentation.

## Related Documentation

- [PipelineBase Specification](../00-pipeline-base.md) — General I/O, determinism, logging, and CLI documentation
- [Pipelines Catalog](../10-pipelines-catalog.md) — Overview of all pipelines
- [ETL Contract Overview](../etl_contract/00-etl-overview.md) — Core ETL principles
- [Determinism Policy](../determinism/00-determinism-policy.md) — Deterministic output requirements
