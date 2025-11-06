# 00 Assay ChEMBL Overview

**Version:** 1.0.0  
**Date:** 2025-01-29  
**Author:** Data Acquisition Team

## Purpose

The Assay (ChEMBL) pipeline extracts assay metadata from ChEMBL API with deterministic results, full reproducibility, and protection against data loss. It runs a multi-stage pipeline (extract → transform → validate → write) including nested structure expansion, enrichment, and strict schema enforcement.

## Pipeline Architecture

The pipeline follows the standard ETL stages:

```text
Extract → Transform → Validate → Write
```

### Components

- **Extract Stage**: ChEMBL Client with batch retrieval (batch_size=25), URL limit enforcement, TTL cache, circuit breaker, fallback manager
- **Transform Stage**: Nested structure expansion (long format), enrichment (targets, assay classes), normalization with strict NA policy
- **Validate Stage**: Pandera schema validation (strict=True), referential integrity checks, quality profile with fail thresholds
- **Write Stage**: Atomic writer with run_id-scoped temp dirs, canonical serialization, metadata builder

## Documentation Structure

This pipeline documentation is organized by stage and topic:

- [00-assay-chembl-overview.md](00-assay-chembl-overview.md) — Pipeline overview (this file)
- [09-assay-chembl-extraction.md](09-assay-chembl-extraction.md) — Extraction from ChEMBL API
- [10-assay-chembl-transformation.md](10-assay-chembl-transformation.md) — Nested structure expansion and transformations
- [11-assay-chembl-validation.md](11-assay-chembl-validation.md) — Pandera schemas and validation
- [12-assay-chembl-io.md](12-assay-chembl-io.md) — Output formats and atomic writing
- [13-assay-chembl-determinism.md](13-assay-chembl-determinism.md) — Determinism, stable sort, hashing
- [14-assay-chembl-qc.md](14-assay-chembl-qc.md) — QC metrics and thresholds
- [15-assay-chembl-logging.md](15-assay-chembl-logging.md) — Structured logging format
- [16-assay-chembl-cli.md](16-assay-chembl-cli.md) — CLI commands and flags
- [17-assay-chembl-config.md](17-assay-chembl-config.md) — Configuration keys and profiles

## CLI Usage Example

```bash
# Standard production extraction
python -m bioetl.cli.main assay_chembl \
  --config configs/pipelines/assay/assay_chembl.yaml \
  --output-dir data/output/assay

# Throttle the client for troubleshooting
python -m bioetl.cli.main assay_chembl \
  --config configs/pipelines/assay/assay_chembl.yaml \
  --output-dir data/output/assay \
  --set sources.chembl.batch_size=20
```

## Configuration

Configuration is defined in `src/bioetl/configs/pipelines/assay/assay_chembl.yaml`. Key settings include:

- ChEMBL source configuration (batch_size ≤ 25, max_url_length ≤ 2000)
- Cache namespace for release-scoped invalidation
- Determinism sort keys: `['assay_chembl_id', 'row_subtype', 'row_index']`
- QC thresholds for fallback usage rate

See [17-assay-chembl-config.md](17-assay-chembl-config.md) for detailed configuration documentation.

## Related Documentation

- [PipelineBase Specification](../00-pipeline-base.md) — General I/O, determinism, logging, and CLI documentation
- [Pipelines Catalog](../10-pipelines-catalog.md) — Overview of all pipelines
- [ETL Contract Overview](../etl_contract/00-etl-overview.md) — Core ETL principles
- [Determinism Policy](../determinism/00-determinism-policy.md) — Deterministic output requirements
