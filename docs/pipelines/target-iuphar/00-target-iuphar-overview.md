# 00 Target IUPHAR Overview

**Version:** 1.0.0
**Date:** 2025-01-29
**Author:** Data Acquisition Team

## Purpose

The Target (IUPHAR) pipeline is a standalone pipeline that extracts and processes target (pharmacological target) data from the Guide to Pharmacology (GtP) / IUPHAR database. This pipeline provides comprehensive pharmacological target information including target classifications, receptor families, and pharmacological properties.

**Note:** This pipeline supports multiple input formats and automatically resolves identifiers to IUPHAR target_id through the search API. Enrichment from external sources (UniProt, ChEMBL) is handled by separate pipelines.

## Pipeline Architecture

The pipeline follows the standard ETL stages:

```
Extract → Transform → Validate → Write
```

### Components

- **Extract Stage**: Guide to Pharmacology API client (`https://www.guidetopharmacology.org/DATA`) with identifier resolution, batch retrieval, API key authentication
- **Transform Stage**: Target classification normalization, family assignment, pharmacological data processing
- **Validate Stage**: Pandera schema validation (TargetIUPHARSchema)
- **Write Stage**: Atomic writer, pharmacological target dataset, meta.yaml, QC reports

## Documentation Structure

This pipeline documentation is organized by stage and topic:

- [00-target-iuphar-overview.md](00-target-iuphar-overview.md) — Pipeline overview (this file)
- [09-target-iuphar-extraction.md](09-target-iuphar-extraction.md) — Extraction from Guide to Pharmacology API
- [10-target-iuphar-transformation.md](10-target-iuphar-transformation.md) — Target classification normalization
- [11-target-iuphar-validation.md](11-target-iuphar-validation.md) — Pandera schemas and validation
- [12-target-iuphar-io.md](12-target-iuphar-io.md) — Output formats and atomic writing
- [13-target-iuphar-determinism.md](13-target-iuphar-determinism.md) — Determinism, stable sort, hashing
- [14-target-iuphar-qc.md](14-target-iuphar-qc.md) — QC metrics and thresholds
- [15-target-iuphar-logging.md](15-target-iuphar-logging.md) — Structured logging format
- [16-target-iuphar-cli.md](16-target-iuphar-cli.md) — CLI commands and flags
- [17-target-iuphar-config.md](17-target-iuphar-config.md) — Configuration keys and profiles

## CLI Usage Example

```bash
# Standard extraction from IUPHAR
# (not implemented)
python -m bioetl.cli.app target-iuphar \
  --config configs/pipelines/iuphar/target.yaml \
  --output-dir data/output/target-iuphar

# With input file containing various identifiers
# (not implemented)
python -m bioetl.cli.app target-iuphar \
  --config configs/pipelines/iuphar/target.yaml \
  --input-file data/input/iuphar_targets.csv \
  --output-dir data/output/target-iuphar
```

## Configuration

Configuration is defined in `src/bioetl/configs/pipelines/iuphar/target.yaml`. Key settings include:

- Guide to Pharmacology API configuration (base URL, API key, rate limits)
- Identifier resolution settings
- Determinism sort keys: `['iuphar_target_id']`
- QC thresholds for data completeness and classification coverage

See [17-target-iuphar-config.md](17-target-iuphar-config.md) for detailed configuration documentation.

## Related Documentation

- [PipelineBase Specification](../00-pipeline-base.md) — General I/O, determinism, logging, and CLI documentation
- [ETL Contract Overview](../etl_contract/00-etl-overview.md) — Core ETL principles
- [Determinism Policy](../determinism/00-determinism-policy.md) — Deterministic output requirements
