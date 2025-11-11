# 00 Target ChEMBL Overview

**Version:** 1.0.0
**Date:** 2025-01-29
**Author:** Data Acquisition Team

## Purpose

The Target (ChEMBL) pipeline collects ChEMBL target definitions and enriches them with UniProt and IUPHAR data to produce a consolidated target gold dataset. It implements a multi-stage pipeline: ChEMBL → UniProt → IUPHAR + post-processing.

## Pipeline Architecture

The pipeline follows the standard ETL stages with enrichment layers:

```text
Extract (ChEMBL) → Enrich (UniProt/IUPHAR) → Transform → Validate → Write
```

### Components

- **Extract Stage**: ChEMBL `/target.json` endpoint via UnifiedAPIClient
- **Enrichment Stage**: UniProt ID mapping and ortholog services, IUPHAR target families
- **Transform Stage**: Data consolidation and normalization
- **Validate Stage**: Pandera schema validation, enrichment success rate monitoring
- **Write Stage**: Atomic writer, unified target dataset, meta.yaml

## Documentation Structure

This pipeline documentation is organized by stage and topic:

- [00-target-chembl-overview.md](00-target-chembl-overview.md) — Pipeline overview (this file)
- [09-target-chembl-extraction.md](09-target-chembl-extraction.md) — Extraction from ChEMBL API
- [10-target-chembl-transformation.md](10-target-chembl-transformation.md) — Data consolidation and transformations
- [11-target-chembl-validation.md](11-target-chembl-validation.md) — Pandera schemas and validation
- [12-target-chembl-io.md](12-target-chembl-io.md) — Output formats and atomic writing
- [13-target-chembl-determinism.md](13-target-chembl-determinism.md) — Determinism, stable sort, hashing
- [14-target-chembl-qc.md](14-target-chembl-qc.md) — QC metrics and thresholds
- [15-target-chembl-logging.md](15-target-chembl-logging.md) — Structured logging format
- [16-target-chembl-cli.md](16-target-chembl-cli.md) — CLI commands and flags
- [17-target-chembl-config.md](17-target-chembl-config.md) — Configuration keys and profiles

## CLI Usage Example

```bash
# Full enrichment run
python -m bioetl.cli.app target \
  --config configs/pipelines/target/target_chembl.yaml \
  --output-dir data/output/target

# Disable UniProt enrichment for a connectivity check
python -m bioetl.cli.app target \
  --config configs/pipelines/target/target_chembl.yaml \
  --output-dir data/output/target \
  --set sources.uniprot.enabled=false
```

## Configuration

Configuration is defined in `src/bioetl/configs/pipelines/target/target_chembl.yaml`. Key settings include:

- ChEMBL source configuration (shared profile)
- UniProt enrichment (enabled/disabled, batch size for ID mapping)
- IUPHAR configuration (API key, caching, minimum enrichment ratios)
- QC thresholds for enrichment success rates and fallback usage

See [17-target-chembl-config.md](17-target-chembl-config.md) for detailed configuration documentation.

## External Sources

External target enrichment adapters:

- [09-target-uniprot-extraction.md](09-target-uniprot-extraction.md) — UniProt REST API
- [09-target-iuphar-extraction.md](09-target-iuphar-extraction.md) — IUPHAR Target Pipeline
- [28-chembl2uniprot-mapping.md](28-chembl2uniprot-mapping.md) — ChEMBL to UniProt Mapping Pipeline

## Related Documentation

- [PipelineBase Specification](../00-pipeline-base.md) — General I/O, determinism, logging, and CLI documentation
- [Pipelines Catalog](../10-pipelines-catalog.md) — Overview of all pipelines
- [ETL Contract Overview](../etl_contract/00-etl-overview.md) — Core ETL principles
- [Determinism Policy](../determinism/00-determinism-policy.md) — Deterministic output requirements
