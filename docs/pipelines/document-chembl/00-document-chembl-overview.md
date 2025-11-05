# 00 Document ChEMBL Overview

**Version:** 1.0.0  
**Date:** 2025-01-29  
**Author:** Data Acquisition Team

## Purpose

The Document (ChEMBL) pipeline deterministically extracts, normalizes, and validates publication metadata from ChEMBL, optionally enriches with data from external bibliographic sources (PubMed, Crossref, OpenAlex, Semantic Scholar), and ensures full traceability, QC metrics, and atomic artifact writing.

## Pipeline Architecture

The pipeline follows the standard ETL stages:

```plaintext
Extract → Transform → Validate → Write
```

### Modes

- **`chembl` mode**: Extraction from ChEMBL API only
- **`all` mode**: ChEMBL + external sources (PubMed, Crossref, OpenAlex, Semantic Scholar)

### Components

- **Extract Stage**: ChEMBL Client with batch retrieval, TTL cache, circuit breaker, fallback manager
- **Transform Stage**: Normalization (DOI, PMID, year, authors, journal), enrichment adapters, field-level merge
- **Validate Stage**: Pandera schema validation, QC coverage checks, conflict detection
- **Write Stage**: Atomic writer, canonical serialization, metadata builder

## Documentation Structure

This pipeline documentation is organized by stage and topic:

- [00-document-chembl-overview.md](00-document-chembl-overview.md) — Pipeline overview (this file)
- [09-document-chembl-extraction.md](09-document-chembl-extraction.md) — Extraction from ChEMBL API
- [10-document-chembl-transformation.md](10-document-chembl-transformation.md) — Normalization and transformations
- [11-document-chembl-validation.md](11-document-chembl-validation.md) — Pandera schemas and validation
- [12-document-chembl-io.md](12-document-chembl-io.md) — Output formats and atomic writing
- [13-document-chembl-determinism.md](13-document-chembl-determinism.md) — Determinism, stable sort, hashing
- [14-document-chembl-qc.md](14-document-chembl-qc.md) — QC metrics and thresholds
- [15-document-chembl-logging.md](15-document-chembl-logging.md) — Structured logging format
- [16-document-chembl-cli.md](16-document-chembl-cli.md) — CLI commands and flags
- [17-document-chembl-config.md](17-document-chembl-config.md) — Configuration keys and profiles

## CLI Usage Example

```bash
# All-source enrichment run
python -m bioetl.cli.main document \
  --config configs/pipelines/chembl/document.yaml \
  --output-dir data/output/document

# ChEMBL only mode
python -m bioetl.cli.main document \
  --config configs/pipelines/chembl/document.yaml \
  --output-dir data/output/document \
  --set sources.pubmed.enabled=false \
  --set sources.crossref.enabled=false \
  --set sources.openalex.enabled=false \
  --set sources.semantic_scholar.enabled=false
```

## Configuration

Configuration is defined in `src/bioetl/configs/pipelines/chembl/document.yaml`. Key settings include:

- ChEMBL source configuration (batch size, URL limits)
- External source adapters (PubMed, Crossref, OpenAlex, Semantic Scholar)
- QC thresholds for coverage and conflicts
- Determinism settings (sort keys, hash generation)

See [17-document-chembl-config.md](17-document-chembl-config.md) for detailed configuration documentation.

## External Sources

External bibliographic adapters are documented separately:

- [09-document-pubmed-extraction.md](../document-pubmed/09-document-pubmed-extraction.md) — PubMed E-utilities API
- [09-document-crossref-extraction.md](../document-crossref/09-document-crossref-extraction.md) — Crossref REST API
- [09-document-openalex-extraction.md](../document-openalex/09-document-openalex-extraction.md) — OpenAlex Works API
- [09-document-semantic-scholar-extraction.md](../document-semantic-scholar/09-document-semantic-scholar-extraction.md) — Semantic Scholar Graph API

## Related Documentation

- [PipelineBase Specification](../00-pipeline-base.md) — General I/O, determinism, logging, and CLI documentation
- [Pipelines Catalog](../10-pipelines-catalog.md) — Overview of all pipelines
- [ETL Contract Overview](../etl_contract/00-etl-overview.md) — Core ETL principles
- [Determinism Policy](../determinism/00-determinism-policy.md) — Deterministic output requirements
