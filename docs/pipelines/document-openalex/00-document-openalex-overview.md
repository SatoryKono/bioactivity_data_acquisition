# 00 Document OpenAlex Overview

**Version:** 1.0.0
**Date:** 2025-01-29
**Author:** Data Acquisition Team

## Purpose

The Document (OpenAlex) pipeline is a standalone pipeline that extracts publication metadata from OpenAlex using the Works API. It provides comprehensive bibliographic information including titles, abstracts, authors, citations, and open access indicators.

## Pipeline Architecture

The pipeline follows the standard ETL stages:

```plaintext
Extract → Transform → Validate → Write
```

### Components

- **Extract Stage**: OpenAlex REST API client (`/works` endpoint) with DOI/PMID-based retrieval, rate limiting (10 req/sec), mailto requirement in User-Agent
- **Transform Stage**: JSON response parsing, field normalization (DOI, title, authors, journal, year, citations)
- **Validate Stage**: Pandera schema validation (DocumentOpenAlexSchema)
- **Write Stage**: Atomic writer, publication dataset, meta.yaml, QC reports

## Documentation Structure

This pipeline documentation is organized by stage and topic:

- [00-document-openalex-overview.md](00-document-openalex-overview.md) — Pipeline overview (this file)
- [09-document-openalex-extraction.md](09-document-openalex-extraction.md) — Extraction from OpenAlex Works API
- [51-document-openalex-transformation.md](51-document-openalex-transformation.md) — JSON parsing and field normalization
- [52-document-openalex-validation.md](52-document-openalex-validation.md) — Pandera schemas and validation
- [53-document-openalex-io.md](53-document-openalex-io.md) — Output formats and atomic writing
- [54-document-openalex-determinism.md](54-document-openalex-determinism.md) — Determinism, stable sort, hashing
- [55-document-openalex-qc.md](55-document-openalex-qc.md) — QC metrics and thresholds
- [56-document-openalex-logging.md](56-document-openalex-logging.md) — Structured logging format
- [57-document-openalex-cli.md](57-document-openalex-cli.md) — CLI commands and flags
- [58-document-openalex-config.md](58-document-openalex-config.md) — Configuration keys and profiles

## CLI Usage Example

```bash
# Standard extraction from OpenAlex
python -m bioetl.cli.app document_openalex \
  --config configs/pipelines/openalex/document.yaml \
  --output-dir data/output/document-openalex

# With input file containing DOIs
python -m bioetl.cli.app document_openalex \
  --config configs/pipelines/openalex/document.yaml \
  --input-file data/input/dois.csv \
  --output-dir data/output/document-openalex
```

## Configuration

Configuration is defined in `src/bioetl/configs/pipelines/openalex/document.yaml`. Key settings include:

- OpenAlex REST API configuration (base URL, mailto in User-Agent)
- Rate limiting (10 requests per second)
- Batch size for DOI/PMID retrieval
- Determinism sort keys: `['doi', 'year']`
- QC thresholds for data completeness

See [58-document-openalex-config.md](58-document-openalex-config.md) for detailed configuration documentation.

## Related Documentation

- [PipelineBase Specification](../00-pipeline-base.md) — General I/O, determinism, logging, and CLI documentation
- [ETL Contract Overview](../etl_contract/00-etl-overview.md) — Core ETL principles
- [Determinism Policy](../determinism/00-determinism-policy.md) — Deterministic output requirements
