# 00 Document Crossref Overview

**Version:** 1.0.0
**Date:** 2025-01-29
**Author:** Data Acquisition Team

> **Статус:** not implemented (CLI команда отсутствует в `COMMAND_REGISTRY`).

## Purpose

The Document (Crossref) pipeline is a standalone pipeline that extracts publication metadata from Crossref using the REST API `/works` endpoint. It provides comprehensive bibliographic information including titles, authors, journal details, publication dates, and citation metadata.

## Pipeline Architecture

The pipeline follows the standard ETL stages:

```text
Extract → Transform → Validate → Write
```

### Components

- **Extract Stage**: Crossref REST API client (`/works` endpoint) with DOI-based retrieval, batch processing (up to 100 DOIs per request), rate limiting (2 req/sec), mailto requirement in User-Agent
- **Transform Stage**: JSON response parsing, field normalization (DOI, title, authors, journal, year, page numbers)
- **Validate Stage**: Pandera schema validation (DocumentCrossrefSchema)
- **Write Stage**: Atomic writer, publication dataset, meta.yaml, QC reports

## Documentation Structure

This pipeline documentation is organized by stage and topic:

- [00-document-crossref-overview.md](00-document-crossref-overview.md) — Pipeline overview (this file)
- [09-document-crossref-extraction.md](09-document-crossref-extraction.md) — Extraction from Crossref REST API
- [59-document-crossref-transformation.md](59-document-crossref-transformation.md) — JSON parsing and field normalization
- [60-document-crossref-validation.md](60-document-crossref-validation.md) — Pandera schemas and validation
- [61-document-crossref-io.md](61-document-crossref-io.md) — Output formats and atomic writing
- [62-document-crossref-determinism.md](62-document-crossref-determinism.md) — Determinism, stable sort, hashing
- [63-document-crossref-qc.md](63-document-crossref-qc.md) — QC metrics and thresholds
- [64-document-crossref-logging.md](64-document-crossref-logging.md) — Structured logging format
- [65-document-crossref-cli.md](65-document-crossref-cli.md) — CLI commands and flags
- [66-document-crossref-config.md](66-document-crossref-config.md) — Configuration keys and profiles

## CLI Usage Example

```bash
# Standard extraction from Crossref (not implemented)
python -m bioetl.cli.app document_crossref \
  --config configs/pipelines/crossref/document.yaml \
  --output-dir data/output/document-crossref

# With input file containing DOIs (not implemented)
python -m bioetl.cli.app document_crossref \
  --config configs/pipelines/crossref/document.yaml \
  --input-file data/input/dois.csv \
  --output-dir data/output/document-crossref
```

## Configuration

Configuration is defined in `src/bioetl/configs/pipelines/crossref/document.yaml`. Key settings include:

- Crossref REST API configuration (base URL, mailto in User-Agent)
- Batch size for DOI requests (up to 100 DOIs per request, max 200)
- Rate limiting (2 requests per second)
- Determinism sort keys: `['doi', 'year']`
- QC thresholds for data completeness

See [66-document-crossref-config.md](66-document-crossref-config.md) for detailed configuration documentation.

## Related Documentation

- [PipelineBase Specification](../00-pipeline-base.md) — General I/O, determinism, logging, and CLI documentation
- [ETL Contract Overview](../etl_contract/00-etl-overview.md) — Core ETL principles
- [Determinism Policy](../determinism/00-determinism-policy.md) — Deterministic output requirements
