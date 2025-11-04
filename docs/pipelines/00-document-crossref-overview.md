# 00 Document Crossref Overview

**Version:** 1.0.0  
**Date:** 2025-01-29  
**Author:** Data Acquisition Team

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
- [10-document-crossref-transformation.md](10-document-crossref-transformation.md) — JSON parsing and field normalization
- [11-document-crossref-validation.md](11-document-crossref-validation.md) — Pandera schemas and validation
- [12-document-crossref-io.md](12-document-crossref-io.md) — Output formats and atomic writing
- [13-document-crossref-determinism.md](13-document-crossref-determinism.md) — Determinism, stable sort, hashing
- [14-document-crossref-qc.md](14-document-crossref-qc.md) — QC metrics and thresholds
- [15-document-crossref-logging.md](15-document-crossref-logging.md) — Structured logging format
- [16-document-crossref-cli.md](16-document-crossref-cli.md) — CLI commands and flags
- [17-document-crossref-config.md](17-document-crossref-config.md) — Configuration keys and profiles

## CLI Usage Example

```bash
# Standard extraction from Crossref
python -m bioetl.cli.main document_crossref \
  --config configs/pipelines/crossref/document.yaml \
  --output-dir data/output/document-crossref

# With input file containing DOIs
python -m bioetl.cli.main document_crossref \
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

See [17-document-crossref-config.md](17-document-crossref-config.md) for detailed configuration documentation.

## Related Documentation

- [ETL Contract Overview](../etl_contract/00-etl-overview.md) — Core ETL principles
- [Determinism Policy](../determinism/00-determinism-policy.md) — Deterministic output requirements
