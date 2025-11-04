# 00 Document Semantic Scholar Overview

**Version:** 1.0.0  
**Date:** 2025-01-29  
**Author:** Data Acquisition Team

## Purpose

The Document (Semantic Scholar) pipeline is a standalone pipeline that extracts publication metadata from Semantic Scholar using the Graph API. It provides comprehensive bibliographic information including titles, abstracts, authors, citations, and research paper embeddings metadata.

## Pipeline Architecture

The pipeline follows the standard ETL stages:

```
Extract → Transform → Validate → Write
```

### Components

- **Extract Stage**: Semantic Scholar Graph API client (`/paper/batch` endpoint) with PMID/DOI-based retrieval, optional API key authentication, rate limiting (1 req/1.25s without key, higher with key)
- **Transform Stage**: JSON response parsing, field normalization (DOI, PMID, title, abstract, authors, year, citation count)
- **Validate Stage**: Pandera schema validation (DocumentSemanticScholarSchema)
- **Write Stage**: Atomic writer, publication dataset, meta.yaml, QC reports

## Documentation Structure

This pipeline documentation is organized by stage and topic:

- [00-document-semantic-scholar-overview.md](00-document-semantic-scholar-overview.md) — Pipeline overview (this file)
- [09-document-semantic-scholar-extraction.md](09-document-semantic-scholar-extraction.md) — Extraction from Semantic Scholar Graph API
- [10-document-semantic-scholar-transformation.md](10-document-semantic-scholar-transformation.md) — JSON parsing and field normalization
- [11-document-semantic-scholar-validation.md](11-document-semantic-scholar-validation.md) — Pandera schemas and validation
- [12-document-semantic-scholar-io.md](12-document-semantic-scholar-io.md) — Output formats and atomic writing
- [13-document-semantic-scholar-determinism.md](13-document-semantic-scholar-determinism.md) — Determinism, stable sort, hashing
- [14-document-semantic-scholar-qc.md](14-document-semantic-scholar-qc.md) — QC metrics and thresholds
- [15-document-semantic-scholar-logging.md](15-document-semantic-scholar-logging.md) — Structured logging format
- [16-document-semantic-scholar-cli.md](16-document-semantic-scholar-cli.md) — CLI commands and flags
- [17-document-semantic-scholar-config.md](17-document-semantic-scholar-config.md) — Configuration keys and profiles

## CLI Usage Example

```bash
# Standard extraction from Semantic Scholar
python -m bioetl.cli.main document_semantic_scholar \
  --config configs/pipelines/semantic_scholar/document.yaml \
  --output-dir data/output/document-semantic-scholar

# With input file containing PMIDs
python -m bioetl.cli.main document_semantic_scholar \
  --config configs/pipelines/semantic_scholar/document.yaml \
  --input-file data/input/pmids.csv \
  --output-dir data/output/document-semantic-scholar
```

## Configuration

Configuration is defined in `src/bioetl/configs/pipelines/semantic_scholar/document.yaml`. Key settings include:

- Semantic Scholar Graph API configuration (base URL, optional API key)
- Rate limiting (1 request per 1.25 seconds without key, configurable with key)
- Batch size for PMID/DOI retrieval
- Determinism sort keys: `['pmid', 'doi', 'year']`
- QC thresholds for data completeness and access denial rate

See [17-document-semantic-scholar-config.md](17-document-semantic-scholar-config.md) for detailed configuration documentation.

## Related Documentation

- [ETL Contract Overview](../etl_contract/00-etl-overview.md) — Core ETL principles
- [Determinism Policy](../determinism/00-determinism-policy.md) — Deterministic output requirements
