# 00 Document Semantic Scholar Overview


**Version:** 1.0.0 **Date:** 2025-01-29 **Author:** Data Acquisition Team


## Purpose

The Document (Semantic Scholar) pipeline is a standalone pipeline that extracts
publication metadata from Semantic Scholar using the Graph API. It provides
comprehensive bibliographic information including titles, abstracts, authors,
citations, and research paper embeddings metadata.

## Pipeline Architecture

The pipeline follows the standard ETL stages:

```text
Extract → Transform → Validate → Write
```

### Components

- **Extract Stage**: Semantic Scholar Graph API client (`/paper/batch` endpoint)
  with PMID/DOI-based retrieval, optional API key authentication, rate limiting
  (1 req/1.25s without key, higher with key)
- **Transform Stage**: JSON response parsing, field normalization (DOI, PMID,
  title, abstract, authors, year, citation count)
- **Validate Stage**: Pandera schema validation (DocumentSemanticScholarSchema)
- **Write Stage**: Atomic writer, publication dataset, meta.yaml, QC reports

## Documentation Structure

This pipeline documentation is organized by stage and topic:

- [00-document-semanticscholar-overview.md](00-document-semanticscholar-overview.md)
  — Pipeline overview (this file)
- [09-document-semanticscholar-extraction.md](09-document-semanticscholar-extraction.md)
  — Extraction from Semantic Scholar Graph API
- [10-document-semanticscholar-transformation.md](10-document-semanticscholar-transformation.md)
  — JSON parsing and field normalization
- [11-document-semanticscholar-validation.md](11-document-semanticscholar-validation.md)
  — Pandera schemas and validation
- [12-document-semanticscholar-io.md](12-document-semanticscholar-io.md) —
  Output formats and atomic writing
- [13-document-semanticscholar-determinism.md](13-document-semanticscholar-determinism.md)
  — Determinism, stable sort, hashing
- [14-document-semanticscholar-qc.md](14-document-semanticscholar-qc.md) — QC
  metrics and thresholds
- [15-document-semanticscholar-logging.md](15-document-semanticscholar-logging.md)
  — Structured logging format
- [16-document-semanticscholar-cli.md](16-document-semanticscholar-cli.md) —
  CLI commands and flags
- [17-document-semanticscholar-config.md](17-document-semanticscholar-config.md)
  — Configuration keys and profiles

## CLI Usage Example

```bash
# Standard extraction from Semantic Scholar (not implemented)
python -m bioetl.cli.cli_app document_semantic_scholar \
  --config configs/pipelines/semantic_scholar/document.yaml \
  --output-dir data/output/document-semanticscholar

# With input file containing PMIDs (not implemented)
python -m bioetl.cli.cli_app document_semantic_scholar \
  --config configs/pipelines/semantic_scholar/document.yaml \
  --input-file data/input/pmids.csv \
  --output-dir data/output/document-semanticscholar
```

## Configuration

Configuration is defined in
`src/bioetl/configs/pipelines/semantic_scholar/document.yaml`. Key settings
include:

- Semantic Scholar Graph API configuration (base URL, optional API key)
- Rate limiting (1 request per 1.25 seconds without key, configurable with key)
- Batch size for PMID/DOI retrieval
- Determinism sort keys: `['pmid', 'doi', 'year']`
- QC thresholds for data completeness and access denial rate

See
[17-document-semanticscholar-config.md](17-document-semanticscholar-config.md)
for detailed configuration documentation.

## Related Documentation

- [PipelineBase Specification](../00-pipeline-base.md) — General I/O,
  determinism, logging, and CLI documentation
- [ETL Contract Overview](../etl_contract/00-etl-overview.md) — Core ETL
  principles
- [Determinism Policy](../determinism/00-determinism-policy.md) — Deterministic
  output requirements
