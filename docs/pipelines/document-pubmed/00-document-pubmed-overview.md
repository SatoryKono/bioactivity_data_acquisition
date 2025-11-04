# 00 Document PubMed Overview

**Version:** 1.0.0  
**Date:** 2025-01-29  
**Author:** Data Acquisition Team

## Purpose

The Document (PubMed) pipeline is a standalone pipeline that extracts publication metadata from PubMed using the E-utilities API. It provides comprehensive bibliographic information including titles, abstracts, authors, journal details, and publication metadata.

## Pipeline Architecture

The pipeline follows the standard ETL stages:

```
Extract → Transform → Validate → Write
```

### Components

- **Extract Stage**: PubMed E-utilities API client (efetch, esearch) with history server support, batch retrieval, rate limiting (3 req/sec without API key)
- **Transform Stage**: PubMed XML parsing, field normalization (PMID, DOI, authors, journal, publication dates)
- **Validate Stage**: Pandera schema validation (DocumentPubMedSchema)
- **Write Stage**: Atomic writer, publication dataset, meta.yaml, QC reports

## Documentation Structure

This pipeline documentation is organized by stage and topic:

- [00-document-pubmed-overview.md](00-document-pubmed-overview.md) — Pipeline overview (this file)
- [09-document-pubmed-extraction.md](09-document-pubmed-extraction.md) — Extraction from PubMed E-utilities API
- [10-document-pubmed-transformation.md](10-document-pubmed-transformation.md) — XML parsing and field normalization
- [11-document-pubmed-validation.md](11-document-pubmed-validation.md) — Pandera schemas and validation
- [12-document-pubmed-io.md](12-document-pubmed-io.md) — Output formats and atomic writing
- [13-document-pubmed-determinism.md](13-document-pubmed-determinism.md) — Determinism, stable sort, hashing
- [14-document-pubmed-qc.md](14-document-pubmed-qc.md) — QC metrics and thresholds
- [15-document-pubmed-logging.md](15-document-pubmed-logging.md) — Structured logging format
- [16-document-pubmed-cli.md](16-document-pubmed-cli.md) — CLI commands and flags
- [17-document-pubmed-config.md](17-document-pubmed-config.md) — Configuration keys and profiles

## CLI Usage Example

```bash
# Standard extraction from PubMed
python -m bioetl.cli.main document_pubmed \
  --config configs/pipelines/pubmed/document.yaml \
  --output-dir data/output/document-pubmed

# With input file containing PMIDs
python -m bioetl.cli.main document_pubmed \
  --config configs/pipelines/pubmed/document.yaml \
  --input-file data/input/pmids.csv \
  --output-dir data/output/document-pubmed
```

## Configuration

Configuration is defined in `src/bioetl/configs/pipelines/pubmed/document.yaml`. Key settings include:

- PubMed E-utilities API configuration (tool, email, optional API key)
- History server usage for large batches
- Rate limiting (3 requests per second without API key)
- Determinism sort keys: `['pmid']`
- QC thresholds for data completeness

See [17-document-pubmed-config.md](17-document-pubmed-config.md) for detailed configuration documentation.

## Related Documentation

- [PipelineBase Specification](../00-pipeline-base.md) — General I/O, determinism, logging, and CLI documentation
- [ETL Contract Overview](../etl_contract/00-etl-overview.md) — Core ETL principles
- [Determinism Policy](../determinism/00-determinism-policy.md) — Deterministic output requirements
