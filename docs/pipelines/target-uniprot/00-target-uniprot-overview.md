# 00 Target UniProt Overview

**Version:** 1.0.0  
**Date:** 2025-01-29  
**Author:** Data Acquisition Team

## Purpose

The Target (UniProt) pipeline is a standalone pipeline that extracts and processes target (protein) data from the UniProt REST API. This pipeline provides comprehensive protein information including sequences, features, gene names, and organism data.

**Note:** This pipeline requires UniProt accession numbers as input. For mapping ChEMBL target IDs to UniProt accessions, use the separate `chembl2uniprot-mapping` pipeline.

## Pipeline Architecture

The pipeline follows the standard ETL stages:

```
Extract → Transform → Validate → Write
```

### Components

- **Extract Stage**: UniProt REST API client (`https://rest.uniprot.org`) with batch retrieval, pagination, rate limiting
- **Transform Stage**: Protein data normalization, feature extraction, sequence processing
- **Validate Stage**: Pandera schema validation (TargetUniProtSchema)
- **Write Stage**: Atomic writer, protein target dataset, meta.yaml, QC reports

## Documentation Structure

This pipeline documentation is organized by stage and topic:

- [00-target-uniprot-overview.md](00-target-uniprot-overview.md) — Pipeline overview (this file)
- [09-target-uniprot-extraction.md](09-target-uniprot-extraction.md) — Extraction from UniProt REST API
- [10-target-uniprot-transformation.md](10-target-uniprot-transformation.md) — Protein data normalization
- [11-target-uniprot-validation.md](11-target-uniprot-validation.md) — Pandera schemas and validation
- [12-target-uniprot-io.md](12-target-uniprot-io.md) — Output formats and atomic writing
- [13-target-uniprot-determinism.md](13-target-uniprot-determinism.md) — Determinism, stable sort, hashing
- [14-target-uniprot-qc.md](14-target-uniprot-qc.md) — QC metrics and thresholds
- [15-target-uniprot-logging.md](15-target-uniprot-logging.md) — Structured logging format
- [16-target-uniprot-cli.md](16-target-uniprot-cli.md) — CLI commands and flags
- [17-target-uniprot-config.md](17-target-uniprot-config.md) — Configuration keys and profiles

## CLI Usage Example

```bash
# Standard extraction from UniProt
python -m bioetl.cli.main target-uniprot \
  --config configs/pipelines/uniprot/target.yaml \
  --output-dir data/output/target-uniprot

# With input file containing UniProt accessions
python -m bioetl.cli.main target-uniprot \
  --config configs/pipelines/uniprot/target.yaml \
  --input-file data/input/uniprot_accessions.csv \
  --output-dir data/output/target-uniprot
```

## Configuration

Configuration is defined in `src/bioetl/configs/pipelines/uniprot/target.yaml`. Key settings include:

- UniProt REST API configuration (base URL, rate limits, batch size)
- Field selection for protein records
- Determinism sort keys: `['uniprot_accession']`
- QC thresholds for data completeness

See [17-target-uniprot-config.md](17-target-uniprot-config.md) for detailed configuration documentation.

## Related Documentation

- [PipelineBase Specification](../00-pipeline-base.md) — General I/O, determinism, logging, and CLI documentation
- [ChEMBL to UniProt Mapping Pipeline](../28-chembl2uniprot-mapping.md) — Mapping ChEMBL targets to UniProt
- [ETL Contract Overview](../etl_contract/00-etl-overview.md) — Core ETL principles
- [Determinism Policy](../determinism/00-determinism-policy.md) — Deterministic output requirements
