# 00 TestItem PubChem Overview

**Version:** 1.0.0  
**Date:** 2025-01-29  
**Author:** Data Acquisition Team

## Purpose

The TestItem (PubChem) pipeline is a standalone pipeline that extracts testitem (molecule) data from PubChem. It is independent from ChEMBL pipelines and does not perform any joins or enrichment with other data sources.

## Pipeline Architecture

The pipeline follows the standard ETL stages:

```
Extract → Transform → Validate → Write
```

### Components

- **Extract Stage**: PubChem PUG REST API client with batch retrieval, rate limiting, caching
- **Transform Stage**: Molecule property normalization, structure data processing
- **Validate Stage**: Pandera schema validation (TestItemPubChemSchema)
- **Write Stage**: Atomic writer, molecule dataset, meta.yaml, QC reports

## Documentation Structure

This pipeline documentation is organized by stage and topic:

- [00-testitem-pubchem-overview.md](00-testitem-pubchem-overview.md) — Pipeline overview (this file)
- [09-testitem-pubchem-extraction.md](09-testitem-pubchem-extraction.md) — Extraction from PubChem PUG REST API
- [10-testitem-pubchem-transformation.md](10-testitem-pubchem-transformation.md) — Molecule property normalization
- [11-testitem-pubchem-validation.md](11-testitem-pubchem-validation.md) — Pandera schemas and validation
- [12-testitem-pubchem-io.md](12-testitem-pubchem-io.md) — Output formats and atomic writing
- [13-testitem-pubchem-determinism.md](13-testitem-pubchem-determinism.md) — Determinism, stable sort, hashing
- [14-testitem-pubchem-qc.md](14-testitem-pubchem-qc.md) — QC metrics and thresholds
- [15-testitem-pubchem-logging.md](15-testitem-pubchem-logging.md) — Structured logging format
- [16-testitem-pubchem-cli.md](16-testitem-pubchem-cli.md) — CLI commands and flags
- [17-testitem-pubchem-config.md](17-testitem-pubchem-config.md) — Configuration keys and profiles

## CLI Usage Example

```bash
# Standard extraction from PubChem
python -m bioetl.cli.main testitem_pubchem \
  --config configs/pipelines/pubchem/testitem_pubchem.yaml \
  --output-dir data/output/testitem_pubchem

# With input file containing PubChem CIDs
python -m bioetl.cli.main testitem_pubchem \
  --config configs/pipelines/pubchem/testitem_pubchem.yaml \
  --input-file data/input/pubchem_cids.csv \
  --output-dir data/output/testitem_pubchem
```

## Configuration

Configuration is defined in `src/bioetl/configs/pipelines/pubchem/testitem_pubchem.yaml`. Key settings include:

- PubChem PUG REST API configuration (base URL, rate limits)
- Batch size for compound retrieval
- Determinism sort keys: `['pubchem_cid']`
- QC thresholds for data completeness

See [17-testitem-pubchem-config.md](17-testitem-pubchem-config.md) for detailed configuration documentation.

## Related Documentation

- [ChEMBL Pipelines Catalog](10-chembl-pipelines-catalog.md) — Overview of ChEMBL pipelines (for comparison)
- [ETL Contract Overview](../etl_contract/00-etl-overview.md) — Core ETL principles
- [Determinism Policy](../determinism/00-determinism-policy.md) — Deterministic output requirements
