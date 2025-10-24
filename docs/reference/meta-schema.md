# Meta Schema Reference

## Overview

The project uses a unified metadata schema for all ETL pipelines. This schema is designed to be:

- **Deterministic**: Consistent structure across all pipelines
- **Portable**: No binary/pickle objects in YAML files
- **Validated**: Pandera schema validation
- **Checksummed**: MD5 and SHA256 hashes for integrity

## Schema Structure

```yaml
pipeline:
  name: <entity>                    # Pipeline name (documents, targets, etc.)
  version: "2.0.0"                 # Pipeline version
  entity_type: <entity>            # Entity type
  source_system: "chembl"          # Source system

execution:
  run_id: <entity>_YYYYMMDD_HHMMSS_<uuid>  # Unique run identifier
  started_at: <ISO8601>            # Start timestamp
  completed_at: <ISO8601>          # Completion timestamp
  duration_sec: <float>             # Duration in seconds

data:
  row_count: <int>                 # Total rows processed
  row_count_accepted: <int>        # Accepted rows
  row_count_rejected: <int>        # Rejected rows
  columns_count: <int>             # Number of columns

sources:
  - name: "chembl"                 # Source name
    version: "ChEMBL_33"           # Source version
    records: <int>                 # Records from this source

validation:
  schema_passed: <bool>            # Schema validation result
  qc_passed: <bool>                # Quality control result
  warnings: <int>                  # Number of warnings
  errors: <int>                    # Number of errors

files:
  dataset: <entity>_YYYYMMDD.csv                    # Main dataset
  quality_report: <entity>_YYYYMMDD_quality_report.csv # QC report
  correlation_report: <entity>_correlation_report_YYYYMMDD/ # Correlation analysis

checksums:
  <filename>_md5: <hash>           # MD5 hash
  <filename>_sha256: <hash>        # SHA256 hash
```

## File Naming Convention

All output files follow the pattern: `<entity>_YYYYMMDD.*`

- **Main dataset**: `<entity>_YYYYMMDD.csv`
- **Metadata**: `<entity>_YYYYMMDD.meta.yaml`
- **Quality report**: `<entity>_YYYYMMDD_quality_report.csv`
- **Correlation analysis**: `<entity>_correlation_report_YYYYMMDD/`

## Validation

The metadata is validated using Pandera schemas in `src/library/schemas/meta_schema.py`:

```python
from library.schemas.meta_schema import validate_metadata_file

# Validate a meta.yaml file
metadata = validate_metadata_file("documents_20251024.meta.yaml")
```

## Determinism

To ensure deterministic output:

1. **Column ordering**: Fixed order from `column_order` in YAML configs
2. **Row sorting**: Stable sort by primary keys before export
3. **Checksums**: MD5 and SHA256 for all output files
4. **No binary data**: YAML files contain only text data

## Example

```yaml
pipeline:
  name: documents
  version: "2.0.0"
  entity_type: documents
  source_system: chembl

execution:
  run_id: documents_20251024_143022_a1b2c3d4
  started_at: "2025-10-24T14:30:22Z"
  completed_at: "2025-10-24T14:35:18Z"
  duration_sec: 296.5

data:
  row_count: 1500
  row_count_accepted: 1485
  row_count_rejected: 15
  columns_count: 12

sources:
  - name: chembl
    version: ChEMBL_33
    records: 1500

validation:
  schema_passed: true
  qc_passed: true
  warnings: 3
  errors: 0

files:
  dataset: documents_20251024.csv
  quality_report: documents_20251024_quality_report.csv
  correlation_report: documents_correlation_report_20251024/

checksums:
  documents_20251024.csv_md5: a1b2c3d4e5f6...
  documents_20251024.csv_sha256: 1a2b3c4d5e6f...
  documents_20251024_quality_report.csv_md5: f6e5d4c3b2a1...
  documents_20251024_quality_report.csv_sha256: 6f5e4d3c2b1a...
```
