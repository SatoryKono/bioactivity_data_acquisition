# 2. Pipeline Configuration

> **Note**: Implementation status: **planned**. All file paths referencing
> `src/bioetl/` in this document describe the intended architecture and are not
> yet implemented in the codebase.

## Overview

Every pipeline in the `bioetl` framework is driven by a declarative YAML
configuration file. This approach separates the pipeline's logic (defined in its
Python class) from its behavior (defined in the YAML), making pipelines
flexible, reusable, and easy to manage.

All configuration files are validated at runtime against a set of strongly-typed
Pydantic models located in `src/bioetl/config/models/`. The models are organized
into logical modules (base, http, cache, paths, determinism, validation,
transform, postprocess, source, cli, fallbacks) and are all re-exported through
`src/bioetl/config/models/__init__.py` for backward compatibility. This ensures
that all configurations are well-formed and contain all necessary parameters
before the pipeline begins execution.

## Configuration Skeleton

`PipelineConfig` now separates domain settings (schemas, transforms, enrichment)
from infrastructure settings (IO, HTTP, logging, determinism). A representative
YAML snippet:

```yaml
# src/bioetl/configs/pipelines/<provider>/<pipeline>.yaml
version: 1
extends:
  - ../../defaults/base.yaml
  - ../../defaults/determinism.yaml

pipeline:
  name: activity
  version: "1.2.3"
  owner: "activity@bioetl"

domain:
  validation:
    schema_out: "bioetl.schemas.chembl.activity.ActivityOutput"
    strict: true
  transform:
    flatten_objects:
      activity_properties:
        - pref_name
        - value
  postprocess:
    hash_row_columns:
      - activity_id
      - assay_id
  fallbacks:
    enabled: true
  sources:
    chembl:
      enabled: true
      description: "Primary ChemBL snapshot"
  chembl:
    enrich_descriptors: true

infrastructure:
  runtime:
    parallelism: 4
  io:
    read_batch_size: 10000
  http:
    default:
      base_url: "https://www.ebi.ac.uk"
      timeout_seconds: 30
  cache:
    ttl_seconds: 86400
  paths:
    input_root: "data/input"
    output_root: "data/output"
  determinism:
    column_order:
      - activity_id
      - assay_id
  materialization:
    default_format: "parquet"
  logging:
    level: "INFO"
  telemetry:
    exporters: []
  cli:
    limit: null
```

## Section Details

- **`extends`**: Allows for the composition of configurations. Common settings
  can be placed in base files (like `base.yaml`) and included in multiple
  pipeline configurations to avoid repetition.
- **`domain.*`**: Business-facing controls. `validation` binds the pipeline to
  Pandera schemas, `transform` describes flattening/serialization rules,
  `postprocess` defines hashing/QC enrichments, `fallbacks` centralizes
  resilience policies, and `sources` enumerates provider overrides (ChemBL,
  UniProt, PubChem, etc.). The optional `chembl` block captures enrichment knobs
  specific to the ChemBL adapters.
- **`infrastructure.*`**: Cross-cutting platform controls. `runtime` sets
  execution parameters, `io` governs buffering and formats, `http` consolidates
  retry/backoff policies, `cache` defines TTL-based reuse, `paths` anchors input
  and output directories, `determinism` enforces sorting/column ordering,
  `materialization` covers file formats and partitioning, `logging` binds to
  UnifiedLogger, `telemetry` wires tracing/metrics exporters, and `cli`
  surfaces operator overrides (limit, sample, dry-run, etc.).
