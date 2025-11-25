# ETL Pipeline Architecture

This document defines the architectural standards for ETL pipelines in the
`bioetl` project. All pipelines **MUST** follow these standards for consistency
and maintainability.

## Principles

- **One Source, One Pipeline**: Each data source **MUST** have exactly one
  public pipeline.
- **Unified Components**: All pipelines **MUST** use unified components (Logger,
  Writer, Client, Schema).
- **Star Schema**: Data **SHOULD** be organized in star schema (dims + fact
  tables).
- **Adapter Pattern**: External sources **MUST** integrate via adapters.
- **Pipeline Contract**: All pipelines **MUST** follow the standard pipeline
  contract.

## Core Package Layout

The `bioetl.core` package is the stable public surface for pipeline
infrastructure. Its internal structure now follows the directory patterns from
`docs/styleguide/11-naming-policy.md`:

- `core/http` — HTTP adapters (`UnifiedAPIClient`, `APIClientFactory`,
  rate limiting, circuit breaker).
- `core/logging` — structured logging (`UnifiedLogger`, `LogEvents`,
  event helpers).
- `core/io` — deterministic output utilities (hashing, serialization,
  atomic writers, QC units).
- `core/schema` — schema factories, normalisers, and validation helpers.
- `core/runtime` — CLI and runtime primitives (`CliCommandBase`,
  `BioETLError`, compatibility shims).
- `core/utils` — shared domain helpers (vocabulary store access,
  molecule joins).

`bioetl.core.__init__` re-exports the supported symbols from these
subpackages so that imports such as `from bioetl.core import UnifiedLogger`
continue to work. The historical compatibility stubs (`bioetl.core.api_client`,
`bioetl.core.logger`, `bioetl.core.log_events`, etc.) were removed in Q4 2025;
new code **MUST** import either from `bioetl.core` (preferred for public API) or
from the canonical subpackages listed above.

## One Source, One Pipeline

Each external data source **MUST** have exactly one public pipeline:

- **ChEMBL**: `ChEMBLActivityPipeline`, `ChEMBLAssayPipeline`, etc.
- **PubChem**: `PubChemTestItemPipeline`
- **UniProt**: `UniProtTargetPipeline`
- **PubMed**: `PubMedDocumentPipeline`

### Valid Examples — Star schema

```python
from bioetl.core.pipeline import PipelineBase


class ChEMBLActivityPipeline(PipelineBase):
    """Public pipeline for ChEMBL activity data extraction."""

    def extract(self) -> pd.DataFrame:
        """Extract activity data from ChEMBL."""
        # Implementation
        pass
```

### Invalid Examples

```python
# Invalid: multiple pipelines for same source
class ChEMBLActivityPipeline1(PipelineBase):  # SHALL NOT create multiple
    pass


class ChEMBLActivityPipeline2(PipelineBase):  # SHALL NOT create multiple
    pass
```

## Pipeline Naming Convention

All pipelines **MUST** follow a consistent naming convention that reflects the
entity being extracted and the data source.

### Pipeline Name Format (for code/configs)

Pipeline names **MUST** use the format: `{entity}_{source}`

- **Entity** comes first (what is being extracted): `document`, `testitem`,
  `target`, `assay`, `activity`
- **Source** comes second (where data comes from): `chembl`, `pubchem`,
  `pubmed`, `crossref`, `openalex`, `semantic_scholar`, `uniprot`, `iuphar`

**Valid Examples:**

- `document_chembl` - document extraction from ChEMBL
- `testitem_pubchem` - testitem extraction from PubChem
- `assay_chembl` - assay extraction from ChEMBL
- `activity_chembl` - activity extraction from ChEMBL
- `target_chembl` - target extraction from ChEMBL
- `target_uniprot` - target extraction from UniProt
- `target_iuphar` - target extraction from IUPHAR

**Invalid Examples:**

- `pubmed_document` ❌ (should be `document_pubmed`)
- `openalex_document` ❌ (should be `document_openalex`)
- `crossref_document` ❌ (should be `document_crossref`)
- `semantic_scholar_document` ❌ (should be `document_semantic_scholar`)

### Documentation File Name Format

Pipeline documentation files **MUST** use the format:
`NN-{entity}-{source}-extraction.md`

Where `NN` is a two-digit number for ordering, and the entity-source pattern
matches the pipeline name.

**Valid Examples:**

- `05-assay-chembl-extraction.md`
- `06-activity-chembl-extraction.md`
- `07-testitem-chembl-extraction.md`
- `08-target-chembl-extraction.md`
- `09-document-chembl-extraction.md`
- `21-testitem-pubchem-extraction.md`

## Unified Components

All pipelines **MUST** use unified components:

### UnifiedLogger

```python
from bioetl.core.logging import UnifiedLogger

log = UnifiedLogger.get(__name__)


class MyPipeline(PipelineBase):
    def extract(self):
        log.info("Extraction started", source="chembl")
```

### UnifiedOutputWriter

```python
from bioetl.core.output_writer import UnifiedOutputWriter


class MyPipeline(PipelineBase):
    def export(self, df: pd.DataFrame):
        writer = UnifiedOutputWriter(self.config.output_dir)
        writer.write(df, schema=ActivitySchema, metadata=self.metadata)
```

### UnifiedAPIClient

```python
from bioetl.core.http.api_client import UnifiedAPIClient


class MyPipeline(PipelineBase):
    def __init__(self, config):
        super().__init__(config)
        self.client = UnifiedAPIClient(self.config.api_config)
```

### UnifiedSchema

```python
from bioetl.schemas import ActivitySchema


class MyPipeline(PipelineBase):
    def validate(self, df: pd.DataFrame):
        return ActivitySchema.validate(df)
```

## Star Schema Design

Data **SHOULD** be organized in star schema:

### Dimension Tables

- `documents_dim`: Document metadata
- `targets_dim`: Target/protein information
- `assays_dim`: Assay definitions
- `testitems_dim`: Test item/compound information

### Fact Table

- `activity_fact`: Central activity measurements with foreign keys to dimensions

### Valid Examples — Adapter pattern

```python
# Dimension table
documents_dim = pd.DataFrame(
    {
        "document_id": ["DOC001", "DOC002"],
        "title": ["Title 1", "Title 2"],
        "doi": ["10.1234/example", "10.5678/example"],
    }
)

# Fact table with foreign keys
activity_fact = pd.DataFrame(
    {
        "activity_id": ["ACT001", "ACT002"],
        "document_id": ["DOC001", "DOC002"],  # FK to documents_dim
        "target_id": ["TGT001", "TGT002"],  # FK to targets_dim
        "value": [1.0, 2.0],
    }
)
```

## Adapter Pattern

External sources **MUST** integrate via adapters:

### Adapter Structure

```python
from abc import ABC, abstractmethod


class SourceAdapter(ABC):
    """Base adapter for external data sources."""

    @abstractmethod
    def fetch_data(self, params: dict) -> pd.DataFrame:
        """Fetch data from external source."""
        pass

    @abstractmethod
    def normalize(self, raw_data: pd.DataFrame) -> pd.DataFrame:
        """Normalize data to project schema."""
        pass
```

### Valid Examples — Pipeline contract

```python
class ChEMBLAdapter(SourceAdapter):
    """Adapter for ChEMBL API."""

    def __init__(self, client: UnifiedAPIClient):
        self.client = client

    def fetch_data(self, params: dict) -> pd.DataFrame:
        """Fetch activity data from ChEMBL API."""
        response = self.client.get("/activity.json", params=params)
        return pd.DataFrame(response.json()["activities"])

    def normalize(self, raw_data: pd.DataFrame) -> pd.DataFrame:
        """Normalize ChEMBL data to project schema."""
        # Normalization logic
        return normalized_df
```

## Pipeline Contract

All pipelines **MUST** follow the standard contract:

### Stages

1. **extract**: Fetch raw data from source
1. **transform**: Normalize and enrich data
1. **validate**: Validate against schema
1. **export**: Write to output files

### Valid Examples — Merge strategies

```python
from bioetl.core.pipeline import PipelineBase


class ActivityPipeline(PipelineBase):
    """Activity data pipeline following standard contract."""

    def extract(self) -> pd.DataFrame:
        """Extract raw activity data."""
        log.info("Extracting activity data", source=self.config.source)
        raw_data = self.adapter.fetch_data(self.config.extract_params)
        return raw_data

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform and normalize data."""
        log.info("Transforming data", rows=len(df))
        normalized = self.adapter.normalize(df)
        enriched = self.enrich_data(normalized)
        return enriched

    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate data against schema."""
        log.info("Validating data", rows=len(df))
        validated = ActivitySchema.validate(df)
        return validated

    def export(self, df: pd.DataFrame) -> None:
        """Export validated data."""
        log.info("Exporting data", rows=len(df))
        self.output_writer.write(df, schema=ActivitySchema)
```

## Merge Strategies

When merging data from multiple sources:

### Valid Examples

```python
def merge_activities(
    primary: pd.DataFrame, secondary: pd.DataFrame, merge_keys: list[str]
) -> pd.DataFrame:
    """Merge activity data with conflict resolution."""
    merged = pd.merge(
        primary,
        secondary,
        on=merge_keys,
        how="outer",
        suffixes=("_primary", "_secondary"),
    )

    # Conflict resolution: prefer primary source
    merged["value"] = merged["value_primary"].fillna(merged["value_secondary"])

    return merged
```

## Pipeline Configuration

Pipeline configuration **MUST** follow the standard structure:

```yaml
# configs/pipelines/activity.yaml
pipeline:
  name: "activity"
  source: "chembl"
  version: "1.0.0"

extract:
  endpoint: "/activity.json"
  params:
    limit: 1000

transform:
  normalizers:
    - "chembl_id"
    - "activity_value"

validate:
  schema: "ActivitySchema"
  column_order: ["activity_id", "assay_id", "value", "unit"]

export:
  format: "csv"
  sort_by: ["activity_id"]
  output_dir: "data/output/activity"
```

## Error Handling

Pipelines **MUST** handle errors gracefully:

```python
class ActivityPipeline(PipelineBase):
    def extract(self) -> pd.DataFrame:
        try:
            return self.adapter.fetch_data(self.config.extract_params)
        except APIError as e:
            log.error("Extraction failed", error=str(e), source=self.config.source)
            raise PipelineError(f"Failed to extract data: {e}") from e
```

## Idempotency

Pipelines **SHOULD** be idempotent:

- Running the same pipeline with the same config produces identical output
- Re-running a completed pipeline should not duplicate data
- Use deterministic sorting and hashing for idempotency

## References

- Pipeline base:
  [`docs/pipelines/00-pipeline-base.md`](../pipelines/00-pipeline-base.md)
- ETL contract: [`docs/etl_contract/`](../etl_contract/)
- Sources architecture:
  [`docs/sources/00-sources-architecture.md`](../sources/00-sources-architecture.md)
