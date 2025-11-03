# ETL Pipeline Contract

## 1. Introduction

This document defines the abstract contract for all ETL pipelines within the `bioetl` framework. Its purpose is to ensure that every pipeline is deterministic, reproducible, and strictly validated, providing a unified development experience.

This contract serves as the blueprint for future refactoring and for the development of all new pipelines. All implementations must adhere to the principles and interfaces outlined here.

## 2. Core Principles

- **Determinism**: The same configuration and input data must always produce a bit-for-bit identical output.
- **Reproducibility**: Pipeline runs must be repeatable, with comprehensive metadata (`meta.yaml`) capturing the full lineage of the output.
- **Strict Validation**: Data schemas must be enforced at both the input and output of the transformation stage using Pandera. Schema violations must immediately halt execution.
- **Atomicity**: The final dataset and its accompanying metadata must be written atomically. A failed run should not leave partial or corrupt artifacts.
- **Clarity**: Pipeline configuration should be declarative and self-documenting, leveraging strongly-typed Pydantic models.

## 3. Pipeline Interface (`PipelineBase`)

Every ETL pipeline must inherit from a common `PipelineBase` class. This class defines the lifecycle of a pipeline through a series of distinct, orchestrated stages.

```python
# Pseudocode illustrating the required interface
from abc import ABC, abstractmethod
import pandas as pd

class PipelineBase(ABC):
    """
    Abstract base class defining the contract for all ETL pipelines.
    """
    def __init__(self, config: PipelineConfig, run_id: str):
        self.config = config
        self.run_id = run_id
        # ... other initializations ...

    @abstractmethod
    def extract(self) -> pd.DataFrame:
        """
        Extracts data from a source system.

        - Must handle pagination, rate limiting, and retries as defined in the configuration.
        - Should be deterministic.
        """
        pass

    @abstractmethod
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transforms raw extracted data into the target schema.

        - Includes normalization, type casting, and application of business logic.
        - May involve enrichment from other sources.
        """
        pass

    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Validates the transformed data against input and output Pandera schemas.

        - This stage is managed by the base class.
        - Any validation failure is a critical error and stops the pipeline.
        - Enforces strict column order as defined in the schema.
        """
        # ... implementation in base class ...

    def write(self, df: pd.DataFrame) -> None:
        """
        Writes the final dataset and metadata to the output directory.

        - This stage is managed by the base class.
        - Ensures atomic file writes.
        - Calculates and stores row and business key hashes.
        - Generates a comprehensive meta.yaml file.
        """
        # ... implementation in base class ...

    def run(self) -> None:
        """
        Orchestrates the execution of the pipeline stages in sequence.

        - Manages logging, timing, and final metric aggregation.
        - The sequence is strictly: extract -> transform -> validate -> write.
        """
        # ... implementation in base class ...
```

### Implementation Example

Here is a more concrete pseudocode example of how a developer might implement the `extract` and `transform` methods for a pipeline that fetches data from a REST API.

```python
# Pseudocode for a pipeline implementation
import pandas as pd
import requests

class MyApiPipeline(PipelineBase):
    """
    Example implementation of a pipeline for a fictional API.
    """
    def __init__(self, config: PipelineConfig, run_id: str):
        super().__init__(config, run_id)
        # Initialize an API client based on config (e.g., with retries and rate limiting)
        self.api_client = self._setup_api_client()

    def _setup_api_client(self) -> requests.Session:
        """Configures a session object with retries, headers, etc."""
        session = requests.Session()
        # ... logic to add retry strategy and rate limiting from self.config ...
        session.headers.update({"User-Agent": "bioetl/1.0"})
        return session

    def extract(self) -> pd.DataFrame:
        """
        Extracts all records from the source API, handling pagination.
        """
        all_records = []
        params = self.config.extract.params.copy()
        next_page_url = self.config.source.endpoint
        cursor_key = self.config.extract.pagination.cursor_key

        while next_page_url:
            response = self.api_client.get(next_page_url, params=params)
            response.raise_for_status() # Fail fast on API errors
            data = response.json()

            records = data.get('results', [])
            all_records.extend(records)

            # Get the URL for the next page
            next_page_url = data.get(cursor_key)
            # Subsequent requests use the full URL, so clear params
            params = {}

        return pd.DataFrame(all_records)

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transforms the raw API data into the canonical schema.
        """
        if df.empty:
            return df

        # 1. Rename columns to match the target schema
        df = df.rename(columns={
            'compound_id': 'id',
            'molecular_weight': 'mol_weight',
            'last_updated': 'updated_at'
        })

        # 2. Apply declarative normalizers from config
        for normalizer in self.config.transform.normalizers:
            if normalizer['type'] == 'lowercase' and normalizer['field'] in df.columns:
                df[normalizer['field']] = df[normalizer['field']].str.lower()

        # 3. Coerce data types as per config
        df = df.astype(self.config.transform.dtypes)

        # 4. Handle complex transformations (e.g., date parsing)
        df['updated_at'] = pd.to_datetime(df['updated_at'], utc=True)

        # 5. Ensure all required output columns exist before returning
        # The `validate` stage will enforce the final order.
        return df
```

### Stage Semantics

- **`extract`**: Responsible for deterministic data retrieval. It must respect source quotas (RPS), pagination rules (cursor/offset), and retry policies (exponential backoff with jitter) defined in the configuration. Caching is permissible within a single run.
- **`transform`**: Applies all business logic to convert the raw data into its final, structured form. This includes data type coercion, value normalization, and enrichment.
- **`validate`**: A non-overridable stage that performs strict Pandera schema validation on the transformed data. It enforces column order and data types. A validation failure immediately terminates the pipeline before any data is written.
- **`write`**: A non-overridable stage that handles the physical materialization of the dataset (e.g., as Parquet or CSV) and its associated `meta.yaml` file. It ensures data is sorted by a stable key and computes hashes for data integrity checks.

## 4. Configuration Model

Pipeline behavior is defined entirely by a YAML configuration file. These configurations are validated by Pydantic models found in `src/bioetl/configs/models.py`.

A configuration file is composed of several key sections:

```yaml
# src/bioetl/configs/pipelines/<source>/<pipeline>.yaml
profile:
  - base.yaml
  - determinism.yaml

# Source connection and rate limiting details
source:
  endpoint: "https://my-api.com/data"
  rps_limit: 10
  retries: { max: 5, backoff: "exponential-jitter", base_seconds: 1.0 }

# Parameters for data extraction
extract:
  pagination: { type: "cursor", cursor_key: "next_page_token", page_size: 100 }
  params: { format: "json" }
  filters: { is_active: true }

# Rules for data transformation
transform:
  normalizers:
    - { field: "name", type: "trim_whitespace" }
    - { field: "status", type: "lowercase" }
  dtypes:
    id: "int64"
    value: "float64"

# Schema validation rules
validate:
  schema_in: "schemas/<source>/<pipeline>_in.py" # Optional: Schema for raw data
  schema_out: "schemas/<source>/<pipeline>_out.py" # Required: Schema for final data
  enforce_column_order: true

# Output format and determinism settings
write:
  format: "parquet"
  sort_by: ["business_key_1", "business_key_2"]
  hash_row: ["col1", "col2", "col3", "col4"] # All columns for row integrity
  hash_business_key: ["business_key_1", "business_key_2"] # Stable business identifier
  output_dir: "data/output/<pipeline>"

# Runtime execution parameters
runtime:
  parallelism: 4
  chunk_size: 1000
  log_level: "INFO"
```

## 5. Determinism and Reproducibility

To guarantee identical outputs, every pipeline must adhere to the following:

- **Stable Sorting**: The output dataset **must** be sorted by a stable key defined in `write.sort_by`.
- **Integrity Hashing**: Two hashes are computed and stored in the `meta.yaml`:
    - `hash_row`: A hash calculated from an ordered concatenation of all columns specified in `write.hash_row`. This ensures bit-for-bit integrity of the entire row.
    - `hash_business_key`: A hash calculated from the columns that form the unique business identifier, specified in `write.hash_business_key`.
- **Atomic Writes**: Output files are written to a temporary location and moved into the final destination only upon successful completion.
- **Comprehensive Metadata**: The `meta.yaml` file must accompany every dataset and contain:
    - Input parameters and configuration hash.
    - Version information (pipeline, source API, etc.).
    - Row count (`row_count`).
    - The computed `hash_row` and `hash_business_key` for the dataset.

Below is an example of a complete `meta.yaml` file:

```yaml
# meta.yaml
pipeline:
  name: "chembl_activity"
  version: "1.0.0"
  run_id: "20231027-143000-abcdef"
  config_hash: "sha256:abcde12345..."

source:
  system: "ChEMBL API"
  version: "ChEMBL_33"
  endpoint: "https://www.ebi.ac.uk/chembl/api/data/activity"
  request_params:
    format: "json"
    pchembl_value__isnull: false

execution:
  start_time_utc: "2023-10-27T14:30:00.123Z"
  end_time_utc: "2023-10-27T14:35:10.456Z"
  duration_seconds: 310.333
  stage_durations_ms:
    extract: 180123.45
    transform: 60234.56
    validate: 15123.78
    write: 54851.21

output:
  row_count: 123456
  dataset_format: "parquet"
  determinism:
    sort_by: ["assay_id", "activity_id"]
    hash_policy_version: "1"
  hashes:
    business_key: "sha256:fedcba9876..."
    row: "sha256:98765fedcba..."
```

## 6. CLI Integration

Pipelines are exposed as commands through a Typer-based CLI.

- **`list`**: The `python -m bioetl.cli.main list` command must automatically discover and display all registered pipelines.
- **`<pipeline>` command**: Each pipeline must be runnable as a subcommand, e.g., `python -m bioetl.cli.main <pipeline>`.
- **Standard Arguments**: The CLI must support standard arguments for overriding key configuration parameters:
    - `--config`: Path to the YAML configuration file.
    - `--output-dir`: The destination for the output artifacts.
    - `--dry-run`: Executes the pipeline up to the `validate` stage but does not write any files. This is used to verify configuration and source connectivity.
