# 1. The ETL Pipeline Contract

## The `PipelineBase` Interface

At the core of the `bioetl` framework is the abstract base class, `PipelineBase`. Every ETL pipeline **must** inherit from this class. It establishes a clear, consistent interface and lifecycle for all data processing tasks. By enforcing this contract, the framework ensures that all pipelines are predictable, maintainable, and integrate seamlessly with the broader system.

The `PipelineBase` class defines the five critical stages of an ETL pipeline: `extract`, `transform`, `validate`, `write`, and `run`. Developers are responsible for implementing the business logic within the `extract` and `transform` stages, while the framework manages the `validate`, `write`, and `run` stages to guarantee consistency.

### Abstract Interface Definition

Below is the abstract interface that every pipeline must implement. This pseudocode illustrates the required methods and their responsibilities.

```python
# file: src/bioetl/pipelines/base.py
from abc import ABC, abstractmethod
import pandas as pd
from bioetl.config import PipelineConfig

class PipelineBase(ABC):
    """
    Abstract base class defining the contract for all ETL pipelines.
    """
    def __init__(self, config: PipelineConfig, run_id: str):
        self.config = config
        self.run_id = run_id
        # ... other framework-managed initializations for logging, clients, etc. ...

    @abstractmethod
    def extract(self) -> pd.DataFrame:
        """
        Extracts data from a source system.

        - This method MUST be implemented by the developer.
        - It is responsible for all source interaction, including API calls,
          database queries, or reading from files.
        - It must handle pagination, rate limiting, and retries as defined
          in the pipeline's YAML configuration.
        - The returned DataFrame should contain the raw, unmodified data from the source.
        """
        pass

    @abstractmethod
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transforms the raw extracted data into the target schema.

        - This method MUST be implemented by the developer.
        - It includes all business logic: cleaning, normalization, type casting,
          column renaming, and enrichment.
        - The returned DataFrame must conform to the structure expected by the
          output Pandera schema.
        """
        pass

    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Validates the transformed data against the output Pandera schema.

        - This method is managed by the framework and SHOULD NOT be overridden.
        - It enforces strict data types, column order, and custom checks.
        - Any validation failure is a critical error and immediately halts the pipeline.
        """
        # ... framework implementation ...
        pass

    def write(self, df: pd.DataFrame) -> None:
        """
        Writes the final dataset and metadata to the output directory.

        - This method is managed by the framework and SHOULD NOT be overridden.
        - It handles the physical materialization of data (e.g., to Parquet).
        - It ensures atomic file writes to prevent partial outputs.
        - It calculates and stores row and business key hashes.
        - It generates the comprehensive `meta.yaml` file.
        """
        # ... framework implementation ...
        pass

    def run(self) -> None:
        """
        Orchestrates the execution of the pipeline stages in sequence.

        - This method is managed by the framework and SHOULD NOT be overridden.
        - It ensures the strict execution order: `extract` -> `transform` -> `validate` -> `write`.
        - It manages logging, timing metrics, and final metric aggregation for the run.
        """
        # ... framework implementation ...
        pass

```

## Implementation Example

To make the contract concrete, here is a practical pseudocode example of a pipeline that fetches data from a REST API. This demonstrates how a developer would implement the required `extract` and `transform` methods.

```python
# file: src/bioetl/pipelines/my_api_pipeline.py
import pandas as pd
import requests
from bioetl.pipelines.base import PipelineBase
from bioetl.config import PipelineConfig

class MyApiPipeline(PipelineBase):
    """
    Example implementation of a pipeline for a fictional API.
    """
    def __init__(self, config: PipelineConfig, run_id: str):
        super().__init__(config, run_id)
        # It is best practice to initialize API clients in the constructor.
        self.api_client = self._setup_api_client()

    def _setup_api_client(self) -> requests.Session:
        """
        Configures a session object with retries, headers, etc.,
        based on the pipeline's YAML configuration.
        """
        session = requests.Session()
        # ... logic to add a retry strategy (e.g., from urllib3) and a
        # rate-limiting adapter based on `self.config.source` ...
        session.headers.update({"User-Agent": "bioetl/1.0"})
        return session

    def extract(self) -> pd.DataFrame:
        """
        Extracts all records from the source API, correctly handling pagination.
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

            # The framework expects the pagination logic to be handled here.
            next_page_url = data.get(cursor_key)
            # Subsequent paginated requests often use the full URL, so we clear
            # the original params to avoid conflicts.
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

        # 5. Ensure all required output columns exist before returning.
        # The `validate` stage will enforce the final order and check for nulls.
        return df
```
