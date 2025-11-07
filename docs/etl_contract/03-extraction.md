# 3. The Extraction Stage

## Overview

The `extract` stage is the first active phase in the `bioetl` pipeline lifecycle. Its sole responsibility is to retrieve data from an external source and load it into a pandas DataFrame. This stage is implemented by the developer as the `extract()` method within their `PipelineBase` subclass.

A well-implemented `extract` method is critical for the pipeline's reliability and performance. It must be designed to interact with the source system respectfully and robustly, handling common challenges such as transient network errors, API rate limits, and large, paginated datasets.

## Implementing the `extract()` Method

The `extract()` method must be implemented by the developer. It takes no arguments and is expected to return a pandas DataFrame containing the raw, unprocessed data from the source.

```python
import pandas as pd
from bioetl.pipelines.base import PipelineBase

class MyPipeline(PipelineBase):
    def extract(self) -> pd.DataFrame:
        # ... implementation to fetch data ...
        # This can be from a REST API, a database, a local file, etc.
        raw_data = self.api_client.get_all_records()
        return pd.DataFrame(raw_data)

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        # ...
        pass
```

## Building a Robust API Client

For most pipelines, the `extract` stage will involve communicating with a web-based API. It is a best practice to encapsulate all API interaction logic within a dedicated client. This client should be configured to handle the following automatically:

### 1. Retries with Exponential Backoff

Network connections can be unreliable. The client **must** implement a retry mechanism to handle transient errors (e.g., HTTP 502, 503, 504). The recommended strategy is **exponential backoff with jitter**:

- **Exponential Backoff**: After a failed request, the client waits for a progressively longer interval before retrying (e.g., 1s, 2s, 4s, 8s).
- **Jitter**: A small, random amount of time is added to the backoff delay. This prevents multiple clients from retrying in lockstep (the "thundering herd" problem).

The `retries` section of the pipeline configuration should be used to configure this behavior.

```python
# Pseudocode for setting up retries with the `requests` library
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

retry_strategy = Retry(
    total=self.config.source.retries.max,
    backoff_factor=self.config.source.retries.base_seconds,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["HEAD", "GET", "OPTIONS"]
)
adapter = HTTPAdapter(max_retries=retry_strategy)
http = requests.Session()
http.mount("https://", adapter)
http.mount("http://", adapter)
```

### 2. Rate Limiting

Respecting the source system's rate limits is non-negotiable. Exceeding them can result in your IP being temporarily or permanently blocked. The client should be configured to adhere to the `rps_limit` (requests per second) defined in the configuration.

Libraries like `ratelimit` or `limits` can be used to decorate the client's request methods, automatically enforcing the desired rate.

```python
# Pseudocode for applying a rate limit
from ratelimit import limits, sleep_and_retry

@sleep_and_retry
@limits(calls=self.config.source.rps_limit, period=1) # 1 second
def make_request(url):
    return requests.get(url)
```

### 3. Pagination

APIs that return large datasets will almost always do so in "pages." The `extract` method is responsible for fetching all pages of data and combining them into a single DataFrame. The `extract.pagination` section of the configuration should define the strategy.

- **Cursor-based pagination**: The API response includes a key (e.g., `next_page_token`, `next_url`) that provides the direct token or URL for the next set of results. This is the most common and robust method.
- **Offset-based pagination**: The client must keep track of its position in the dataset (`offset`) and request subsequent pages by incrementing it (`page=2`, `page=3`, etc.).

The `extract` method must contain the logic to loop through the pages until the last page is reached.

```python
# Pseudocode for cursor-based pagination
def extract(self) -> pd.DataFrame:
    all_records = []
    next_page_url = self.config.source.endpoint
    cursor_key = self.config.extract.pagination.cursor_key

    while next_page_url:
        response = self.api_client.get(next_page_url)
        response.raise_for_status()
        data = response.json()

        all_records.extend(data.get('results', []))
        next_page_url = data.get(cursor_key)

    return pd.DataFrame(all_records)
```

By building these behaviors into a reusable client, you ensure that the `extract` method remains clean, readable, and focused on its primary task of retrieving data, while the underlying client handles the complexities of robust network communication.
