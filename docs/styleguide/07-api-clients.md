# API Clients

This document defines the standards for HTTP API clients in the `bioetl` project. All external API interactions **MUST** use the `UnifiedAPIClient` system.

## Principles

- **Unified Client**: All API calls **MUST** use `UnifiedAPIClient` for consistency.
- **Retry and Backoff**: Transient failures **MUST** be handled with exponential backoff.
- **Throttling**: Rate limiting **MUST** be enforced via token bucket with jitter.
- **Circuit Breaker**: Protection against cascading failures **MUST** be implemented.
- **Caching**: TTL-based caching **SHOULD** be used for expensive operations.
- **Timeout Policies**: Strict timeout policies **MUST** be enforced.

## UnifiedAPIClient

All external API calls **MUST** use `UnifiedAPIClient`:

```python
from bioetl.core.api_client import UnifiedAPIClient, APIConfig

# Configuration
config = APIConfig(
    name="chembl",
    base_url="https://www.ebi.ac.uk/chembl/api/data",
    timeout=30.0,
    retry_max_attempts=3,
    retry_backoff_factor=2.0,
    rate_limit_max_calls=10,
    rate_limit_period=60,
    cache_enabled=True,
    cache_ttl=3600,
)

# Create client
client = UnifiedAPIClient(config)
```

## Retry and Backoff

### Exponential Backoff

Retries **MUST** use exponential backoff with jitter:

```python
from bioetl.core.api_client import RetryPolicy

retry_policy = RetryPolicy(
    max_attempts=3,
    backoff_factor=2.0,  # Exponential: 1s, 2s, 4s
    jitter=True,  # Add randomness to prevent thundering herd
    retryable_status_codes=[429, 500, 502, 503, 504],
)
```

### Giveup Conditions

Retries **SHOULD** give up on:

- Non-retryable status codes (4xx except 429)
- Timeout errors after max attempts
- Authentication errors (401, 403)

### Valid Examples

```python
from bioetl.core.api_client import UnifiedAPIClient

client = UnifiedAPIClient(
    APIConfig(
        name="pubchem",
        base_url="https://pubchem.ncbi.nlm.nih.gov/rest/pug",
        retry_max_attempts=5,
        retry_backoff_factor=1.5,
        retry_jitter=True,
    )
)

# Automatic retry on 429/5xx
response = client.get("/compound/cid/123/json")
```

## Throttling and Rate Limiting

### Token Bucket Limiter

Rate limiting **MUST** use token bucket algorithm with jitter:

```python
from bioetl.core.api_client import APIConfig

config = APIConfig(
    name="chembl",
    rate_limit_max_calls=10,  # Tokens per period
    rate_limit_period=60,  # Seconds
    rate_limit_jitter=True,  # Add randomness
)
```

### Handling 429 Responses

When receiving `429 Too Many Requests`:

1. Respect `Retry-After` header if present
2. Use exponential backoff with jitter
3. Reduce rate limit if sustained 429s occur

### Valid Examples

```python
# Client automatically throttles requests
for i in range(100):
    response = client.get(f"/data/{i}")  # Rate-limited automatically
```

## Circuit Breaker

Circuit breaker **MUST** protect against cascading failures:

```python
from bioetl.core.api_client import CircuitBreakerConfig

circuit_breaker = CircuitBreakerConfig(
    failure_threshold=5,  # Open after 5 failures
    success_threshold=2,  # Close after 2 successes
    timeout=60,  # Seconds before half-open
)
```

### States

- **Closed**: Normal operation
- **Open**: Failures exceeded threshold; reject requests immediately
- **Half-Open**: Test recovery; allow limited requests

## Caching

### TTL Cache

Expensive API calls **SHOULD** use TTL-based caching:

```python
from bioetl.core.api_client import APIConfig

config = APIConfig(
    name="chembl",
    cache_enabled=True,
    cache_ttl=3600,  # 1 hour
    cache_maxsize=1024,  # Max cached responses
)
```

### Cache Keys

Cache keys **MUST** include:

- HTTP method
- URL path and query parameters
- Request headers (if relevant)

### Valid Examples

```python
# First call: API request
response1 = client.get("/compound/cid/123")  # API call

# Second call: cache hit
response2 = client.get("/compound/cid/123")  # Cache hit (no API call)
```

## Timeout Policies

### Strict Timeouts

All requests **MUST** have strict timeouts:

```python
from bioetl.core.api_client import APIConfig

config = APIConfig(
    name="pubchem",
    timeout=30.0,  # Connection + read timeout (seconds)
    connect_timeout=5.0,  # Connection timeout only
)
```

### Valid Examples

```python
# Request with timeout
try:
    response = client.get("/data", timeout=30.0)
except TimeoutError:
    log.error("Request timeout", url="/data")
    raise
```

## User-Agent and Headers

### User-Agent

All requests **MUST** include proper User-Agent header:

```python
from bioetl.core.api_client import APIConfig

config = APIConfig(
    name="chembl",
    headers={
        "User-Agent": "BioETL/1.0.0 (https://github.com/your-org/bioetl)",
    },
)
```

### Custom Headers

API-specific headers **SHOULD** be configured per client:

```python
config = APIConfig(
    name="pubmed",
    headers={
        "User-Agent": "BioETL/1.0.0",
        "Accept": "application/json",
        "Accept-Encoding": "gzip, deflate",
    },
)
```

## Pagination

Pagination **MUST** be handled consistently:

### Valid Examples

```python
from bioetl.core.api_client import UnifiedAPIClient

def fetch_all_pages(client: UnifiedAPIClient, endpoint: str):
    """Fetch all pages from paginated API."""
    all_items = []
    page = 1
    
    while True:
        response = client.get(endpoint, params={"page": page, "page_size": 100})
        data = response.json()
        all_items.extend(data["items"])
        
        if not data.get("has_more"):
            break
        page += 1
    
    return all_items
```

## Error Handling

### Retryable Errors

The following errors **SHOULD** trigger retries:

- `429 Too Many Requests` (with backoff)
- `500 Internal Server Error`
- `502 Bad Gateway`
- `503 Service Unavailable`
- `504 Gateway Timeout`
- Network timeouts
- Connection errors

### Non-Retryable Errors

The following errors **SHOULD NOT** trigger retries:

- `400 Bad Request`
- `401 Unauthorized`
- `403 Forbidden`
- `404 Not Found`

### Valid Examples

```python
from bioetl.core.api_client import APIError

try:
    response = client.get("/data/123")
except APIError as e:
    if e.status_code == 404:
        log.warning("Resource not found", resource_id="123")
    elif e.status_code == 429:
        log.warning("Rate limited", retry_after=e.retry_after)
    else:
        log.error("API error", status_code=e.status_code, error=str(e))
        raise
```

## Configuration Profiles

API client configuration **SHOULD** use profiles:

### network.yaml Profile

```yaml
# configs/defaults/network.yaml
api_clients:
  default:
    timeout: 30.0
    retry_max_attempts: 3
    retry_backoff_factor: 2.0
    rate_limit_max_calls: 10
    rate_limit_period: 60
    cache_enabled: true
    cache_ttl: 3600
```

## References

- HTTP clients documentation: [`docs/http/00-http-clients-and-retries.md`](../http/00-http-clients-and-retries.md)
- Pipeline API client usage: [`docs/pipelines/03-data-extraction.md`](../pipelines/03-data-extraction.md)
