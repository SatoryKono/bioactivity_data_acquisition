# Specification: HTTP Clients, Retries, and Request Rules

> **Note**: Implementation status: **planned**. All file paths referencing `src/bioetl/` in this document describe the intended architecture and are not yet implemented in the codebase.

## 1. Overview and Goals

The `bioetl` framework relies on a unified HTTP client, `UnifiedAPIClient`, to interact with external data sources. This client provides a centralized, configurable, and resilient layer for all outgoing HTTP requests. Its implementation can be found in `[ref: repo:src/bioetl/core/api_client.py@refactoring_001]`.

The primary goals of this unified client are:
- **Centralized Configuration**: Provide a single point for setting up timeouts, retry policies, rate limits, and headers.
- **Resilience**: Automatically handle transient network errors, server-side issues (`5xx`), and rate limiting (`429`) through a robust retry mechanism with exponential backoff.
- **Predictability**: Ensure consistent behavior across all pipelines by using shared configuration profiles.

Configuration is managed through a layered system, where settings from standardized profiles are merged with pipeline-specific configs. The key profiles are:
- **`base.yaml`**: Provides foundational settings for all pipelines.
- **`network.yaml`**: Contains a standard, resilient configuration for network interactions, including timeouts and retry policies. It is recommended for pipelines that interact with external APIs.
- **`determinism.yaml`**: Provides settings to ensure reproducible, deterministic outputs, such as sort keys and hashing configurations.

These profiles are merged with pipeline-specific configs and CLI overrides to create the final configuration.

- **Reference**: [RFC 7231, Section 6.6.4: 503 Service Unavailable](https://datatracker.ietf.org/doc/html/rfc7231#section-6.6.4) (describes `Retry-After` header).

## 2. Type-Safe Configuration

All HTTP client settings are defined in the `PipelineConfig` Pydantic model (`[ref: repo:src/bioetl/config/models.py@refactoring_001]`). The newly created `configs/profiles/network.yaml` provides a standard set of these values.

**Key Configuration Fields (`APIConfig` in `api_client.py`):**

This configuration is typically defined in `configs/profiles/base.yaml` and applied to named profiles in the `http` section of a pipeline's main configuration file.

| Key | Default | Description |
|---|---|---|
| `timeout_connect` | `10.0` | Timeout in seconds for establishing a connection. |
| `timeout_read` | `30.0` | Timeout in seconds for waiting for data from the server. |
| `retry_total` | `3` | Maximum number of retry attempts. |
| `retry_backoff_factor` | `2.0` | Multiplier for the exponential backoff delay (e.g., 2.0 means delays of 1, 2, 4, 8... seconds). |
| `retry_backoff_max` | `None` | The maximum backoff delay in seconds. If `None`, there is no cap. |
| `retry_status_codes` | `[]` | A list of additional HTTP status codes that **SHOULD** trigger a retry. `5xx` codes are retried by default. |
| `rate_limit_max_calls` | `1` | Maximum number of calls allowed per `period`. |
| `rate_limit_period` | `1.0` | The time period in seconds for the rate limit. |
| `rate_limit_jitter` | `True` | Adds a small, random delay to requests to avoid thundering herd problems. |
| `cb_failure_threshold` | `5` | Number of consecutive failures before the circuit breaker opens. |
| `cb_timeout` | `60.0` | Time in seconds the circuit breaker will stay open before transitioning to half-open. |

**Merge Order:**
Configuration is merged in the following order (later items override earlier ones):

**Order of Precedence (Lowest to Highest):**

1. **Base Profiles**: Files listed in the `extends` key (e.g., `base.yaml`, `network.yaml`, `determinism.yaml`).
2. **Pipeline Config**: The main pipeline-specific YAML file provided via `--config`.
3. **CLI `--set` Flags**: Key-value pairs from the `--set` flag.
4. **Environment Variables**: Environment variables have the highest precedence (e.g., `BIOETL__HTTP__DEFAULT__TIMEOUT_SEC=120`).

## 3. Timeouts, Pools, and Headers

- **Timeouts**: The `UnifiedAPIClient` uses a tuple of `(connect_timeout, read_timeout)` for all requests, derived from the `connect_timeout_sec` and `read_timeout_sec` config fields.
- **Connection Pool**: The client is built on `requests.Session`, which automatically manages a connection pool for reusing connections to the same host, improving performance.
- **User-Agent**: A `User-Agent` header SHOULD be defined in the configuration to identify the client to the remote server.

### 3.1 Concurrency Guardrails

The `TokenBucketLimiter` protects upstream services by throttling the number of simultaneous requests. Each call to `_execute` acquires a token before a request is sent; once the configured budget is exhausted, callers block until the bucket is refilled and optionally incur a small, random jitter to desynchronise bursts.【F:src/bioetl/core/api_client.py†L325-L384】【F:src/bioetl/core/api_client.py†L1292-L1363】 The limiter is parameterised through `APIConfig.rate_limit_max_calls`, `APIConfig.rate_limit_period`, and `APIConfig.rate_limit_jitter`, which are populated from `RateLimitConfig` entries in `PipelineConfig`. The factory wiring these values enforces that each source inherits the correct `max_calls` and `period` from its HTTP profile or per-source override.【F:src/bioetl/core/client_factory.py†L51-L170】 In practice this means the maximum in-flight requests across worker threads equals `rate_limit.max_calls`, refreshed every `rate_limit.period` seconds.

## 4. Retries and Backoff

The retry logic is implemented in the `RetryPolicy` class within `api_client.py`.

**Error Classification:**
- **Retryable**: The system **MUST** retry on the following conditions:
  - Any exception that is a subclass of `requests.exceptions.RequestException` (e.g., `ConnectionError`, `Timeout`).
  - HTTP responses with a `5xx` status code (e.g., `500`, `502`, `503`, `504`).
  - Any additional status codes explicitly defined in the `retry_status_codes` configuration list (typically `429` for rate limiting).
- **Non-Retryable**: The system **MUST NOT** retry on `4xx` client errors (e.g., `400`, `401`, `403`, `404`), as these indicate a problem with the request itself that is unlikely to be resolved by retrying. The only exception is if a `4xx` code is explicitly added to `retry_status_codes`.

**Backoff Algorithm:**
The client uses an **exponential backoff with jitter** algorithm. The delay is calculated as `backoff_factor ** (attempt_number - 1)`, plus a small random jitter. This delay is capped at `backoff_max`.

If a `Retry-After` header is present in a `429` or `503` response, its value **MUST** take precedence over the calculated backoff delay.

**Interaction with Concurrency Controls:**

- A retry that follows a `Retry-After` header waits for the advised duration and then re-acquires the token bucket permit, preventing retry storms that could violate upstream quotas.【F:src/bioetl/core/api_client.py†L1292-L1363】
- Jitter is applied both when the limiter hands out tokens and when calculating exponential backoff delays, which keeps parallel workers from re-issuing retries in lockstep. This stabilises aggregate QPS under high contention.【F:src/bioetl/core/api_client.py†L325-L384】

## 5. Circuit Breaker

The `UnifiedAPIClient` includes a **circuit breaker** to protect against cascading failures. The circuit breaker has three states:

- **Closed**: Normal operation, requests pass through. Failures are tracked and counted.
- **Open**: Circuit is open after exceeding the failure threshold. Requests are immediately rejected with `CircuitBreakerOpenError` without calling the upstream service.
- **Half-Open**: After the timeout period, the circuit transitions to half-open to test if the service has recovered. A limited number of requests are allowed to pass through.

**Circuit Breaker Configuration:**

| Key | Default | Description |
|---|---|---|
| `circuit_breaker.failure_threshold` | `5` | Number of consecutive failures before the circuit breaker opens. |
| `circuit_breaker.timeout` | `60.0` | Time in seconds the circuit breaker will stay open before transitioning to half-open. |
| `circuit_breaker.half_open_max_calls` | `1` | Maximum number of calls allowed in half-open state before transitioning back to closed or open. |

**State Transitions:**

1. **Closed → Open**: When `failure_count >= failure_threshold`, the circuit opens and all subsequent requests are blocked.
2. **Open → Half-Open**: After `timeout` seconds have elapsed, the circuit transitions to half-open to allow a test request.
3. **Half-Open → Closed**: If the test request succeeds, the circuit closes and normal operation resumes.
4. **Half-Open → Open**: If the test request fails, the circuit immediately reopens.

**Error Handling:**

When the circuit breaker is open, the client raises `CircuitBreakerOpenError`, which should be caught and handled appropriately (e.g., by using fallback data or returning a graceful error response).

## 6. Quotas, Limits, and `429 Too Many Requests`

The client handles rate limiting in two ways:
1. **Proactive Rate Limiting**: The `TokenBucketLimiter` class ensures that the client does not exceed the `rate_limit.max_calls` per `rate_limit.period` defined in the configuration.
2. **Reactive Backoff**: If the server responds with a `429 Too Many Requests` status, the retry logic is triggered. The client **MUST** prioritize the `Retry-After` header from the response to determine the backoff delay.
- **Reference**: [RFC 6585, Section 4: 429 Too Many Requests](https://datatracker.ietf.org/doc/html/rfc6585#section-4)

## 7. Telemetry and Logging

The `UnifiedAPIClient` is instrumented with structured logging via the `UnifiedLogger`. The `http_log_context` automatically injects the following fields into log records related to HTTP requests, providing a rich context for debugging and monitoring.

- `endpoint`: The full URL of the request.
- `attempt`: The current retry attempt number.
- `duration_ms`: The duration of the request in milliseconds.
- `params`: The query parameters sent with the request.
- `retry_after`: The value of the `Retry-After` header, if present.
- `trace_id`: Correlates all HTTP activity back to the pipeline invocation trace (propagated through `UnifiedLogger` context extras).
- `request_id`: Stable identifier for a single HTTP call attempt, allowing downstream collectors to deduplicate retries.

### 6.1 Metrics Emitted

Operational dashboards ingest the structured events emitted by the client and derive metrics such as:

- `http.requests.total`: incremented for every attempt recorded by `_RequestRetryContext.start_attempt()`.
- `http.requests.duration_ms`: histogram sourced from the `duration_ms` field attached to the log context.
- `http.rate_limiter.wait_seconds`: timer populated whenever the token bucket forces a wait, capturing both short and long waits.【F:src/bioetl/core/api_client.py†L344-L368】
- `http.retries.total`: counter derived from `retrying_request` events and `attempt` metadata.
- `http.retry_after.seconds`: gauge summarising parsed `Retry-After` values so that alerting can react to upstream back-pressure.【F:src/bioetl/core/api_client.py†L1292-L1363】

### Example Log Record

This is an example of a structured log record for a retryable error, as it would appear in a JSON log file.

```json
{
  "event": "retrying_request",
  "level": "warning",
  "timestamp": "2024-10-28T14:30:01.123Z",
  "logger": "bioetl.core.api_client",
  "context": {
    "run_id": "activity_20241028142959",
    "stage": "extract",
    "http": {
      "endpoint": "https://www.ebi.ac.uk/chembl/api/data/activity.json",
      "attempt": 2,
      "duration_ms": 1532.45,
      "params": {
        "limit": 100,
        "offset": 200
      },
      "retry_after": 10.0
    }
  },
  "wait_seconds": 10.0,
  "sleep_seconds": 10.0,
  "error": "503 Server Error: Service Unavailable for url: ...",
  "status_code": 503,
  "retry_after": "10"
}
```

## 7. Pagination

- **Current State**: The core `UnifiedAPIClient` does not have a generic pagination handler. Pagination logic is expected to be handled by the source-specific clients that use it (e.g., a ChEMBL client).
- **Normative Standard**: Source-specific clients SHOULD implement pagination by:
  - Handling page/size or offset/limit parameters.
  - Parsing `next` links from response bodies or `Link` headers.
  - Respecting rate limits between page requests.

## 8. Response and Error Handling

- **Response Processing**: The `request_json` and `request_text` methods handle the decoding of response bodies.
- **Error Hierarchy**: The client uses standard `requests.exceptions`, primarily `HTTPError` for `4xx`/`5xx` responses and `RequestException` for other network issues. A custom `CircuitBreakerOpenError` is raised when the circuit breaker is in the open state and blocks requests.

## 9. Test Plan

- **Unit Tests**:
  - Verify that the `RetryPolicy` correctly identifies retryable vs. non-retryable status codes.
  - Verify that `parse_retry_after` correctly parses both integer and HTTP-date formats.
  - Verify the backoff calculation is correct and respects `backoff_max`.
  - Verify that a `Retry-After` value correctly overrides the calculated backoff.
- **Integration Tests**:
  - Using a mock server, simulate a `429` response with a `Retry-After` header and assert the client waits for the specified duration.
  - Simulate a `503` error and assert that the client performs the correct number of retries with exponential backoff.
  - Test the circuit breaker by sending a series of failing requests and asserting that it opens and subsequently closes.

## 10. Examples

**Creating and using a client with `network.yaml`:**

*Pseudocode:*

```python
# The CLI and config loader handle this part automatically.
# 1. Load the `network.yaml` profile.
# 2. Merge it with the pipeline's main config.
# 3. Instantiate the client using the final config.

from bioetl.core.api_client import UnifiedAPIClient, APIConfig
from bioetl.config.loader import load_config

# This is what happens inside the framework:
config = load_config("my_pipeline.yaml") # Assumes `extends: [network.yaml]`
http_profile = config.http["default"]

api_config = APIConfig(
    name="my_client",
    base_url="https://api.example.com",
    retry_total=http_profile.retries.total,
    # ... and so on for all other fields
)
client = UnifiedAPIClient(api_config)

# Making a request
try:
    # This call will automatically handle retries, rate limits, etc.
    data = client.request_json("/my-data")
except RequestException as e:
    print(f"Request failed after all retries: {e}")

```

### PipelineConfig overrides for concurrency and tracing

```yaml
version: 1
pipeline:
  name: example
  entity: example
  version: "1.0.0"
http:
  default:
    timeout_sec: 60.0
    retries:
      total: 5
      backoff_multiplier: 2.0
      backoff_max: 60.0
      statuses: [429, 500, 502, 503, 504]
    rate_limit:
      max_calls: 4
      period: 1.0
    rate_limit_jitter: true
sources:
  chembl:
    http_profile: default
  crossref:
    base_url: "https://api.crossref.org"
    rate_limit:
      max_calls: 2
      period: 1.0
    rate_limit_jitter: false
    fallback_strategies: ["cache", "network"]
```

This snippet demonstrates how a pipeline can raise or lower concurrency targets (`rate_limit.max_calls`/`period`), toggle jitter, and rely on shared HTTP profiles. When the CLI loads this configuration it is validated into `PipelineConfig`; `APIClientFactory` then materialises per-source `APIConfig` objects using the merged rate-limit and retry values so that every `UnifiedAPIClient` shares the same guardrails.【F:src/bioetl/config/models.py†L513-L527】【F:src/bioetl/core/client_factory.py†L51-L170】 Trace metadata such as `trace_id` and `request_id` are bound separately through `UnifiedLogger.set_context`, allowing downstream logs and metrics to be correlated with this configuration.

**Integration in a ChEMBL pipeline:**

A ChEMBL-specific pipeline would get a pre-configured client from a factory that uses the loaded configuration.
`[ref: repo:src/bioetl/pipelines/base.py@refactoring_001]`

```python
# Inside a method in a PipelineBase subclass:
class MyChemblPipeline(PipelineBase):
    def extract(self):
        # This context manager provides a client configured from the pipeline's config.
        with self.init_chembl_client() as client:
            # The client is an instance of UnifiedAPIClient
            activities = client.activity.all() # Makes requests under the hood
            # ...
```
