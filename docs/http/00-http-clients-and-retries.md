# Specification: HTTP Clients, Retries, and Request Rules

## 1. Overview and Goals

The `bioetl` framework relies on a unified HTTP client, `UnifiedAPIClient`, to interact with external data sources. This client provides a centralized, configurable, and resilient layer for all outgoing HTTP requests. Its implementation can be found in `[ref: repo:src/bioetl/core/api_client.py@refactoring_001]`.

The primary goals of this unified client are:
-   **Centralized Configuration**: Provide a single point for setting up timeouts, retry policies, rate limits, and headers.
-   **Resilience**: Automatically handle transient network errors, server-side issues (`5xx`), and rate limiting (`429`) through a robust retry mechanism with exponential backoff.
-   **Predictability**: Ensure consistent behavior across all pipelines by using shared configuration profiles.

Configuration is managed through a layered system, where settings from profiles like `base.yaml` and the new `network.yaml` are merged with pipeline-specific configs and CLI overrides.

-   **Reference**: [RFC 7231, Section 6.6.4: 503 Service Unavailable](https://datatracker.ietf.org/doc/html/rfc7231#section-6.6.4) (describes `Retry-After` header).

## 2. Type-Safe Configuration

All HTTP client settings are defined in the `PipelineConfig` Pydantic model (`[ref: repo:src/bioetl/config/models.py@refactoring_001]`). The newly created `configs/profiles/network.yaml` provides a standard set of these values.

**Key Configuration Fields (`HttpConfig` and `RetryConfig`):**

| Key | Type | Description |
|---|---|---|
| `timeout_sec` | `float` | Total request timeout. Overridden by `connect` and `read` if set. |
| `connect_timeout_sec` | `float` | Timeout for establishing a connection. |
| `read_timeout_sec` | `float` | Timeout for waiting for data from the server. |
| `retries.total` | `int` | Maximum number of retry attempts. |
| `retries.backoff_multiplier` | `float` | The multiplier for the exponential backoff delay. |
| `retries.backoff_max` | `float` | The maximum backoff delay in seconds. |
| `retries.statuses` | `list[int]` | A list of HTTP status codes that should trigger a retry. |
| `rate_limit.max_calls` | `int` | Maximum number of calls allowed per `period`. |
| `rate_limit.period` | `float` | The time period in seconds for the rate limit. |
| `headers` | `dict` | A dictionary of default headers to send with each request. |

**Merge Order:**
Configuration is merged in the following order (later items override earlier ones):
1.  `base.yaml`
2.  `network.yaml` / `determinism.yaml` (if extended)
3.  Pipeline-specific `--config` file
4.  CLI `--set` flags
5.  Environment variables (e.g., `BIOETL__HTTP__DEFAULT__TIMEOUT_SEC=120`)

## 3. Timeouts, Pools, and Headers

-   **Timeouts**: The `UnifiedAPIClient` uses a tuple of `(connect_timeout, read_timeout)` for all requests, derived from the `connect_timeout_sec` and `read_timeout_sec` config fields.
-   **Connection Pool**: The client is built on `requests.Session`, which automatically manages a connection pool for reusing connections to the same host, improving performance.
-   **User-Agent**: A `User-Agent` header SHOULD be defined in the configuration to identify the client to the remote server.

## 4. Retries and Backoff

The retry logic is implemented in the `RetryPolicy` class within `api_client.py`.

**Error Classification:**
-   **Retryable**: The system MUST retry on `requests.exceptions.RequestException` (which covers network timeouts and DNS/TLS errors) and specific HTTP status codes. The current implementation retries on:
    -   `5xx` server errors (by default).
    -   Any status code listed in the `retries.statuses` configuration list (e.g., `429`, `408`).
-   **Non-Retryable**: The system MUST NOT retry on `4xx` client errors (except those explicitly in the retryable list), as these indicate a problem with the request itself.

**Backoff Algorithm:**
The client uses an **exponential backoff** algorithm. The delay is calculated as `backoff_multiplier ** attempt_number`, capped at `backoff_max`.
Crucially, if a `Retry-After` header is present in a `429` or `503` response, the client's `parse_retry_after` function will parse it, and this value **MUST** take precedence over the calculated backoff delay.

**Idempotency:**
-   **Current State**: The `UnifiedAPIClient` does not currently implement a mechanism for ensuring the idempotency of `POST` or `PATCH` requests, such as sending an `Idempotency-Key` header.
-   **Normative Standard**: Safe methods (`GET`, `HEAD`, `OPTIONS`) are inherently idempotent and SHOULD always be retried. Unsafe methods (`POST`, `PATCH`, `DELETE`) MUST NOT be retried unless an idempotency mechanism is implemented. If such a mechanism is added, it SHOULD follow the IETF draft standard for the `Idempotency-Key` header.
-   **Reference**: [IETF Draft: The Idempotency-Key HTTP Header Field](https://datatracker.ietf.org/doc/html/draft-ietf-httpapi-idempotency-key-header)

## 5. Quotas, Limits, and `429 Too Many Requests`

The client handles rate limiting in two ways:
1.  **Proactive Rate Limiting**: The `TokenBucketLimiter` class ensures that the client does not exceed the `rate_limit.max_calls` per `rate_limit.period` defined in the configuration.
2.  **Reactive Backoff**: If the server responds with a `429 Too Many Requests` status, the retry logic is triggered. The client MUST prioritize the `Retry-After` header from the response to determine the backoff delay.
-   **Reference**: [RFC 6585, Section 4: 429 Too Many Requests](https://datatracker.ietf.org/doc/html/rfc6585#section-4)

## 6. Telemetry and Logging

-   **Current State**: The `UnifiedAPIClient` is instrumented with structured logging via the `UnifiedLogger`. It logs retry attempts, backoff delays, and circuit breaker state changes. There is **no current implementation** of distributed tracing (W3C Trace Context) or metrics (OpenTelemetry).
-   **Normative Standard**:
    -   **Tracing**: If tracing is implemented, the client MUST propagate the `traceparent` and `tracestate` headers. It SHOULD create a client `span` for each outgoing request with the semantic attributes defined by OpenTelemetry, such as `http.method`, `http.url`, `http.status_code`, and `net.peer.name`.
    -   **Metrics**: If metrics are implemented, the client SHOULD record the following:
        -   `http.client.duration` (histogram)
        -   `http.client.active_requests` (up/down counter)
        -   `http.client.request.retries` (counter)
-   **References**:
    -   [W3C Trace Context](https://www.w3.org/TR/trace-context/)
    -   [OpenTelemetry: HTTP Client Semantic Conventions](https://opentelemetry.io/docs/specs/semconv/http/http-spans/#http-client)

## 7. Pagination

-   **Current State**: The core `UnifiedAPIClient` does not have a generic pagination handler. Pagination logic is expected to be handled by the source-specific clients that use it (e.g., a ChEMBL client).
-   **Normative Standard**: Source-specific clients SHOULD implement pagination by:
    -   Handling page/size or offset/limit parameters.
    -   Parsing `next` links from response bodies or `Link` headers.
    -   Respecting rate limits between page requests.

## 8. Response and Error Handling

-   **Response Processing**: The `request_json` and `request_text` methods handle the decoding of response bodies.
-   **Error Hierarchy**: The client uses standard `requests.exceptions`, primarily `HTTPError` for `4xx`/`5xx` responses and `RequestException` for other network issues. A custom `CircuitBreakerOpenError` is raised if the circuit breaker is open.

## 9. Test Plan

-   **Unit Tests**:
    -   Verify that the `RetryPolicy` correctly identifies retryable vs. non-retryable status codes.
    -   Verify that `parse_retry_after` correctly parses both integer and HTTP-date formats.
    -   Verify the backoff calculation is correct and respects `backoff_max`.
    -   Verify that a `Retry-After` value correctly overrides the calculated backoff.
-   **Integration Tests**:
    -   Using a mock server, simulate a `429` response with a `Retry-After` header and assert the client waits for the specified duration.
    -   Simulate a `503` error and assert that the client performs the correct number of retries with exponential backoff.
    -   Test the circuit breaker by sending a series of failing requests and asserting that it opens and subsequently closes.

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
