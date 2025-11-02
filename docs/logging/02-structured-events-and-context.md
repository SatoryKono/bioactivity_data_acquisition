# 2. Structured Events and Context

## The Log Event Contract

A core principle of the logging system is that every log record is a structured event. This means every log is a dictionary with a consistent set of key-value pairs. This contract ensures that logs can be reliably parsed, queried, and analyzed by automated systems.

### Mandatory Fields

Every single log record, regardless of its level or origin, is guaranteed to contain the following base fields:

| Field          | Type       | Description                                                                 |
| -------------- | ---------- | --------------------------------------------------------------------------- |
| `run_id`       | `str`      | The stable, unique identifier for the current pipeline run.                 |
| `stage`        | `str`      | The current execution stage (`extract`, `transform`, `validate`, `load`).   |
| `actor`        | `str`      | The entity that initiated the run (e.g., `scheduler`, `<username>`).        |
| `source`       | `str`      | The data source being processed (e.g., `chembl`, `pubmed`).                 |
| `generated_at` | `str(ISO)` | A UTC timestamp in ISO 8601 format.                                         |
| `level`        | `str`      | The log level (`info`, `warning`, `error`).                                 |
| `message`      | `str`      | The main, human-readable event description.                                 |

### Context-Specific Fields

Certain events, especially those related to external service calls (like API requests), must include additional context:

| Field         | Type    | Description                                                     |
| ------------- | ------- | --------------------------------------------------------------- |
| `endpoint`    | `str`   | The URL or path of the API endpoint being called.               |
| `params`      | `dict`  | (Conditional) The query parameters sent with the request.       |
| `attempt`     | `int`   | The attempt number for the request (e.g., `1` for the first try). |
| `duration_ms` | `int`   | The total duration of the call in milliseconds.                 |
| `trace_id`    | `str`   | (Conditional) The OpenTelemetry trace ID for this request.      |

## `structlog` Processor Pipeline

The consistency of the log event contract is enforced by a predefined pipeline of `structlog` processors. When a log event is created, it passes through this pipeline, where it is progressively enriched and standardized.

The order of processors is critical:

1.  **`structlog.contextvars.merge_contextvars`**: This is the first and most important processor. It merges the thread-safe `ContextVar` data (containing `run_id`, `stage`, etc.) into the event dictionary. This ensures that every log record is automatically stamped with the correct execution context.
2.  **`add_utc_timestamp`**: Adds the `generated_at` timestamp.
3.  **`add_log_level`**: Adds the `level` field.
4.  **`add_context_base_fields`**: Adds the core application context fields (`run_id`, `stage`, `actor`, `source`).
5.  **`redact_secrets_processor`**: Scrubs the event dictionary for any sensitive data before it can be rendered.
6.  **(Renderer)**: The final step, which serializes the completed event dictionary into JSON or `key=value` text.

This fixed pipeline guarantees that all safety and enrichment steps are applied in the correct order for every log event.

## Execution Context Management (`ContextVar`)

To ensure that the logging context is safe to use in multi-threaded or asynchronous environments, the framework stores it in a `contextvars.ContextVar`. This is a modern Python feature that allows data to be managed in a way that is isolated to the current execution context (e.g., the current thread or `asyncio` task).

The `set_run_context()` helper function is the designated way to manage this context. When you call it at the start of a pipeline run, it sets the `run_id`, `stage`, and other core fields for the current context. The `merge_contextvars` processor then ensures this data is automatically included in all subsequent log calls made within that context.

This design elegantly separates the **setting** of the context (which happens once per run) from the **using** of the context (which happens automatically at every log call), making the application code cleaner and free of repetitive context-passing.
