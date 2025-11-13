# 2. Structured Events and Context

## The Log Event Contract

A core principle of the logging system is that every log record is a structured
event. This means every log is a dictionary with a consistent set of key-value
pairs. This contract ensures that logs can be reliably parsed, queried, and
analyzed by automated systems.

### Mandatory Fields

Every single log record, regardless of its level or origin, is guaranteed to
contain the following base fields:

| Field       | Type  | Description                                                          |
| ----------- | ----- | -------------------------------------------------------------------- |
| `run_id`    | `str` | Stable identifier of the current run (UUID or deterministic tag).    |
| `pipeline`  | `str` | Canonical pipeline code, e.g. `chembl_activity`.                     |
| `stage`     | `str` | Execution stage (`extract`, `transform`, `validate`, `write`, ...).  |
| `dataset`   | `str` | Primary dataset or logical entity name for the record.               |
| `component` | `str` | Logical component emitting the event (CLI, pipeline module, client). |
| `trace_id`  | `str` | Deterministic 32-character trace identifier for distributed tracing. |
| `span_id`   | `str` | Deterministic 16-character span identifier nested under `trace_id`.  |

The processor chain enriches every record with `timestamp` in UTC ISO-8601
format and with `level`/`message` fields. When any of the mandatory fields is
missing, the event carries an extra `missing_context` array that lists the
absent keys; consumers **must** treat that as a contract violation in production
pipelines.

### Event Identifiers

All event identifiers are sourced from `bioetl.core.logging.log_events.LogEvents` and
follow the deterministic naming pattern `namespace.action.suffix`. Dots separate
the hierarchy, and only lowercase ASCII, digits, `_` and `-` are permitted.
Dedicated helper factories `stage_event()` and `client_event()` produce
compliant identifiers for dynamic situations.

### Context-Specific Fields

Certain events, especially those related to external service calls (like API
requests), must include additional context:

| Field         | Type   | Description                                                       |
| ------------- | ------ | ----------------------------------------------------------------- |
| `endpoint`    | `str`  | The URL or path of the API endpoint being called.                 |
| `params`      | `dict` | (Conditional) The query parameters sent with the request.         |
| `attempt`     | `int`  | The attempt number for the request (e.g., `1` for the first try). |
| `duration_ms` | `int`  | The total duration of the call in milliseconds.                   |
| `trace_id`    | `str`  | (Conditional) The OpenTelemetry trace ID for this request.        |

## `structlog` Processor Pipeline

The consistency of the log event contract is enforced by a predefined pipeline
of `structlog` processors. When a log event is created, it passes through this
pipeline, where it is progressively enriched and standardized.

The order of processors is critical:

1. **`structlog.contextvars.merge_contextvars`**: This is the first and most
   important processor. It merges the thread-safe `ContextVar` data (containing
   `run_id`, `stage`, etc.) into the event dictionary. This ensures that every
   log record is automatically stamped with the correct execution context.
1. **`structlog.processors.TimeStamper`**: Adds the `timestamp` field in UTC.
1. **`structlog.stdlib.add_log_level`**: Adds the `level` field.
1. **`_ensure_mandatory_fields`**: Validates presence of mandatory keys and
   populates `missing_context` when necessary.
1. **`structlog.processors.EventRenamer("message")`**: Normalises the event name
   under the `message` key.
1. **`_redact_sensitive_values`**: Scrubs the event dictionary for any sensitive
   data before it can be rendered.
1. **(Renderer)**: The final step, which serializes the completed event
   dictionary into JSON or `key=value` text.

This fixed pipeline guarantees that all safety and enrichment steps are applied
in the correct order for every log event.

## Execution Context Management (`ContextVar`)

To ensure that the logging context is safe to use in multi-threaded or
asynchronous environments, the framework stores it in a
`contextvars.ContextVar`. This is a modern Python feature that allows data to be
managed in a way that is isolated to the current execution context (e.g., the
current thread or `asyncio` task).

The `UnifiedLogger.bind()` helper (or pipeline-specific shortcuts such as
`UnifiedLogger.stage()`) are the designated ways to manage this context. When
you call them at the start of a pipeline run, they set the `run_id`, `pipeline`,
`stage`, and other core fields for the current context. The `merge_contextvars`
processor then ensures this data is automatically included in all subsequent log
calls made within that context.

This design elegantly separates the **setting** of the context (which happens
once per run) from the **using** of the context (which happens automatically at
every log call), making the application code cleaner and free of repetitive
context-passing.
