# 0. Logging System Overview

## Introduction

The `bioetl` framework's logging system is built to provide structured,
deterministic, and traceable logs for all ETL and CLI operations. It is designed
to produce machine-parsable output suitable for modern observability platforms
while remaining human-readable during development.

This document provides a high-level overview of the logging architecture, its
core principles, and the goals it aims to achieve.

## Core Principles

- **Structured Output**: All log records are structured as dictionaries
  (key-value pairs), not plain text. This is the foundation for reliable
  parsing, filtering, and analysis in production environments. The system uses
  `structlog` as a frontend for Python's standard `logging` library to achieve
  this.
- **Determinism**: In production environments, log output is rendered as JSON
  with sorted keys (`sort_keys=True`). This ensures that identical events
  produce identical log lines, which is critical for golden testing and
  auditing.
- **Traceability**: Every log record is automatically enriched with a consistent
  execution context (e.g., `run_id`, `stage`). When OpenTelemetry is enabled,
  logs are also automatically correlated with `trace_id` and `span_id`,
  providing a unified view of requests across distributed services.
- **Security**: The system includes a robust secret redaction mechanism that
  automatically masks sensitive values (e.g., API keys, tokens) before they are
  written to a log, preventing accidental exposure.
- **Environment-Specific Outputs**: The system provides different output formats
  tailored to the environment: a human-readable `key=value` format for local
  development and a machine-parsable JSON format for testing and production.

## High-Level Architecture

The logging system is built on a pipeline of `structlog` processors that
progressively enrich a log event dictionary before it is rendered.

1. **`structlog` Frontend**: Application code interacts with the logger via a
   simple, unified interface: `bioetl.core.get_logger()`. This returns a
   `structlog` bound logger, which captures key-value data.
1. **Context Injection**: The first processor (`merge_contextvars`)
   automatically injects shared context (like `run_id` and `stage`) into every
   log record. This context is stored in a thread-safe `ContextVar`, making it
   compatible with both multi-threaded and asynchronous code.
1. **Enrichment Pipeline**: A series of processors adds additional, standardized
   fields:
   - `add_utc_timestamp`: Adds a UTC timestamp.
   - `add_log_level`: Adds the log level (e.g., "INFO", "ERROR").
   - `add_context_base_fields`: Adds core application context.
   - (Optional) OpenTelemetry Processor: Adds `trace_id` and `span_id`.
1. **Security Processing**:
   - `redact_secrets_processor`: Scrubs sensitive data from the event
     dictionary.
   - `logging.Filter`: A standard logging filter provides a second layer of
     defense, redacting secrets from formatted string messages.
1. **Rendering**: The final processor in the chain, the renderer, serializes the
   event dictionary into its final output format:
   - **Development Console**: `KeyValueRenderer` produces human-readable
     `key=value` lines.
   - **File/Production**: `JSONRenderer` produces one JSON object per line, with
     sorted keys to ensure deterministic output.

This architecture ensures that all log records are consistent, secure, and
enriched with valuable context, regardless of where in the application they
originate.

## Output formats: JSON vs key-value

Two renderers are bundled with the core logger configuration:

- **JSON (`LogFormat.JSON`)** – the default for CI, golden tests, and production
  runs. We rely on `JSONRenderer(sort_keys=True, ensure_ascii=False)` so the
  output is stable across executions and safe for downstream ingestion.
- **Key-Value (`LogFormat.KEY_VALUE`)** – a deterministic yet human-friendly
  development format powered by `KeyValueRenderer`. Keys appear in a fixed order
  (`timestamp`, `level`, `pipeline`, `stage`, `component`, `dataset`, `run_id`,
  `trace_id`, `span_id`, `message`), making it easy to scan log streams locally.

Switch formats by passing `LogConfig(format=LogFormat.KEY_VALUE)` to
`configure_logging`. Both renderers share the same processor pipeline, so every
record contains identical context regardless of the output
style.【F:src/bioetl/core/logger.py†L29-L121】

### Default log level

`DEFAULT_LOG_LEVEL` is `INFO`. Adjust it per run through configuration
(`LogConfig(level="DEBUG")`) or a CLI option that feeds into the `LogConfig`.
The processor stack respects the final level through `filter_by_level`, so
downstream handlers never receive filtered-out
events.【F:src/bioetl/core/logger.py†L29-L134】

## Category naming and component taxonomy

Every log is categorised with two layers:

- **`stage`** – coarse ETL lifecycle buckets: `extract`, `transform`,
  `validate`, and `write`.
- **`component`** – the precise module or service responsible for the event (for
  example `chembl_client`, `standardiser`, or `csv_writer`).

This pairing provides a stable grouping scheme for observability dashboards
while keeping the taxonomy compact enough for quick filtering. Choose verbs for
stages and snake_case nouns for components to remain consistent across the
codebase.【F:src/bioetl/core/logger.py†L40-L60】

## Correlation and execution context fields

The logger enforces a shared correlation envelope so that every log can be
traced to a single pipeline run and request:

- `run_id` – UUID representing the full pipeline execution.
- `pipeline` – canonical name of the orchestrated pipeline.
- `trace_id` / `span_id` – OpenTelemetry-compatible identifiers that correlate
  logs with distributed traces and upstream HTTP requests.
- `dataset` – dataset nickname or target table.

Bind these values once via
`UnifiedLogger.bind(run_id=..., pipeline=..., trace_id=..., span_id=...)`.
Subsequent calls to `UnifiedLogger.bind` can extend or override context for
narrower scopes (e.g., per-stage metadata).【F:src/bioetl/core/logger.py†L123-L145】

## Mandatory context data

`MANDATORY_FIELDS` enumerates the fields every event must carry. The
`_ensure_mandatory_fields` processor records missing keys in `missing_context`,
making gaps immediately visible during development. The full set is:

`run_id`, `pipeline`, `stage`, `dataset`, `component`, `trace_id`, `span_id`.

When writing new code paths, always bind these fields before emitting logs.
Optional enrichments (row counts, timings, API URLs) can be added freely but
should never replace the mandatory schema.【F:src/bioetl/core/logger.py†L33-L86】

## Example events per stage

### Extract

```json
{
  "timestamp": "2024-03-15T08:24:11.519Z",
  "level": "INFO",
  "pipeline": "chembl_molecule_sync",
  "stage": "extract",
  "component": "chembl_client",
  "dataset": "chembl_activity",
  "run_id": "9d247d44-f4d7-4f73-8a6f-2a9d06f4fe17",
  "trace_id": "03f7a8827d4c5e01",
  "span_id": "5ce9b2ae135d4307",
  "message": "Fetched batch",
  "http_status": 200,
  "records": 500
}
```

### Transform

```json
{
  "timestamp": "2024-03-15T08:24:12.041Z",
  "level": "INFO",
  "pipeline": "chembl_molecule_sync",
  "stage": "transform",
  "component": "standardiser",
  "dataset": "chembl_activity",
  "run_id": "9d247d44-f4d7-4f73-8a6f-2a9d06f4fe17",
  "trace_id": "03f7a8827d4c5e01",
  "span_id": "7420f1cd8cc0c81f",
  "message": "Normalised dose units",
  "rows_in": 500,
  "rows_out": 498,
  "dropped_records": 2
}
```

### Validate

```json
{
  "timestamp": "2024-03-15T08:24:12.317Z",
  "level": "WARNING",
  "pipeline": "chembl_molecule_sync",
  "stage": "validate",
  "component": "pandera_validator",
  "dataset": "chembl_activity",
  "run_id": "9d247d44-f4d7-4f73-8a6f-2a9d06f4fe17",
  "trace_id": "03f7a8827d4c5e01",
  "span_id": "e931dc4d7b474f5c",
  "message": "Schema violations detected",
  "invalid_rows": 3,
  "schema": "ActivitySchema",
  "action": "rows quarantined"
}
```

### Write

```json
{
  "timestamp": "2024-03-15T08:24:13.002Z",
  "level": "INFO",
  "pipeline": "chembl_molecule_sync",
  "stage": "write",
  "component": "csv_writer",
  "dataset": "chembl_activity",
  "run_id": "9d247d44-f4d7-4f73-8a6f-2a9d06f4fe17",
  "trace_id": "03f7a8827d4c5e01",
  "span_id": "adcf96c0289e41f1",
  "message": "Persisted dataset",
  "path": "data/output/chembl_activity.csv",
  "rows_written": 498,
  "duration_ms": 412
}
```

The same events rendered via `LogFormat.KEY_VALUE` collapse onto one line per
event, keeping the key ordering shown above for easy inspection during local
development.
