# 0. Logging System Overview

## Introduction

The `bioetl` framework's logging system is built to provide structured, deterministic, and traceable logs for all ETL and CLI operations. It is designed to produce machine-parsable output suitable for modern observability platforms while remaining human-readable during development.

This document provides a high-level overview of the logging architecture, its core principles, and the goals it aims to achieve.

## Core Principles

-   **Structured Output**: All log records are structured as dictionaries (key-value pairs), not plain text. This is the foundation for reliable parsing, filtering, and analysis in production environments. The system uses `structlog` as a frontend for Python's standard `logging` library to achieve this.
-   **Determinism**: In production environments, log output is rendered as JSON with sorted keys (`sort_keys=True`). This ensures that identical events produce identical log lines, which is critical for golden testing and auditing.
-   **Traceability**: Every log record is automatically enriched with a consistent execution context (e.g., `run_id`, `stage`). When OpenTelemetry is enabled, logs are also automatically correlated with `trace_id` and `span_id`, providing a unified view of requests across distributed services.
-   **Security**: The system includes a robust secret redaction mechanism that automatically masks sensitive values (e.g., API keys, tokens) before they are written to a log, preventing accidental exposure.
-   **Environment-Specific Outputs**: The system provides different output formats tailored to the environment: a human-readable `key=value` format for local development and a machine-parsable JSON format for testing and production.

## High-Level Architecture

The logging system is built on a pipeline of `structlog` processors that progressively enrich a log event dictionary before it is rendered.

1.  **`structlog` Frontend**: Application code interacts with the logger via a simple, unified interface: `UnifiedLogger.get()`. This returns a `structlog` bound logger, which captures key-value data.
2.  **Context Injection**: The first processor (`merge_contextvars`) automatically injects shared context (like `run_id` and `stage`) into every log record. This context is stored in a thread-safe `ContextVar`, making it compatible with both multi-threaded and asynchronous code.
3.  **Enrichment Pipeline**: A series of processors adds additional, standardized fields:
    -   `add_utc_timestamp`: Adds a UTC timestamp.
    -   `add_log_level`: Adds the log level (e.g., "INFO", "ERROR").
    -   `add_context_base_fields`: Adds core application context.
    -   (Optional) OpenTelemetry Processor: Adds `trace_id` and `span_id`.
4.  **Security Processing**:
    -   `redact_secrets_processor`: Scrubs sensitive data from the event dictionary.
    -   `logging.Filter`: A standard logging filter provides a second layer of defense, redacting secrets from formatted string messages.
5.  **Rendering**: The final processor in the chain, the renderer, serializes the event dictionary into its final output format:
    -   **Development Console**: `KeyValueRenderer` produces human-readable `key=value` lines.
    -   **File/Production**: `JSONRenderer` produces one JSON object per line, with sorted keys to ensure deterministic output.

This architecture ensures that all log records are consistent, secure, and enriched with valuable context, regardless of where in the application they originate.
