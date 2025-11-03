# 1. Public API and Configuration

## Overview

The `UnifiedLogger` provides a simple and consistent public API for all logging operations. Its behavior is controlled by a strongly-typed `LoggerConfig` dataclass, which allows for declarative configuration of log levels, formats, and outputs.

## Public API

The `UnifiedLogger` class exposes three primary static methods for application-wide use.

### `UnifiedLogger.configure(config: LoggerConfig, ...)`

This method initializes the global logging system. It **must be called once** at the application's entry point (e.g., in the main CLI function) before any logging occurs.

-   **`config: LoggerConfig`**: An instance of the `LoggerConfig` dataclass containing all the configuration settings for the logging system.
-   **`additional_processors: list[Processor] | None = None`**: An optional list of custom `structlog` processors to be added to the end of the processing chain, just before the renderer.

### `UnifiedLogger.get(name: str | None = None)`

This method returns a `structlog.BoundLogger` instance that is ready for use. It is the standard way to get a logger within any module.

-   **`name: str | None = None`**: The name of the logger. By convention, this should be `__name__`, which will associate the logger with the module's hierarchy (e.g., `bioetl.pipelines.chembl.activity`).

### `set_run_context(...)`

This is a standalone helper function that sets the global, thread-safe context for a pipeline run. It should be called at the beginning of a pipeline execution. All log records emitted after this call will be automatically enriched with this context.

-   **`run_id: str`**: A stable, unique identifier for the pipeline run.
-   **`stage: str`**: The name of the current execution stage (e.g., `extract`, `transform`).
-   **`actor: str`**: The entity initiating the run (e.g., `scheduler`, `username`).
-   **`source: str`**: The data source being processed (e.g., `chembl`, `pubmed`).
-   **`trace_id: str | None = None`**: (Optional) The OpenTelemetry trace ID, if available.
-   **`generated_at: str | None = None`**: (Optional) An ISO 8601 timestamp. If not provided, a UTC timestamp is generated automatically.

## Configuration (`LoggerConfig`)

The `LoggerConfig` dataclass provides a declarative way to control the entire logging system. An instance of this class is passed to `UnifiedLogger.configure()`.

```python
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

@dataclass
class LoggerConfig:
    level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"
    console_format: Literal["text", "json"] = "text"
    file_enabled: bool = True
    file_path: Path = Path("var/logs/app.log")
    file_format: Literal["json"] = "json"
    max_bytes: int = 10 * 1024 * 1024
    backup_count: int = 10
    telemetry_enabled: bool = False
    redact_secrets: bool = True
    json_sort_keys: bool = True
    json_ensure_ascii: bool = False
```

### Configuration Details

| Parameter             | Type                               | Default                                | Description                                                                                                                              |
| --------------------- | ---------------------------------- | -------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------- |
| `level`               | `str`                              | `"INFO"`                               | The minimum log level to be processed. Messages below this level will be discarded.                                                      |
| `console_format`      | `"text"` or `"json"`                 | `"text"`                               | The output format for logs sent to the console (stdout/stderr). Use `"text"` for development and `"json"` for containerized environments. |
| `file_enabled`        | `bool`                             | `True`                                 | If `True`, logs will be written to a file in addition to the console.                                                                    |
| `file_path`           | `Path`                             | `"var/logs/app.log"`                   | The path to the log file.                                                                                                                |
| `file_format`         | `"json"`                           | `"json"`                               | The output format for the log file. Must be `"json"` for machine-parsable output.                                                        |
| `max_bytes`           | `int`                              | `10485760` (10 MB)                     | The maximum size of a log file before it is rotated.                                                                                     |
| `backup_count`        | `int`                              | `10`                                   | The number of old log files to keep after rotation.                                                                                      |
| `telemetry_enabled`   | `bool`                             | `False`                                | If `True`, enables the OpenTelemetry processor to inject `trace_id` and `span_id` into log records.                                        |
| `redact_secrets`      | `bool`                             | `True`                                 | If `True`, enables the secret redaction processor to mask sensitive data.                                                                |
| `json_sort_keys`      | `bool`                             | `True`                                 | If `True`, the JSON renderer will sort keys in the output. This is **critical for deterministic logs**.                                  |
| `json_ensure_ascii`   | `bool`                             | `False`                                | If `False`, the JSON renderer will correctly serialize Unicode characters (e.g., Cyrillic) instead of escaping them.                   |
