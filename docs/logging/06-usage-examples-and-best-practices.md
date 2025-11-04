# 6. Usage Examples and Best Practices

> **Note**: Implementation status: **planned**. All file paths referencing `src/bioetl/` in this document describe the intended architecture and are not yet implemented in the codebase.

This document provides a set of practical, copy-paste-ready examples for using the `UnifiedLogger` in your application.

## 1. Initialization (Entry Point)

The logger **must** be configured once at the very beginning of your application's lifecycle. A good place for this is the main function of your CLI application.

```python
# file: src/bioetl/cli/main.py

from pathlib import Path
from bioetl.core.logger import UnifiedLogger, LoggerConfig

def main():
    # 1. Create a configuration object.
    #    In a real application, you might load this from a YAML file or environment variables.
    config = LoggerConfig(
        level="INFO",
        console_format="text", # Use "text" for local dev, "json" for prod
        file_enabled=True,
        file_path=Path("var/logs/pipeline_run.log"),
        telemetry_enabled=False # Disable for local dev to reduce noise
    )

    # 2. Configure the logger. This should only be called once.
    UnifiedLogger.configure(config)

    # ... rest of your application logic ...
```

## 2. Getting a Logger Instance

To log from any module in your application, get a logger instance at the module level. By convention, you should always pass `__name__`.

```python
# file: src/bioetl/pipelines/my_pipeline.py

from bioetl.core.logger import UnifiedLogger

# Get the logger instance at the module level
log = UnifiedLogger.get(__name__)

class MyPipeline:
    def run(self):
        log.info("Pipeline started.")
        # ...
```

## 3. Setting the Run Context

At the start of a pipeline run, you must set the execution context. This will ensure all subsequent logs are automatically enriched with the correct `run_id`, `stage`, etc.

```python
# Inside your pipeline's main execution method

from bioetl.core.logger import set_run_context

def run_pipeline(run_id, source_name):
    # Set the context for this entire run.
    set_run_context(
        run_id=run_id,
        stage="bootstrap",
        actor="scheduler",
        source=source_name
    )

    log.info("Context set. Starting pipeline execution.")

    # ... call your extract, transform, etc. stages ...
```

## 4. Logging Structured Events

The primary benefit of `structlog` is adding key-value context to your log messages.

### Basic Informational Logging

```python
# Log a simple message
log.info("Extraction complete.")

# Add structured context
log.info(
    "API request successful.",
    endpoint="/api/v1/data",
    duration_ms=150,
    rows_fetched=1000
)
```

**JSON Output:**

```json
{
  "message": "API request successful.",
  "endpoint": "/api/v1/data",
  "duration_ms": 150,
  "rows_fetched": 1000,
  "run_id": "...",
  "stage": "extract",
  ...
}
```

### Logging Warnings

Use `log.warning` for events that are unexpected but do not prevent the process from continuing.

```python
log.warning(
    "Rate limit approaching.",
    requests_remaining=5,
    reset_time_seconds=60
)
```

### Logging Errors

Use `log.error` for events that cause a process to fail.

#### Logging Handled Exceptions

It is a best practice to catch specific exceptions and log them with context. The `exc_info=True` argument will automatically capture the full exception traceback and format it correctly.

```python
try:
    response = requests.get("https://bad-url.com/data")
    response.raise_for_status()
except requests.exceptions.RequestException as e:
    log.error(
        "API request failed.",
        endpoint="/data",
        attempt=3,
        exc_info=True # This adds the traceback
    )
```

**JSON Output:**

```json
{
  "message": "API request failed.",
  "endpoint": "/data",
  "attempt": 3,
  "exception": "requests.exceptions.HTTPError: 404 Client Error...",
  "traceback": "Traceback (most recent call last):\n  ...\n",
  ...
}
```

## 5. Best Practices Summary

- **Configure Once**: Call `UnifiedLogger.configure()` exactly once at application startup.
- **Get Logger at Module Level**: Use `log = UnifiedLogger.get(__name__)` at the top of each file.
- **Set Context Early**: Call `set_run_context()` at the beginning of each pipeline run.
- **Log with Context**: Don't just log strings. Add key-value pairs to provide context for your events.
- **Use `exc_info=True` for Errors**: Always include the full traceback when logging caught exceptions to aid in debugging.
- **Match Log Level to Severity**: Use `info` for routine events, `warning` for potential problems, and `error` for definite failures.
