# Logging Guidelines

This document defines the logging standards for the `bioetl` project. All logging **MUST** use the centralized `UnifiedLogger` system based on `structlog`.

## Principles

- **Centralization**: All logging **MUST** use `UnifiedLogger` from `bioetl.core.logger`.
- **Structured Logging**: All logs **MUST** be structured (JSON or key-value format).
- **No Print Statements**: Direct `print()` calls **SHALL NOT** be used in production code.
- **Context Enrichment**: All log records **MUST** include mandatory context fields.
- **Security**: Sensitive data **MUST** be redacted from logs.

## UnifiedLogger Usage

### Initialization

The logger **MUST** be configured once at the application entry point (e.g., CLI main function):

```python
from bioetl.core.logger import UnifiedLogger, LoggerConfig
from pathlib import Path

def main():
    config = LoggerConfig(
        level="INFO",
        console_format="text",  # "text" for dev, "json" for prod
        file_enabled=True,
        file_path=Path("logs/pipeline_run.log"),
        telemetry_enabled=False
    )
    UnifiedLogger.configure(config)
    # ... rest of application
```

### Getting a Logger

Get a logger instance at the module level using `__name__`:

```python
from bioetl.core.logger import UnifiedLogger

log = UnifiedLogger.get(__name__)

class MyPipeline:
    def run(self):
        log.info("Pipeline started", step="initialization")
```

### Setting Run Context

Set execution context at the start of each pipeline run:

```python
from bioetl.core.logger import set_run_context

def run_pipeline():
    set_run_context(
        run_id="run_20240101_120000",
        stage="extract",
        actor="user@example.com",
        source="chembl",
        trace_id=None  # Optional: OpenTelemetry trace ID
    )
    log.info("Context set")
```

## Mandatory Fields

All log records **MUST** include the following fields (automatically injected):

- `run_id`: Unique identifier for the pipeline run
- `pipeline`: Name of the pipeline
- `stage`: Current execution stage (e.g., `extract`, `transform`, `validate`, `export`)
- `timestamp_utc`: UTC timestamp in ISO-8601 format

### Valid Examples

```python
log.info(
    "Extraction started",
    source="chembl",
    batch_size=1000,
    row_count=50000
)
```

This automatically includes: `run_id`, `pipeline`, `stage`, `timestamp_utc`.

### Invalid Examples

```python
# Invalid: using print()
print("Extraction started")  # SHALL NOT be used

# Invalid: using standard logging
import logging
logger = logging.getLogger(__name__)  # SHALL NOT be used
logger.info("Message")
```

## Log Levels

Use the following log levels consistently:

- **DEBUG**: Detailed diagnostic information (development only)
- **INFO**: General informational messages (default level)
- **WARNING**: Warning messages for recoverable issues
- **ERROR**: Error messages for failures that don't stop execution
- **CRITICAL**: Critical errors that halt execution

### Default Level

The default log level is `INFO`. Set via `LoggerConfig.level`.

## Structured Events

All log events **SHOULD** include structured key-value data:

```python
# Valid: structured logging
log.info(
    "Data validation completed",
    valid_rows=9500,
    invalid_rows=500,
    validation_time_ms=1250
)

log.error(
    "API request failed",
    url="https://api.example.com/data",
    status_code=429,
    retry_after=60,
    error_message="Rate limit exceeded"
)
```

## Security: Secret Redaction

Sensitive data **MUST** be redacted from logs:

- API keys
- Passwords
- Tokens
- Personal identifiers

The `UnifiedLogger` includes automatic redaction processors. Ensure sensitive values are not included in log messages or structured fields.

### Valid Examples

```python
# Valid: redacted sensitive data
log.info(
    "API authentication successful",
    api_endpoint="https://api.example.com",
    # API key is NOT logged
)
```

### Invalid Examples

```python
# Invalid: sensitive data in logs
log.info(
    "API key obtained",
    api_key="sk_live_1234567890abcdef"  # SHALL NOT log secrets
)
```

## Output Formats

### Development (Console)

Use `console_format="text"` for human-readable key-value output:

```
2024-01-01T12:00:00.123Z [INFO] run_id=run_123 stage=extract pipeline=activity Data extraction started source=chembl rows=1000
```

### Production (File/JSON)

Use `console_format="json"` for machine-readable JSON output:

```json
{"timestamp": "2024-01-01T12:00:00.123Z", "level": "INFO", "run_id": "run_123", "stage": "extract", "pipeline": "activity", "event": "Data extraction started", "source": "chembl", "rows": 1000}
```

## Best Practices

1. **Context Binding**: Use `set_run_context()` at pipeline start to enrich all subsequent logs.
2. **Structured Data**: Prefer structured key-value pairs over formatted strings.
3. **Error Context**: Always include relevant context in error logs (URLs, IDs, counts).
4. **Performance**: Log data volumes and execution times for monitoring.
5. **Traceability**: Include `trace_id` when available for distributed tracing.

### Valid Example

```python
from bioetl.core.logger import UnifiedLogger, set_run_context

log = UnifiedLogger.get(__name__)

def extract_data(source: str, batch_size: int) -> pd.DataFrame:
    start_time = time.time()
    log.info(
        "Starting extraction",
        source=source,
        batch_size=batch_size
    )
    
    try:
        data = fetch_from_api(source, batch_size)
        elapsed_ms = (time.time() - start_time) * 1000
        log.info(
            "Extraction completed",
            source=source,
            rows=len(data),
            duration_ms=elapsed_ms
        )
        return data
    except Exception as e:
        log.error(
            "Extraction failed",
            source=source,
            error=str(e),
            error_type=type(e).__name__
        )
        raise
```

## References

- UnifiedLogger implementation: `src/bioetl/core/logger.py`
- Detailed documentation: [`docs/logging/`](../logging/)
- Security guidelines: [`docs/logging/04-security-secret-redaction.md`](../logging/04-security-secret-redaction.md)
