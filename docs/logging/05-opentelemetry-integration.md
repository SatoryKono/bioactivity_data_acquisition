# 5. OpenTelemetry Integration

## Overview

Modern applications, especially those in a microservices environment, rely on
distributed tracing to provide observability into requests as they travel across
different services. [OpenTelemetry](https://opentelemetry.io/) has emerged as
the industry standard for instrumenting code to generate traces, metrics, and
logs.

The `bioetl` logging system includes an optional, first-class integration with
OpenTelemetry. When enabled, this integration automatically enriches all log
records with the `trace_id` and `span_id` from the active OpenTelemetry span.
This allows for seamless correlation of logs with traces in observability
platforms like Jaeger, Datadog, or SigNoz.

## Enabling the Integration

The OpenTelemetry integration is disabled by default. It can be enabled by
setting a single parameter in the `LoggerConfig`:

```python
# In your main application entry point
from bioetl.core.logger import UnifiedLogger, LoggerConfig

config = LoggerConfig(
    # ... other settings ...
    telemetry_enabled=True  # This enables the integration
)
UnifiedLogger.configure(config)
```

When `telemetry_enabled` is set to `True`, the `UnifiedLogger` adds a special
processor to the `structlog` pipeline. This processor inspects the active
OpenTelemetry context, extracts the current `trace_id` and `span_id`, and
injects them into the log event dictionary.

## How It Works

For the integration to work, your application must be instrumented with the
OpenTelemetry SDK. This is typically done at the application's entry point using
an auto-instrumentation agent or by manually configuring the SDK.

**Example of Manual OpenTelemetry Setup:**

```python
# In your application's startup code
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter

# Set up a tracer provider
provider = TracerProvider()
processor = BatchSpanProcessor(ConsoleSpanExporter())
provider.add_span_processor(processor)
trace.set_tracer_provider(provider)

# Get a tracer
tracer = trace.get_tracer(__name__)
```

Once the tracer is configured, you can create spans to trace the execution of
your code.

```python
from bioetl.core.logger import UnifiedLogger

log = UnifiedLogger.get(__name__)

# Create a new span for this operation
with tracer.start_as_current_span("my_etl_operation") as span:
    # Any logs created within this `with` block will automatically
    # be enriched with the trace_id and span_id of the "my_etl_operation" span.
    log.info("Starting operation")

    # ... perform work ...

    log.info("Operation complete")
```

### Log Output with Telemetry Enabled

When the code above is run with `telemetry_enabled=True`, the resulting JSON
logs will automatically contain the new fields:

```json
{
  "generated_at": "...",
  "level": "info",
  "message": "Starting operation",
  "run_id": "...",
  "stage": "...",
  "trace_id": "0x1234567890abcdef1234567890abcdef",  // <-- Injected
  "span_id": "0x1234567890abcdef"                   // <-- Injected
}
```

This automatic enrichment is the key to powerful observability. In a compatible
backend, you can now instantly jump from a specific trace to all the logs that
were generated during that trace, or from a specific log message directly to the
trace that produced it. This dramatically reduces the time it takes to debug
issues in production environments.
