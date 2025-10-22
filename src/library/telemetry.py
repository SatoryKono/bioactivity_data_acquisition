"""OpenTelemetry configuration and utilities for tracing."""

from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Any

from opentelemetry import trace
from opentelemetry.exporter.jaeger.thrift import JaegerExporter
from opentelemetry.instrumentation.requests import RequestsInstrumentor
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

from library.logging_setup import get_logger

logger = get_logger(__name__)


def setup_telemetry(
    service_name: str = "bioactivity-etl",
    jaeger_endpoint: str | None = None,
    enable_requests_instrumentation: bool = True,
) -> None:
    """Setup OpenTelemetry tracing.
    
    Args:
        service_name: Name of the service for tracing
        jaeger_endpoint: Jaeger collector endpoint (e.g., "http://localhost:14268/api/traces")
        enable_requests_instrumentation: Whether to instrument requests library
    """
    # Get configuration from environment variables
    jaeger_endpoint = jaeger_endpoint or os.getenv("JAEGER_ENDPOINT")
    
    # Get package version
    try:
        from importlib.metadata import version
        package_version = version("bioactivity-data-acquisition")
    except Exception:
        package_version = "0.1.0"
    
    # Create resource with service name
    resource = Resource.create({
        "service.name": service_name,
        "service.version": os.getenv("SERVICE_VERSION", package_version),
        "deployment.environment": os.getenv("ENVIRONMENT", "development"),
    })
    
    # Set up tracer provider
    tracer_provider = TracerProvider(resource=resource)
    trace.set_tracer_provider(tracer_provider)
    
    # Add Jaeger exporter if endpoint is provided
    if jaeger_endpoint:
        jaeger_exporter = JaegerExporter(
            agent_host_name=os.getenv("JAEGER_AGENT_HOST", "localhost"),
            agent_port=int(os.getenv("JAEGER_AGENT_PORT", "6831")),
        )
        
        span_processor = BatchSpanProcessor(jaeger_exporter)
        tracer_provider.add_span_processor(span_processor)
        
        logger.info(
            "OpenTelemetry configured with Jaeger",
            jaeger_endpoint=jaeger_endpoint,
            service_name=service_name,
        )
    else:
        logger.info(
            "OpenTelemetry configured without exporter",
            service_name=service_name,
        )
    
    # Instrument requests library if enabled
    if enable_requests_instrumentation:
        RequestsInstrumentor().instrument()
        logger.info("HTTP requests instrumentation enabled")


@contextmanager
def traced_operation(operation_name: str, **attributes: Any):
    """Context manager for tracing operations.
    
    Args:
        operation_name: Name of the operation to trace
        **attributes: Additional attributes to add to the span
    
    Example:
        with traced_operation("etl.extract", source="chembl", batch_id=123):
            # ETL operation code
            pass
    """
    tracer = trace.get_tracer(__name__)
    
    with tracer.start_as_current_span(operation_name) as span:
        # Add attributes to span
        for key, value in attributes.items():
            span.set_attribute(key, value)
        
        try:
            yield span
        except Exception as e:
            # Record exception in span
            span.record_exception(e)
            span.set_status(trace.Status(trace.StatusCode.ERROR, str(e)))
            raise


def get_current_trace_id() -> str | None:
    """Get current trace ID for logging correlation."""
    span = trace.get_current_span()
    if span and span.is_recording():
        return format(span.get_span_context().trace_id, "032x")
    return None


def get_current_span_id() -> str | None:
    """Get current span ID for logging correlation."""
    span = trace.get_current_span()
    if span and span.is_recording():
        return format(span.get_span_context().span_id, "016x")
    return None


def add_span_attribute(key: str, value: Any) -> None:
    """Add attribute to current span."""
    span = trace.get_current_span()
    if span and span.is_recording():
        span.set_attribute(key, value)


def add_span_event(name: str, attributes: dict[str, Any] | None = None) -> None:
    """Add event to current span."""
    span = trace.get_current_span()
    if span and span.is_recording():
        span.add_event(name, attributes or {})


class TraceContextManager:
    """Context manager for managing trace context across operations."""
    
    def __init__(self, operation_name: str, **attributes: Any):
        self.operation_name = operation_name
        self.attributes = attributes
        self.span = None
    
    def __enter__(self):
        tracer = trace.get_tracer(__name__)
        self.span = tracer.start_span(self.operation_name)
        
        # Add attributes
        for key, value in self.attributes.items():
            self.span.set_attribute(key, value)
        
        # Set as current span
        trace.set_span_in_context(self.span)
        return self.span
    
    def __exit__(self, exc_type, exc_val, _exc_tb):
        if self.span:
            if exc_type:
                self.span.record_exception(exc_val)
                self.span.set_status(trace.Status(trace.StatusCode.ERROR, str(exc_val)))
            
            self.span.end()


def instrument_etl_stage(stage_name: str):
    """Decorator for instrumenting ETL stages.
    
    Args:
        stage_name: Name of the ETL stage (e.g., "extract", "transform", "load")
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            with traced_operation(f"etl.{stage_name}", stage=stage_name):
                return func(*args, **kwargs)
        return wrapper
    return decorator


def instrument_api_client(client_name: str):
    """Decorator for instrumenting API client methods.
    
    Args:
        client_name: Name of the API client (e.g., "chembl", "crossref")
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            with traced_operation(f"api.{client_name}", client=client_name):
                return func(*args, **kwargs)
        return wrapper
    return decorator
