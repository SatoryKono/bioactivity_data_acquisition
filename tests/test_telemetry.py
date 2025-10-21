"""Tests for telemetry functionality."""

from unittest.mock import Mock, patch

import pytest

from library.telemetry import (
    TraceContextManager,
    add_span_attribute,
    add_span_event,
    get_current_span_id,
    get_current_trace_id,
    instrument_api_client,
    instrument_etl_stage,
    setup_telemetry,
    traced_operation,
)


class TestTelemetrySetup:
    """Test telemetry setup functionality."""

    def test_setup_telemetry_without_jaeger(self):
        """Test that telemetry can be set up without Jaeger endpoint."""
        with patch('library.telemetry.trace.set_tracer_provider') as mock_set_provider:
            setup_telemetry(
                service_name="test-service",
                jaeger_endpoint=None,
                enable_requests_instrumentation=False,
            )
            
            # Should set up tracer provider
            mock_set_provider.assert_called_once()

    def test_setup_telemetry_with_jaeger(self):
        """Test that telemetry can be set up with Jaeger endpoint."""
        with patch('library.telemetry.trace.set_tracer_provider') as mock_set_provider, \
             patch('library.telemetry.JaegerExporter') as mock_jaeger, \
             patch('library.telemetry.BatchSpanProcessor') as mock_processor:
            
            setup_telemetry(
                service_name="test-service",
                jaeger_endpoint="http://localhost:14268/api/traces",
                enable_requests_instrumentation=False,
            )
            
            # Should set up tracer provider and Jaeger exporter
            mock_set_provider.assert_called_once()
            mock_jaeger.assert_called_once()

    def test_setup_telemetry_with_requests_instrumentation(self):
        """Test that requests instrumentation can be enabled."""
        with patch('library.telemetry.trace.set_tracer_provider'), \
             patch('library.telemetry.RequestsInstrumentor') as mock_instrumentor:
            
            setup_telemetry(
                service_name="test-service",
                enable_requests_instrumentation=True,
            )
            
            # Should instrument requests
            mock_instrumentor.return_value.instrument.assert_called_once()


class TestTracedOperation:
    """Test traced operation context manager."""

    def test_traced_operation_success(self):
        """Test successful traced operation."""
        with patch('library.telemetry.trace.get_tracer') as mock_get_tracer:
            mock_span = Mock()
            mock_tracer = Mock()
            mock_tracer.start_as_current_span.return_value.__enter__ = Mock(return_value=mock_span)
            mock_tracer.start_as_current_span.return_value.__exit__ = Mock(return_value=None)
            mock_get_tracer.return_value = mock_tracer
            
            with traced_operation("test.operation", key="value") as span:
                assert span == mock_span
                span.set_attribute.assert_called_with("key", "value")

    def test_traced_operation_with_exception(self):
        """Test traced operation with exception."""
        with patch('library.telemetry.trace.get_tracer') as mock_get_tracer:
            mock_span = Mock()
            mock_tracer = Mock()
            mock_tracer.start_as_current_span.return_value.__enter__ = Mock(return_value=mock_span)
            mock_tracer.start_as_current_span.return_value.__exit__ = Mock(return_value=None)
            mock_get_tracer.return_value = mock_tracer
            
            with pytest.raises(ValueError):
                with traced_operation("test.operation") as span:
                    raise ValueError("test error")
            
            # Should record exception
            span.record_exception.assert_called_once()

    def test_get_current_trace_id_no_span(self):
        """Test getting trace ID when no current span."""
        with patch('library.telemetry.trace.get_current_span') as mock_get_span:
            mock_span = Mock()
            mock_span.is_recording.return_value = False
            mock_get_span.return_value = mock_span
            
            trace_id = get_current_trace_id()
            assert trace_id is None

    def test_get_current_trace_id_with_span(self):
        """Test getting trace ID when current span exists."""
        with patch('library.telemetry.trace.get_current_span') as mock_get_span:
            mock_span = Mock()
            mock_span.is_recording.return_value = True
            mock_span_context = Mock()
            mock_span_context.trace_id = 12345
            mock_span.get_span_context.return_value = mock_span_context
            mock_get_span.return_value = mock_span
            
            trace_id = get_current_trace_id()
            assert trace_id == "00000000000000000000000000003039"  # 12345 in hex

    def test_get_current_span_id_no_span(self):
        """Test getting span ID when no current span."""
        with patch('library.telemetry.trace.get_current_span') as mock_get_span:
            mock_span = Mock()
            mock_span.is_recording.return_value = False
            mock_get_span.return_value = mock_span
            
            span_id = get_current_span_id()
            assert span_id is None

    def test_get_current_span_id_with_span(self):
        """Test getting span ID when current span exists."""
        with patch('library.telemetry.trace.get_current_span') as mock_get_span:
            mock_span = Mock()
            mock_span.is_recording.return_value = True
            mock_span_context = Mock()
            mock_span_context.span_id = 67890
            mock_span.get_span_context.return_value = mock_span_context
            mock_get_span.return_value = mock_span
            
            span_id = get_current_span_id()
            assert span_id == "0000000000010932"  # 67890 in hex

    def test_add_span_attribute_no_span(self):
        """Test adding attribute when no current span."""
        with patch('library.telemetry.trace.get_current_span') as mock_get_span:
            mock_span = Mock()
            mock_span.is_recording.return_value = False
            mock_get_span.return_value = mock_span
            
            # Should not raise exception
            add_span_attribute("key", "value")

    def test_add_span_attribute_with_span(self):
        """Test adding attribute when current span exists."""
        with patch('library.telemetry.trace.get_current_span') as mock_get_span:
            mock_span = Mock()
            mock_span.is_recording.return_value = True
            mock_get_span.return_value = mock_span
            
            add_span_attribute("key", "value")
            mock_span.set_attribute.assert_called_with("key", "value")

    def test_add_span_event_no_span(self):
        """Test adding event when no current span."""
        with patch('library.telemetry.trace.get_current_span') as mock_get_span:
            mock_span = Mock()
            mock_span.is_recording.return_value = False
            mock_get_span.return_value = mock_span
            
            # Should not raise exception
            add_span_event("test_event", {"key": "value"})

    def test_add_span_event_with_span(self):
        """Test adding event when current span exists."""
        with patch('library.telemetry.trace.get_current_span') as mock_get_span:
            mock_span = Mock()
            mock_span.is_recording.return_value = True
            mock_get_span.return_value = mock_span
            
            add_span_event("test_event", {"key": "value"})
            mock_span.add_event.assert_called_with("test_event", {"key": "value"})


class TestTraceContextManager:
    """Test TraceContextManager."""

    def test_trace_context_manager_success(self):
        """Test successful trace context manager."""
        with patch('library.telemetry.trace.get_tracer') as mock_get_tracer, \
             patch('library.telemetry.trace.set_span_in_context') as mock_set_context:
            
            mock_span = Mock()
            mock_tracer = Mock()
            mock_tracer.start_span.return_value = mock_span
            mock_get_tracer.return_value = mock_tracer
            
            with TraceContextManager("test.operation", key="value") as span:
                assert span == mock_span
                span.set_attribute.assert_called_with("key", "value")
                mock_set_context.assert_called_with(span)
            
            span.end.assert_called_once()

    def test_trace_context_manager_with_exception(self):
        """Test trace context manager with exception."""
        with patch('library.telemetry.trace.get_tracer') as mock_get_tracer, \
             patch('library.telemetry.trace.set_span_in_context'):
            
            mock_span = Mock()
            mock_tracer = Mock()
            mock_tracer.start_span.return_value = mock_span
            mock_get_tracer.return_value = mock_tracer
            
            with pytest.raises(ValueError):
                with TraceContextManager("test.operation") as span:
                    raise ValueError("test error")
            
            # Should record exception and set error status
            span.record_exception.assert_called_once()


class TestInstrumentationDecorators:
    """Test instrumentation decorators."""

    def test_instrument_etl_stage_decorator(self):
        """Test ETL stage instrumentation decorator."""
        with patch('library.telemetry.traced_operation') as mock_traced:
            @instrument_etl_stage("extract")
            def test_function():
                return "test"
            
            result = test_function()
            assert result == "test"
            mock_traced.assert_called_once()

    def test_instrument_api_client_decorator(self):
        """Test API client instrumentation decorator."""
        with patch('library.telemetry.traced_operation') as mock_traced:
            @instrument_api_client("chembl")
            def test_function():
                return "test"
            
            result = test_function()
            assert result == "test"
            mock_traced.assert_called_once()
