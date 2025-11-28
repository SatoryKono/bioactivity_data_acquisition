"""Aggregated exports for HTTP utilities.

This module provides a stable import surface for HTTP-related helpers used
across client factories and adapters. It re-exports common mixins and
resilience utilities and includes a lightweight ``UnifiedAPIClient`` stub to
wire together prepared request components.
"""
from __future__ import annotations


from bioetl.core.http.config import APIConfig
from bioetl.core.http.circuit_breaker import CircuitBreakerOpenError
from bioetl.core.http.client_mixins import ApiClientMixin, ClosableMixin
from bioetl.core.http.request_executor import HTTPClientError
from bioetl.core.http.resilience import (
    ResilienceComponents,
    ResilientRequestExecutorFactory,
)


from bioetl.core.http.api_client import UnifiedAPIClient


__all__ = [
    "APIConfig",
    "ApiClientMixin",
    "ClosableMixin",
    "CircuitBreakerOpenError",
    "HTTPClientError",
    "ResilientRequestExecutorFactory",
    "ResilienceComponents",
    "UnifiedAPIClient",
]

