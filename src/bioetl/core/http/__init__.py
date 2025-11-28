"""Aggregated exports for HTTP utilities.

This module provides a stable import surface for HTTP-related helpers used
across client factories and adapters. It re-exports common mixins and
resilience utilities and includes a lightweight ``UnifiedAPIClient`` stub to
wire together prepared request components.
"""
from __future__ import annotations

from typing import Any

from bioetl.core.http.api_client import APIConfig
from bioetl.core.http.circuit_breaker import CircuitBreakerOpenError
from bioetl.core.http.client_mixins import ApiClientMixin, ClosableMixin
from bioetl.core.http.request_executor import HTTPClientError
from bioetl.core.http.resilience import (
    ResilienceComponents,
    ResilientRequestExecutorFactory,
)


class UnifiedAPIClient:
    """Simple adapter bundling executor, builder, and pagination strategy."""

    def __init__(
        self,
        api_config: APIConfig,
        *,
        request_executor: Any,
        request_builder: Any,
        pagination_strategy: Any,
    ) -> None:
        self.api_config = api_config
        self.request_executor = request_executor
        self.request_builder = request_builder
        self.pagination_strategy = pagination_strategy

    def close(self) -> None:
        close_fn = getattr(self.request_executor, "close", None)
        if callable(close_fn):
            close_fn()
            return
        close_fn = getattr(self.request_builder, "close", None)
        if callable(close_fn):
            close_fn()


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

