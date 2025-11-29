"""Aggregated exports for HTTP utilities.

This module provides a stable import surface for HTTP-related helpers used
across client factories and adapters. It re-exports common mixins and
resilience utilities and includes a lightweight ``UnifiedAPIClient`` stub to
wire together prepared request components.
"""
from __future__ import annotations

from bioetl.core.http.api_client import UnifiedAPIClient
from bioetl.core.http.base_http_client import BaseHttpClient
from bioetl.core.http.api_entity_client import (
    BaseApiEntityClient,
    EntityClientProtocol,
)

from bioetl.core.http.cache import CacheStrategy, TTLCache, TTLCacheConfig
from bioetl.core.http.circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitBreakerOpenError,
    CircuitBreakerStrategy,
)
from bioetl.core.http.client_mixins import ApiClientMixin, ClosableMixin
from bioetl.core.http.config import APIConfig
from bioetl.core.http.interfaces import (
    ApiTransportProtocol,
    BaseApiClient,
    ResilientApiClient,
)
from bioetl.core.http.pagination import (
    DEFAULT_NEXT_KEY,
    DEFAULT_PAGE_KEY,
    DEFAULT_PAGE_PARAM,
    DefaultPaginationStrategy,
    NextLinkPagination,
    PageParamPagination,
    PaginationStrategy,
)
from bioetl.core.http.rate_limiter import (
    RateLimiter,
    TokenBucketConfig,
    TokenBucketRateLimiter,
)
from bioetl.core.http.request_builder import RequestBuilder
from bioetl.core.http.request_executor import HTTPClientError
from bioetl.core.http.resilience import (
    ResilienceComponents,
    ResilientRequestExecutorFactory,
)
from bioetl.core.http.retry import RetryPolicy, RetryStrategy
from bioetl.core.http.types import (
    JSONPage,
    JSONPayload,
    JSONRecord,
    JSONRecordStream,
    Normalizer,
    WrapCallable,
    WrapIterator,
)


__all__ = [
    "APIConfig",
    "ApiClientMixin",
    "BaseHttpClient",
    "BaseApiClient",
    "ResilientApiClient",
    "ApiTransportProtocol",
    "BaseApiEntityClient",
    "CacheStrategy",
    "CircuitBreaker",
    "CircuitBreakerConfig",
    "CircuitBreakerOpenError",
    "CircuitBreakerStrategy",
    "ClosableMixin",
    "DEFAULT_NEXT_KEY",
    "DEFAULT_PAGE_KEY",
    "DEFAULT_PAGE_PARAM",
    "DefaultPaginationStrategy",
    "EntityClientProtocol",
    "HTTPClientError",
    "JSONPage",
    "JSONPayload",
    "JSONRecord",
    "JSONRecordStream",
    "NextLinkPagination",
    "Normalizer",
    "PageParamPagination",
    "PaginationStrategy",
    "RateLimiter",
    "RequestBuilder",
    "ResilienceComponents",
    "ResilientRequestExecutorFactory",
    "RetryPolicy",
    "RetryStrategy",
    "TokenBucketConfig",
    "TokenBucketRateLimiter",
    "TTLCache",
    "TTLCacheConfig",
    "UnifiedAPIClient",
    "WrapCallable",
    "WrapIterator",
]
