"""Deprecated compatibility layer for pipeline configuration models."""

from __future__ import annotations

import warnings

from .models.models import (
    CacheConfig,
    CLIConfig,
    IOConfig,
    IOInputConfig,
    IOOutputConfig,
    LoggingConfig,
    MaterializationConfig,
    PathsConfig,
    PipelineConfig,
    PipelineMetadata,
    PostprocessConfig,
    PostprocessCorrelationConfig,
    RuntimeConfig,
    SourceConfig,
    TelemetryConfig,
    TransformConfig,
    ValidationConfig,
)
from .models.policies import (
    CircuitBreakerConfig,
    DeterminismConfig,
    DeterminismEnvironmentConfig,
    DeterminismHashColumnSchema,
    DeterminismHashingConfig,
    DeterminismMetaConfig,
    DeterminismSerializationConfig,
    DeterminismSerializationCSVConfig,
    DeterminismSortingConfig,
    DeterminismWriteConfig,
    FallbacksConfig,
    HTTPClientConfig,
    HTTPConfig,
    RateLimitConfig,
    RetryConfig,
    StatusCode,
)

__all__ = [
    "PipelineConfig",
    "PipelineMetadata",
    "HTTPConfig",
    "HTTPClientConfig",
    "RetryConfig",
    "RateLimitConfig",
    "CircuitBreakerConfig",
    "StatusCode",
    "CacheConfig",
    "RuntimeConfig",
    "IOConfig",
    "IOInputConfig",
    "IOOutputConfig",
    "PathsConfig",
    "MaterializationConfig",
    "DeterminismConfig",
    "DeterminismSortingConfig",
    "DeterminismSerializationConfig",
    "DeterminismSerializationCSVConfig",
    "DeterminismHashingConfig",
    "DeterminismHashColumnSchema",
    "DeterminismEnvironmentConfig",
    "DeterminismWriteConfig",
    "DeterminismMetaConfig",
    "ValidationConfig",
    "TransformConfig",
    "PostprocessConfig",
    "PostprocessCorrelationConfig",
    "SourceConfig",
    "CLIConfig",
    "LoggingConfig",
    "FallbacksConfig",
    "TelemetryConfig",
]

warnings.warn(
    "Importing from 'bioetl.config.models' (module) is deprecated; use 'bioetl.config.models." \
    "models' or 'bioetl.config.models.policies' instead.",
    DeprecationWarning,
    stacklevel=2,
)
