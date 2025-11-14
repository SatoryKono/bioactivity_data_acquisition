"""Deprecated compatibility layer for configuration models.

Prefer importing from `bioetl.config.models.models` (core models) or
`bioetl.config.models.policies` (policy objects). This package now simply
re-exports the canonical modules for backward compatibility.
"""

from __future__ import annotations

import warnings

from .models import (
    CLIConfig,
    FallbacksConfig,
    IOConfig,
    IOInputConfig,
    IOOutputConfig,
    MaterializationConfig,
    PathsConfig,
    PipelineCommonCompat,
    PipelineConfig,
    PipelineDomainConfig,
    PipelineInfrastructureConfig,
    PipelineMetadata,
    PostprocessConfig,
    PostprocessCorrelationConfig,
    RuntimeConfig,
    SourceConfig,
    SourceParameters,
    TelemetryConfig,
    TransformConfig,
    ValidationConfig,
)
from .policies import (
    CacheConfig,
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
    HTTPClientConfig,
    HTTPConfig,
    LoggingConfig,
    RateLimitConfig,
    RetryConfig,
    StatusCode,
)

__all__ = (
    "PipelineConfig",
    "PipelineMetadata",
    "PipelineCommonCompat",
    "RuntimeConfig",
    "IOConfig",
    "IOInputConfig",
    "IOOutputConfig",
    "PathsConfig",
    "MaterializationConfig",
    "SourceConfig",
    "SourceParameters",
    "CLIConfig",
    "FallbacksConfig",
    "TransformConfig",
    "PostprocessConfig",
    "PostprocessCorrelationConfig",
    "ValidationConfig",
    "PipelineDomainConfig",
    "PipelineInfrastructureConfig",
    "CacheConfig",
    "DeterminismConfig",
    "DeterminismEnvironmentConfig",
    "DeterminismHashColumnSchema",
    "DeterminismHashingConfig",
    "DeterminismMetaConfig",
    "DeterminismSerializationConfig",
    "DeterminismSerializationCSVConfig",
    "DeterminismSortingConfig",
    "DeterminismWriteConfig",
    "HTTPConfig",
    "HTTPClientConfig",
    "RateLimitConfig",
    "RetryConfig",
    "CircuitBreakerConfig",
    "StatusCode",
    "LoggingConfig",
    "TelemetryConfig",
)

warnings.warn(
    (
        "bioetl.config.models is deprecated; import from "
        "bioetl.config.models.models or bioetl.config.models.policies instead."
    ),
    DeprecationWarning,
    stacklevel=2,
)
