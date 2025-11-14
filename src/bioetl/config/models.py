"""Compatibility shim re-exporting canonical pipeline configuration models."""

from __future__ import annotations

import warnings

from .models.base import PipelineCommonCompat, PipelineConfig, PipelineMetadata
from .models.cache import CacheConfig
from .models.cli import CLIConfig
from .models.determinism import (
    DeterminismConfig,
    DeterminismEnvironmentConfig,
    DeterminismHashColumnSchema,
    DeterminismHashingConfig,
    DeterminismMetaConfig,
    DeterminismSerializationConfig,
    DeterminismSerializationCSVConfig,
    DeterminismSortingConfig,
    DeterminismWriteConfig,
)
from .models.fallbacks import FallbacksConfig
from .models.http import (
    CircuitBreakerConfig,
    HTTPClientConfig,
    HTTPConfig,
    RateLimitConfig,
    RetryConfig,
    StatusCode,
)
from .models.io import IOConfig, IOInputConfig, IOOutputConfig
from .models.logging import LoggingConfig
from .models.paths import MaterializationConfig, PathsConfig
from .models.postprocess import PostprocessConfig, PostprocessCorrelationConfig
from .models.runtime import RuntimeConfig
from .models.source import SourceConfig, SourceParameters
from .models.telemetry import TelemetryConfig
from .models.transform import TransformConfig
from .models.validation import ValidationConfig

__all__ = [
    "PipelineConfig",
    "PipelineMetadata",
    "PipelineCommonCompat",
    "RuntimeConfig",
    "IOConfig",
    "IOInputConfig",
    "IOOutputConfig",
    "HTTPConfig",
    "HTTPClientConfig",
    "RetryConfig",
    "RateLimitConfig",
    "CircuitBreakerConfig",
    "StatusCode",
    "CacheConfig",
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
    "FallbacksConfig",
    "ValidationConfig",
    "TransformConfig",
    "PostprocessConfig",
    "PostprocessCorrelationConfig",
    "LoggingConfig",
    "TelemetryConfig",
    "SourceConfig",
    "SourceParameters",
    "CLIConfig",
]

warnings.warn(
    (
        "bioetl.config.models is deprecated; import from "
        "bioetl.config.models.base or bioetl.config.models.policies instead."
    ),
    DeprecationWarning,
    stacklevel=2,
)

