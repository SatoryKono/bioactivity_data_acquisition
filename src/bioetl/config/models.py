"""Deprecated compatibility layer for pipeline configuration models."""

from __future__ import annotations

import warnings

from .models.base import PipelineConfig, PipelineMetadata
from .models.cache import CacheConfig
from .models.cli import CLIConfig
from .models.determinism import (
    DeterminismConfig,
    DeterminismEnvironmentConfig,
    DeterminismHashColumnSchema,
    DeterminismHashingConfig,
    DeterminismMetaConfig,
    DeterminismSerializationCSVConfig,
    DeterminismSerializationConfig,
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
from .models.source import SourceConfig
from .models.telemetry import TelemetryConfig
from .models.transform import TransformConfig
from .models.validation import ValidationConfig

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
    "Importing from 'bioetl.config.models' is deprecated; use modules under "
    "'bioetl.config.models.*' instead.",
    DeprecationWarning,
    stacklevel=2,
)
