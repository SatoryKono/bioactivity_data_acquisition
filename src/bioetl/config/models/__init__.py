"""Pydantic models describing the pipeline configuration schema."""

from __future__ import annotations

import sys
import types
import warnings
from typing import Any

from .models import (
    CacheConfig,
    ChemblCircuitBreakerConfig,
    ChemblClientConfig,
    ChemblPreflightConfig,
    ChemblPreflightRetryConfig,
    ChemblTimeoutConfig,
    CLIConfig,
    ClientsConfig,
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
from .policies import (
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
    "ClientsConfig",
    "ChemblClientConfig",
    "ChemblPreflightConfig",
    "ChemblPreflightRetryConfig",
    "ChemblTimeoutConfig",
    "ChemblCircuitBreakerConfig",
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

_DEPRECATION_MESSAGE = (
    "Importing from 'bioetl.config.models' submodules is deprecated and will be removed in "
    "bioetl 2.0. Import from 'bioetl.config.models.models' or 'bioetl.config.models.policies' "
    "instead."
)

_DEPRECATED_NAMES = frozenset(__all__)


def __getattr__(name: str) -> Any:
    if name in globals() and name in _DEPRECATED_NAMES:
        warnings.warn(_DEPRECATION_MESSAGE, DeprecationWarning, stacklevel=2)
        return globals()[name]
    msg = f"module 'bioetl.config.models' has no attribute '{name}'"
    raise AttributeError(msg)


def _install_deprecated_module(module_name: str, exported: tuple[str, ...], target: str) -> None:
    full_name = f"{__name__}.{module_name}"
    module = types.ModuleType(full_name)
    module_dict = module.__dict__
    module_dict["__all__"] = list(exported)
    message = (
        f"Module '{full_name}' is deprecated and will be removed in bioetl 2.0. Import from "
        f"'bioetl.config.models.{target}' instead."
    )

    def _deprecated_getattr(attr: str) -> Any:
        if attr in exported:
            warnings.warn(message, DeprecationWarning, stacklevel=2)
            return globals()[attr]
        raise AttributeError(f"module '{full_name}' has no attribute '{attr}'")

    module_dict["__getattr__"] = _deprecated_getattr
    for attr in exported:
        module_dict[attr] = globals()[attr]
    sys.modules[full_name] = module


for _name, _symbols, _target in [
    ("base", ("PipelineConfig", "PipelineMetadata"), "models"),
    ("cache", ("CacheConfig",), "models"),
    ("cli", ("CLIConfig",), "models"),
    (
        "determinism",
        (
            "DeterminismConfig",
            "DeterminismEnvironmentConfig",
            "DeterminismHashColumnSchema",
            "DeterminismHashingConfig",
            "DeterminismMetaConfig",
            "DeterminismSerializationConfig",
            "DeterminismSerializationCSVConfig",
            "DeterminismSortingConfig",
            "DeterminismWriteConfig",
        ),
        "policies",
    ),
    ("fallbacks", ("FallbacksConfig",), "policies"),
    (
        "http",
        (
            "HTTPConfig",
            "HTTPClientConfig",
            "RetryConfig",
            "RateLimitConfig",
            "CircuitBreakerConfig",
            "StatusCode",
        ),
        "policies",
    ),
    ("io", ("IOConfig", "IOInputConfig", "IOOutputConfig"), "models"),
    ("logging", ("LoggingConfig",), "models"),
    ("paths", ("PathsConfig", "MaterializationConfig"), "models"),
    ("postprocess", ("PostprocessConfig", "PostprocessCorrelationConfig"), "models"),
    ("runtime", ("RuntimeConfig",), "models"),
    ("source", ("SourceConfig",), "models"),
    ("telemetry", ("TelemetryConfig",), "models"),
    ("transform", ("TransformConfig",), "models"),
    ("validation", ("ValidationConfig",), "models"),
]:
    _install_deprecated_module(_name, _symbols, _target)

warnings.warn(_DEPRECATION_MESSAGE, DeprecationWarning, stacklevel=2)

