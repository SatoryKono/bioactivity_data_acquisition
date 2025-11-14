"""Canonical entry point for BioETL configuration models.

Internal code MUST import high-level configuration types (e.g., `PipelineConfig`)
from this module instead of the legacy `bioetl.config.models` namespace. The
legacy path remains as a thin shim only for external compatibility.
"""

from __future__ import annotations

from .base import PipelineCommonCompat, PipelineConfig, PipelineMetadata
from .cli import CLIConfig
from .domain import PipelineDomainConfig
from .fallbacks import FallbacksConfig
from .infrastructure import PipelineInfrastructureConfig
from .io import IOConfig, IOInputConfig, IOOutputConfig
from .paths import MaterializationConfig, PathsConfig
from .postprocess import PostprocessConfig, PostprocessCorrelationConfig
from .runtime import RuntimeConfig
from .source import SourceConfig, SourceParameters
from .telemetry import TelemetryConfig
from .transform import TransformConfig
from .validation import ValidationConfig

__all__ = [
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
    "TransformConfig",
    "PostprocessConfig",
    "PostprocessCorrelationConfig",
    "ValidationConfig",
    "FallbacksConfig",
    "TelemetryConfig",
    "PipelineDomainConfig",
    "PipelineInfrastructureConfig",
]

