"""Infrastructure-level configuration sections for pipelines."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field

from .cache import CacheConfig
from .cli import CLIConfig
from .determinism import DeterminismConfig
from .http import HTTPConfig
from .io import IOConfig
from .logging import LoggingConfig
from .paths import MaterializationConfig, PathsConfig
from .runtime import RuntimeConfig
from .telemetry import TelemetryConfig


class PipelineInfrastructureConfig(BaseModel):
    """Cross-cutting infrastructure knobs (I/O, HTTP, logging, determinism)."""

    model_config = ConfigDict(extra="forbid")

    runtime: RuntimeConfig = Field(
        default_factory=RuntimeConfig, description="Execution/runtime scheduling controls."
    )
    io: IOConfig = Field(
        default_factory=IOConfig,
        description="Input/output serialization, formats, and buffer sizing.",
    )
    http: HTTPConfig = Field(
        ..., description="HTTP client defaults, profiles, and retry/backoff policies."
    )
    cache: CacheConfig = Field(
        default_factory=CacheConfig, description="Cache storage (TTL, backends, fan-out)."
    )
    paths: PathsConfig = Field(
        default_factory=PathsConfig, description="Root paths and layout for IO artifacts."
    )
    determinism: DeterminismConfig = Field(
        default_factory=DeterminismConfig,
        description="Deterministic write and sorting policies enforced before export.",
    )
    materialization: MaterializationConfig = Field(
        default_factory=MaterializationConfig,
        description="Materialization formats, partitioning, and output entity descriptors.",
    )
    logging: LoggingConfig = Field(
        default_factory=LoggingConfig, description="Structured logging setup (UnifiedLogger)."
    )
    telemetry: TelemetryConfig = Field(
        default_factory=TelemetryConfig, description="Telemetry/metrics exporters and run context."
    )
    cli: CLIConfig = Field(
        default_factory=CLIConfig, description="CLI flags and overrides exposed to operators."
    )


