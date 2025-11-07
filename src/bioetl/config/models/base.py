"""Base pipeline configuration models."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from .cache import CacheConfig
from .cli import CLIConfig
from .determinism import DeterminismConfig
from .fallbacks import FallbacksConfig
from .http import HTTPConfig
from .io import IOConfig
from .logging import LoggingConfig
from .paths import MaterializationConfig, PathsConfig
from .postprocess import PostprocessConfig
from .runtime import RuntimeConfig
from .source import SourceConfig
from .telemetry import TelemetryConfig
from .transform import TransformConfig
from .validation import ValidationConfig


class PipelineMetadata(BaseModel):
    """Descriptive metadata for the pipeline itself."""

    model_config = ConfigDict(extra="forbid")

    name: str = Field(..., description="Unique name of the pipeline (e.g., activity, assay).")
    version: str = Field(..., description="Semantic version of the pipeline implementation.")
    owner: str | None = Field(default=None, description="Team or individual responsible for the pipeline.")
    description: str | None = Field(default=None, description="Short human readable description.")


class PipelineConfig(BaseModel):
    """Root configuration object validated at CLI startup."""

    model_config = ConfigDict(extra="forbid")

    version: Literal[1] = Field(
        ..., description="Configuration schema version. Only version=1 is currently supported."
    )
    extends: Sequence[str] = Field(
        default_factory=tuple,
        description="Optional list of profile paths merged before this configuration.",
    )
    pipeline: PipelineMetadata = Field(..., description="Metadata describing the pipeline.")
    runtime: RuntimeConfig = Field(default_factory=RuntimeConfig, description="Runtime execution parameters.")
    io: IOConfig = Field(default_factory=IOConfig, description="Input/output configuration for the pipeline.")
    http: HTTPConfig = Field(..., description="HTTP client defaults and profiles.")
    cache: CacheConfig = Field(default_factory=CacheConfig)
    paths: PathsConfig = Field(default_factory=PathsConfig)
    determinism: DeterminismConfig = Field(default_factory=DeterminismConfig)
    materialization: MaterializationConfig = Field(default_factory=MaterializationConfig)
    fallbacks: FallbacksConfig = Field(default_factory=FallbacksConfig)
    validation: ValidationConfig = Field(default_factory=ValidationConfig)
    transform: TransformConfig = Field(default_factory=TransformConfig)
    postprocess: PostprocessConfig = Field(default_factory=PostprocessConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    telemetry: TelemetryConfig = Field(default_factory=TelemetryConfig)
    sources: dict[str, SourceConfig] = Field(
        default_factory=dict,
        description="Per-source settings keyed by a short identifier.",
    )
    cli: CLIConfig = Field(default_factory=CLIConfig)
    chembl: Mapping[str, Any] | None = Field(
        default=None,
        description="ChEMBL-specific configuration (e.g., enrichment settings).",
    )

    @model_validator(mode="after")
    def ensure_column_order_when_set(self) -> PipelineConfig:
        if self.determinism.column_order and not self.validation.schema_out:
            msg = (
                "determinism.column_order requires validation.schema_out to be set so the schema "
                "can enforce the order"
            )
            raise ValueError(msg)
        return self

