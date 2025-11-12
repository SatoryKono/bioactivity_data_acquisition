"""Base pipeline configuration models."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, Literal

# ruff: noqa: I001
from pydantic import BaseModel, ConfigDict, Field, model_validator

from .cache import CacheConfig
from .cli import CLIConfig
from .determinism import DeterminismConfig
from .fallbacks import FallbacksConfig
from .http import HTTPConfig
from .io import IOConfig
from .logging import LoggingConfig
from .paths import MaterializationConfig
from .paths import PathsConfig
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
    owner: str | None = Field(
        default=None, description="Team or individual responsible for the pipeline."
    )
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
    runtime: RuntimeConfig = Field(
        default_factory=RuntimeConfig, description="Runtime execution parameters."
    )
    io: IOConfig = Field(
        default_factory=IOConfig, description="Input/output configuration for the pipeline."
    )
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

    @property
    def common(self) -> PipelineCommonCompat:
        """Compatibility shim exposing legacy `config.common` attributes.

        Older test helpers expect `config.common` to surface runtime overrides
        such as `input_file`, `limit`, and `extended`. The modern configuration
        schema captures these under `config.cli`; this property delegates
        attribute access without duplicating state.
        """

        return PipelineCommonCompat(self.cli)

    @model_validator(mode="after")
    def ensure_column_order_when_set(self) -> PipelineConfig:
        if self.determinism.column_order and not self.validation.schema_out:
            msg = (
                "determinism.column_order requires validation.schema_out to be set so the schema "
                "can enforce the order"
            )
            raise ValueError(msg)
        return self


class PipelineCommonCompat:
    """Read-only facade mapping legacy `common` fields onto ``CLIConfig``."""

    __slots__ = ("_cli",)

    def __init__(self, cli: CLIConfig) -> None:
        self._cli = cli

    @property
    def input_file(self) -> str | None:
        return self._cli.input_file

    @property
    def limit(self) -> int | None:
        return self._cli.limit

    @property
    def sample(self) -> int | None:
        return self._cli.sample

    @property
    def extended(self) -> bool:
        return self._cli.extended

    @property
    def dry_run(self) -> bool:
        return self._cli.dry_run

    @property
    def fail_on_schema_drift(self) -> bool:
        return self._cli.fail_on_schema_drift

    @property
    def validate_columns(self) -> bool:
        return self._cli.validate_columns

    @property
    def golden(self) -> str | None:
        return self._cli.golden

    @property
    def verbose(self) -> bool:
        return self._cli.verbose

    def __getattr__(self, item: str) -> Any:
        return getattr(self._cli, item)
