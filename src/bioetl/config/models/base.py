"""Base pipeline configuration models."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import MISSING, fields, is_dataclass
from typing import Any, Literal, Protocol, Type, TypeVar

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

SourceConfigModelT = TypeVar("SourceConfigModelT", bound=BaseModel)
SourceParametersT = TypeVar("SourceParametersT", bound="SourceParametersProtocol")


class SourceParametersProtocol(Protocol):
    @classmethod
    def from_mapping(
        cls: Type[SourceParametersT], params: Mapping[str, Any]
    ) -> SourceParametersT:
        ...


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

    @model_validator(mode="after")
    def ensure_column_order_when_set(self) -> PipelineConfig:
        if self.determinism.column_order and not self.validation.schema_out:
            msg = (
                "determinism.column_order requires validation.schema_out to be set so the schema "
                "can enforce the order"
            )
            raise ValueError(msg)
        return self


def _resolve_batch_field(cls: Type[Any]) -> str:
    if hasattr(cls, "model_fields"):
        model_fields = getattr(cls, "model_fields")
        if "batch_size" in model_fields:
            return "batch_size"
        if "page_size" in model_fields:
            return "page_size"
    if is_dataclass(cls):
        cls_fields = {field.name for field in fields(cls)}
        if "batch_size" in cls_fields:
            return "batch_size"
        if "page_size" in cls_fields:
            return "page_size"
    return "batch_size"


def _extract_int(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    return None


def _default_batch_size(cls: Type[Any], field_name: str, fallback: int) -> int:
    if hasattr(cls, "model_fields"):
        field = getattr(cls, "model_fields").get(field_name)
        if field is not None:
            default = getattr(field, "default", None)
            default_int = _extract_int(default)
            if default_int is not None:
                return default_int
    if is_dataclass(cls):
        for field in fields(cls):
            if field.name == field_name:
                default_raw = field.default
                if default_raw is not MISSING:
                    default_int = _extract_int(default_raw)
                    if default_int is not None:
                        return default_int
                if field.default_factory is not MISSING:
                    candidate_raw = field.default_factory()
                    candidate_int = _extract_int(candidate_raw)
                    if candidate_int is not None:
                        return candidate_int
    return fallback


def build_source_config(
    cls: Type[SourceConfigModelT],
    params_cls: Type[SourceParametersT],
    config: SourceConfig,
) -> SourceConfigModelT:
    batch_field = _resolve_batch_field(cls)
    default_batch = _default_batch_size(cls, batch_field, fallback=25)
    batch_value = getattr(config, "batch_size", None)
    if batch_value is None:
        batch_value = default_batch

    parameters = params_cls.from_mapping(getattr(config, "parameters"))

    payload: dict[str, Any] = {
        "enabled": getattr(config, "enabled"),
        "description": getattr(config, "description"),
        "http_profile": getattr(config, "http_profile"),
        "http": getattr(config, "http"),
        "parameters": parameters,
    }
    payload[batch_field] = batch_value

    return cls(**payload)
