"""Strict Pydantic models for pipeline configuration validation."""

from __future__ import annotations

from pydantic import ConfigDict, Field, FieldValidationInfo, field_validator

from bioetl.config.models import (
    CacheConfig,
    CliConfig,
    DeterminismConfig,
    FallbackOptions,
    HttpConfig,
    MaterializationDatasetPaths,
    MaterializationFormatConfig,
    MaterializationPaths,
    MaterializationStagePaths,
    PathConfig,
    PipelineMetadata,
    PipelineConfig as _LegacyPipelineConfig,
    PostprocessConfig,
    QCConfig,
    RateLimitConfig,
    RetryConfig,
    SortConfig,
    TargetSourceConfig,
)

__all__ = [
    "CacheConfig",
    "CliConfig",
    "DeterminismConfig",
    "FallbackOptions",
    "HttpConfig",
    "MaterializationDatasetPaths",
    "MaterializationFormatConfig",
    "MaterializationPaths",
    "MaterializationStagePaths",
    "PathConfig",
    "PipelineConfig",
    "PipelineMetadata",
    "PostprocessConfig",
    "QCConfig",
    "RateLimitConfig",
    "RetryConfig",
    "SortConfig",
    "Source",
]


class Source(TargetSourceConfig):
    """Strict wrapper around :class:`TargetSourceConfig` disallowing unknown keys."""

    model_config = ConfigDict(extra="forbid")

    tool: str | None = None
    email: str | None = None
    mailto: str | None = None
    rate_limit_max_calls: int | None = Field(default=None, ge=1)
    rate_limit_period: float | None = Field(default=None, gt=0)
    workers: int | None = Field(default=None, ge=1)

    @field_validator("tool", "email", "mailto", mode="before")
    @classmethod
    def resolve_contact_secrets(
        cls,
        value: str | None,
        info: FieldValidationInfo,
    ) -> str | None:  # type: ignore[override]
        """Resolve ``env:VAR``/``${VAR}`` references for contact metadata."""

        if value is None:
            return None

        if not isinstance(value, str):
            return value

        candidate = value.strip()
        if not candidate:
            return value

        if candidate.startswith("env:"):
            reference = candidate.split(":", 1)[1]
            resolved = cls._resolve_api_key_reference(reference)
            if resolved is None:
                raise ValueError(
                    f"Environment variable '{reference.split(':', 1)[0]}' referenced for {info.field_name} is not set",
                )
            return resolved

        if candidate.startswith("${") and candidate.endswith("}"):
            reference = candidate[2:-1]
            resolved = cls._resolve_api_key_reference(reference)
            if resolved is None:
                raise ValueError(
                    f"Environment variable '{reference.split(':', 1)[0]}' referenced for {info.field_name} is not set",
                )
            return resolved

        return value


class PipelineConfig(_LegacyPipelineConfig):
    """Re-export of the legacy pipeline config with strict source typing."""

    model_config = ConfigDict(extra="forbid", use_enum_values=True)

    sources: dict[str, Source] = Field(default_factory=dict)
