"""Domain-specific configuration sections for pipelines."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from .fallbacks import FallbacksConfig
from .postprocess import PostprocessConfig
from .source import SourceConfig
from .transform import TransformConfig
from .validation import ValidationConfig


class PipelineDomainConfig(BaseModel):
    """Pure domain knobs: schemas, transforms, enrichments, and business metadata."""

    model_config = ConfigDict(extra="forbid")

    validation: ValidationConfig = Field(
        default_factory=ValidationConfig,
        description="Schema registry bindings and validation toggles.",
    )
    transform: TransformConfig = Field(
        default_factory=TransformConfig,
        description="Declarative data transformation directives.",
    )
    postprocess: PostprocessConfig = Field(
        default_factory=PostprocessConfig,
        description="Domain-level post processing (hashing, QC adjustments, joins).",
    )
    fallbacks: FallbacksConfig = Field(
        default_factory=FallbacksConfig,
        description="Business fallback logic for missing attributes or overrides.",
    )
    sources: dict[str, SourceConfig] = Field(
        default_factory=dict,
        description="Per-source toggles and metadata keyed by provider identifier.",
    )
    chembl: Mapping[str, Any] | None = Field(
        default=None,
        description="ChEMBL domain configuration, e.g., descriptor enrichment hints.",
    )


