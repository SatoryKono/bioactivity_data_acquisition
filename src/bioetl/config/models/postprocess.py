"""Post-processing configuration models."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


class PostprocessCorrelationConfig(BaseModel):
    """Post-processing options for correlation analysis."""

    model_config = ConfigDict(extra="forbid")

    enabled: bool = Field(
        default=False,
        description="If true, emit correlation reports alongside the dataset.",
    )


class PostprocessConfig(BaseModel):
    """Top-level post-processing configuration."""

    model_config = ConfigDict(extra="forbid")

    correlation: PostprocessCorrelationConfig = Field(
        default_factory=PostprocessCorrelationConfig,
        description="Correlation report controls.",
    )
