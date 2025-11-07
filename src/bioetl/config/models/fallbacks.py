"""Fallback configuration models."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field, PositiveInt


class FallbacksConfig(BaseModel):
    """Global behavior toggles for fallback mechanisms."""

    model_config = ConfigDict(extra="forbid")

    enabled: bool = Field(
        default=True,
        description="Whether the pipeline should attempt configured fallback strategies.",
    )
    max_depth: PositiveInt | None = Field(
        default=None,
        description="Maximum fallback depth allowed before failing fast.",
    )
