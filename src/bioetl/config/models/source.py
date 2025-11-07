"""Source configuration models."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, PositiveInt

from .http import HTTPClientConfig


class SourceConfig(BaseModel):
    """Per-source overrides and metadata."""

    model_config = ConfigDict(extra="forbid")

    enabled: bool = Field(default=True, description="Toggle processing of this data source.")
    description: str | None = Field(
        default=None, description="Human readable description of the source."
    )
    http_profile: str | None = Field(
        default=None,
        description="Reference to a named HTTP profile defined under http.profiles.",
    )
    http: HTTPClientConfig | None = Field(
        default=None,
        description="Inline HTTP overrides that take precedence over profile settings.",
    )
    batch_size: PositiveInt | None = Field(
        default=None,
        description="Batch size used when paginating requests for this source.",
    )
    parameters: Mapping[str, Any] = Field(
        default_factory=dict,
        description="Free-form parameters consumed by source-specific components.",
    )
