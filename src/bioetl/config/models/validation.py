"""Validation configuration models."""

from __future__ import annotations

from collections.abc import Sequence

from pydantic import BaseModel, ConfigDict, Field


class ValidationConfig(BaseModel):
    """Pandera schema configuration."""

    model_config = ConfigDict(extra="allow")

    schema_in: str | None = Field(
        default=None,
        description="Dotted path to the Pandera schema validating extracted data.",
    )
    schema_out: str | None = Field(
        default=None,
        description="Dotted path to the Pandera schema validating transformed data.",
    )
    strict: bool = Field(default=True, description="If true, Pandera enforces column order and presence.")
    coerce: bool = Field(default=True, description="If true, Pandera coerces data types during validation.")
    data_validity_comment_whitelist: Sequence[str] | None = Field(
        default=None,
        description="Whitelist of allowed values for data_validity_comment (soft enum validation).",
    )

