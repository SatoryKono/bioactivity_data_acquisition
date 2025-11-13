"""Logging configuration models."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


class LoggingConfig(BaseModel):
    """Structured logging configuration."""

    model_config = ConfigDict(extra="forbid")

    level: str = Field(default="INFO", description="Log level for UnifiedLogger.")
    format: str = Field(
        default="json",
        description="Log format (json, console).",
    )
    with_timestamps: bool = Field(
        default=True,
        description="Whether to include UTC timestamps in log output.",
    )
    context_fields: tuple[str, ...] = Field(
        default_factory=lambda: ("pipeline", "run_id"),
        description="Required context fields that must appear in every log record.",
    )
