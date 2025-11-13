"""Telemetry configuration models."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field, PositiveFloat


class TelemetryConfig(BaseModel):
    """Telemetry export configuration (OpenTelemetry)."""

    model_config = ConfigDict(extra="forbid")

    enabled: bool = Field(default=False, description="Enable telemetry export.")
    exporter: str | None = Field(
        default=None,
        description="Exporter type (jaeger, otlp, console).",
    )
    endpoint: str | None = Field(
        default=None,
        description="Exporter URL or address, when required.",
    )
    sampling_ratio: PositiveFloat = Field(
        default=1.0,
        description="Sampling ratio for traces (1.0 = 100%).",
    )
