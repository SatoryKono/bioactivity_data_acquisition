"""Telemetry configuration models."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field, PositiveFloat


class TelemetryConfig(BaseModel):
    """Настройки экспорта метрик и трейсов (OpenTelemetry)."""

    model_config = ConfigDict(extra="forbid")

    enabled: bool = Field(default=False, description="Включить экспорт телеметрии.")
    exporter: str | None = Field(
        default=None,
        description="Тип экспортера (jaeger, otlp, console).",
    )
    endpoint: str | None = Field(
        default=None,
        description="URL или адрес экспортера, если требуется.",
    )
    sampling_ratio: PositiveFloat = Field(
        default=1.0,
        description="Доля выборки трасс (1.0 = 100%).",
    )
