"""Logging configuration models."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


class LoggingConfig(BaseModel):
    """Определяет параметры структурного логирования."""

    model_config = ConfigDict(extra="forbid")

    level: str = Field(default="INFO", description="Уровень логирования UnifiedLogger.")
    format: str = Field(
        default="json",
        description="Формат логов (json, console).",
    )
    with_timestamps: bool = Field(
        default=True,
        description="Включать ли UTC временные метки в вывод логов.",
    )
    context_fields: tuple[str, ...] = Field(
        default_factory=lambda: ("pipeline", "run_id"),
        description="Обязательные поля контекста, которые должны присутствовать в каждом сообщении.",
    )
