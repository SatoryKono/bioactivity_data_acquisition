"""Runtime execution configuration models."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field, PositiveInt


class RuntimeConfig(BaseModel):
    """Controls execution-level parameters shared across pipelines."""

    model_config = ConfigDict(extra="forbid")

    parallelism: PositiveInt = Field(
        default=4,
        description="Количество параллельных воркеров для этапов extract/transform.",
    )
    chunk_rows: PositiveInt = Field(
        default=100_000,
        description="Размер чанка строк для пакетной обработки источников.",
    )
    dry_run: bool = Field(
        default=False,
        description="Режим проверки без записи артефактов во внешние системы.",
    )
    seed: int = Field(
        default=42,
        description="Инициализационное значение генераторов случайных чисел для детерминизма.",
    )
