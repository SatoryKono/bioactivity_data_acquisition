"""Input/output configuration models."""

from __future__ import annotations

from collections.abc import Sequence

from pydantic import BaseModel, ConfigDict, Field


class IOInputConfig(BaseModel):
    """Настройки чтения входных ресурсов."""

    model_config = ConfigDict(extra="forbid")

    format: str = Field(default="csv", description="Формат входного файла (csv, parquet, json).")
    encoding: str = Field(default="utf-8", description="Кодировка входных файлов.")
    header: bool = Field(default=True, description="Наличие строки заголовков во входном файле.")
    path: str | None = Field(
        default=None,
        description="Явный путь к локальному входному файлу, если он задан.",
    )


class IOOutputConfig(BaseModel):
    """Настройки сериализации итоговых артефактов."""

    model_config = ConfigDict(extra="forbid")

    format: str = Field(default="parquet", description="Формат итоговых данных (parquet, csv).")
    partition_by: Sequence[str] = Field(
        default_factory=tuple,
        description="Список колонок для партиционирования набора данных.",
    )
    overwrite: bool = Field(
        default=True,
        description="Разрешить перезапись ранее существующих артефактов.",
    )
    path: str | None = Field(
        default=None,
        description="Явный путь к выходному файлу или директории.",
    )


class IOConfig(BaseModel):
    """Единая конфигурация ввода/вывода для пайплайна."""

    model_config = ConfigDict(extra="forbid")

    input: IOInputConfig = Field(default_factory=IOInputConfig)
    output: IOOutputConfig = Field(default_factory=IOOutputConfig)
