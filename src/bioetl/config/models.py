from __future__ import annotations

from typing import Any, Mapping, TypedDict

from pydantic import BaseModel, ConfigDict


class ChemblPaginationConfig(TypedDict, total=False):
    """Настройки пагинации для вызовов ChEMBL."""

    page_size: int | None
    start_page: int | None


class ChemblPipelineMetadata(TypedDict, total=False):
    """Общая форма конфигурации для ChEMBL-пайплайнов."""

    ids: list[str] | None
    pagination: ChemblPaginationConfig | None
    batch_size: int | None
    chunk_size: int | None
    chembl_release: str | None
    determinism: Mapping[str, Any]
    fail_on_schema_drift: bool


class ChemblPaginationConfigModel(BaseModel):
    """Pydantic-валидация настроек пагинации."""

    model_config = ConfigDict(extra="ignore", frozen=True)

    page_size: int | None = None
    start_page: int | None = None


class ChemblPipelineMetadataModel(BaseModel):
    """Pydantic-валидация метаданных ChEMBL-пайплайнов."""

    model_config = ConfigDict(extra="ignore", frozen=True)

    ids: list[str] | None = None
    pagination: ChemblPaginationConfigModel | None = None
    batch_size: int | None = None
    chunk_size: int | None = None
    chembl_release: str | None = None
    determinism: Mapping[str, Any] | None = None
    fail_on_schema_drift: bool | None = None


class PipelineConfig(BaseModel):
    """Lightweight pipeline configuration wrapper.

    The model accepts arbitrary keys so that pipeline-specific options can be
    expressed without having to update the core schema for every change.
    """

    model_config = ConfigDict(extra="allow", frozen=True)

    name: str | None = None
    metadata: ChemblPipelineMetadata | ChemblPipelineMetadataModel | None = None


class ChemblPipelineConfigModel(BaseModel):
    """Общий контейнер для конфигураций ChEMBL-пайплайнов."""

    model_config = ConfigDict(extra="allow", frozen=True)

    name: str | None = None
    metadata: ChemblPipelineMetadataModel | None = None


__all__ = [
    "ChemblPaginationConfig",
    "ChemblPipelineConfigModel",
    "ChemblPipelineMetadata",
    "ChemblPipelineMetadataModel",
    "ChemblPaginationConfigModel",
    "PipelineConfig",
]
