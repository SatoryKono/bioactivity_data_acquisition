from __future__ import annotations

from typing import Any, Mapping

from typing_extensions import TypedDict

from pydantic import BaseModel, ConfigDict, Field


class ChemblAPIConfig(TypedDict, total=False):
    """Параметры HTTP-доступа к ChEMBL API."""

    base_url: str
    timeout_sec: float
    max_retries: int
    backoff_factor: float
    max_backoff_sec: float
    rate_limit_calls: int
    rate_limit_period_sec: float
    cache_enabled: bool
    cache_ttl_sec: int
    circuit_breaker_fail_max: int
    circuit_breaker_reset_sec: int
    default_headers: Mapping[str, str]
    user_agent: str


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
    chembl_api: ChemblAPIConfig | None
    determinism: Mapping[str, Any]
    fail_on_schema_drift: bool


class ChemblPaginationConfigModel(BaseModel):
    """Pydantic-валидация настроек пагинации."""

    model_config = ConfigDict(extra="ignore", frozen=True)

    page_size: int | None = None
    start_page: int | None = None


class ChemblAPIConfigModel(BaseModel):
    """Pydantic-валидация параметров ChEMBL API."""

    model_config = ConfigDict(extra="ignore", frozen=True)

    base_url: str = "https://www.ebi.ac.uk/chembl/api/data"
    timeout_sec: float = 30
    max_retries: int = 3
    backoff_factor: float = 1
    max_backoff_sec: float = 30
    rate_limit_calls: int = 10
    rate_limit_period_sec: float = 1.0
    cache_enabled: bool = True
    cache_ttl_sec: int = 300
    circuit_breaker_fail_max: int = 5
    circuit_breaker_reset_sec: int = 60
    default_headers: Mapping[str, str] = Field(default_factory=dict)
    user_agent: str = "bioetl-chembl-client"


class ChemblPipelineMetadataModel(BaseModel):
    """Pydantic-валидация метаданных ChEMBL-пайплайнов."""

    model_config = ConfigDict(extra="ignore", frozen=True)

    ids: list[str] | None = None
    pagination: ChemblPaginationConfigModel | None = None
    batch_size: int | None = None
    chunk_size: int | None = None
    chembl_release: str | None = None
    chembl_api: ChemblAPIConfigModel | None = None
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
    "ChemblAPIConfig",
    "ChemblAPIConfigModel",
    "ChemblPaginationConfig",
    "ChemblPipelineConfigModel",
    "ChemblPipelineMetadata",
    "ChemblPipelineMetadataModel",
    "ChemblPaginationConfigModel",
    "PipelineConfig",
]
