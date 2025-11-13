"""Общая обёртка для типизированных конфигов источников ChEMBL."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, ClassVar, Generic, TypeVar, cast

from pydantic import BaseModel, ConfigDict, Field, PositiveInt, model_validator

from bioetl.config.models.models import SourceConfig
from bioetl.config.models.policies import HTTPClientConfig

from .common.source_adapter import (
    extract_allowed_parameters,
    normalize_base_url,
    normalize_select_fields,
)
from .utils import coerce_bool

ParamsT = TypeVar("ParamsT", bound="BaseSourceParameters")


@dataclass(frozen=True, slots=True)
class SourceConfigDefaults:
    """Стандартные значения и ограничения для пайплайновых конфигов."""

    page_size: int = 25
    page_size_cap: int = 25
    max_url_length: int = 2000
    max_url_length_cap: int = 2000
    handshake_endpoint: str = "/status.json"
    handshake_enabled: bool = True


class BaseSourceParameters(BaseModel):
    """Базовые параметры источника с общими полями."""

    model_config = ConfigDict(extra="forbid")
    allowed_fields: ClassVar[tuple[str, ...]] = ("base_url", "select_fields")

    base_url: str | None = Field(
        default=None,
        description="Базовый URL для ChEMBL API. Если None, используется значение из профиля.",
    )
    select_fields: tuple[str, ...] | None = Field(
        default=None,
        description="Список полей для параметра `only`. Значение None оставляет дефолт API.",
    )

    @classmethod
    def from_mapping(cls, params: Mapping[str, Any]) -> BaseSourceParameters:
        """Создать параметры из произвольного словаря."""

        allowed = extract_allowed_parameters(params, cls.allowed_fields)
        return cls(
            base_url=normalize_base_url(allowed.get("base_url")),
            select_fields=normalize_select_fields(allowed.get("select_fields")),
        )


class ChemblPipelineSourceConfig(BaseModel, Generic[ParamsT]):
    """Унифицированный конфиг источника для ChEMBL-пайплайнов."""

    model_config = ConfigDict(extra="forbid")

    enabled: bool = Field(default=True, description="Включен ли источник.")
    description: str | None = Field(
        default=None,
        description="Человекочитаемое описание источника.",
    )
    http_profile: str | None = Field(
        default=None,
        description="Имя HTTP-профиля из конфигурации.",
    )
    http: HTTPClientConfig | None = Field(
        default=None,
        description="Inline-переопределения HTTP-параметров (приоритетнее профиля).",
    )
    page_size: PositiveInt = Field(
        default=25,
        description="Размер страницы для пагинации (каппинг по ограничениям ChEMBL).",
    )
    max_url_length: PositiveInt = Field(
        default=2000,
        description="Максимальная допустимая длина URL для батчевых операций.",
    )
    handshake_endpoint: str = Field(
        default="/status.json",
        description="Endpoint для handshake перед экстракцией.",
    )
    handshake_enabled: bool = Field(
        default=True,
        description="Флаг выполнения handshake перед экстракцией.",
    )
    parameters: ParamsT

    defaults: ClassVar[SourceConfigDefaults] = SourceConfigDefaults()
    parameters_model: ClassVar[type[BaseSourceParameters]] = BaseSourceParameters

    @property
    def batch_size(self) -> int:
        """Совместимость с историческим именованием."""

        return self.page_size

    @model_validator(mode="after")
    def _apply_limits(self) -> ChemblPipelineSourceConfig[ParamsT]:
        """Pydantic-валидатор для применения ограничений."""

        self._clamp_limits()
        return self

    def enforce_limits(self) -> ChemblPipelineSourceConfig[ParamsT]:
        """Идемпотентное применение ограничений после инициализации."""

        self._clamp_limits()
        return self

    @classmethod
    def from_source(cls, config: SourceConfig) -> ChemblPipelineSourceConfig[ParamsT]:
        """Сконструировать пайплайновый конфиг из общих SourceConfig-данных."""

        params_mapping: dict[str, Any] = dict(config.parameters)
        handshake_endpoint = cls._resolve_handshake_endpoint(
            params_mapping.pop("handshake_endpoint", None),
        )
        handshake_enabled = cls._resolve_handshake_enabled(
            params_mapping.pop("handshake_enabled", None),
        )
        max_url_length = cls._resolve_max_url_length(
            params_mapping.pop("max_url_length", None),
        )
        page_size = cls._resolve_page_size(
            config_batch_size=config.batch_size,
            parameter_batch_size=params_mapping.pop("batch_size", None),
            parameter_page_size=params_mapping.pop("page_size", None),
        )

        parameters_model: type[BaseSourceParameters] = getattr(
            cls, "parameters_model", BaseSourceParameters
        )
        parameters = cast(ParamsT, parameters_model.from_mapping(params_mapping))

        return cls(
            enabled=config.enabled,
            description=config.description,
            http_profile=config.http_profile,
            http=config.http,
            page_size=page_size,
            max_url_length=max_url_length,
            handshake_endpoint=handshake_endpoint,
            handshake_enabled=handshake_enabled,
            parameters=parameters,
        )

    @classmethod
    def from_source_config(cls, config: SourceConfig) -> ChemblPipelineSourceConfig[ParamsT]:
        """Обратная совместимость. Используйте `from_source`."""

        return cls.from_source(config)

    def _clamp_limits(self) -> None:
        defaults = self.defaults
        if self.page_size > defaults.page_size_cap:
            self.page_size = defaults.page_size_cap
        if self.max_url_length > defaults.max_url_length_cap:
            self.max_url_length = defaults.max_url_length_cap

    @classmethod
    def _resolve_page_size(
        cls,
        *,
        config_batch_size: int | None,
        parameter_batch_size: Any,
        parameter_page_size: Any,
    ) -> int:
        defaults = cls.defaults
        cap = defaults.page_size_cap
        candidates: tuple[Any, ...] = (
            config_batch_size,
            parameter_page_size,
            parameter_batch_size,
        )
        for candidate in candidates:
            if candidate is None:
                continue
            try:
                value = int(candidate)
            except (TypeError, ValueError):
                continue
            if value > 0:
                return min(value, cap)
        return min(defaults.page_size, cap)

    @classmethod
    def _resolve_max_url_length(cls, raw: Any) -> int:
        defaults = cls.defaults
        cap = defaults.max_url_length_cap
        if raw is None:
            return min(defaults.max_url_length, cap)
        try:
            value = int(raw)
        except (TypeError, ValueError) as exc:
            msg = "max_url_length должен приводиться к положительному целому"
            raise ValueError(msg) from exc
        if value <= 0:
            msg = "max_url_length должен быть положительным целым"
            raise ValueError(msg)
        return min(value, cap)

    @classmethod
    def _resolve_handshake_endpoint(cls, raw: Any) -> str:
        default_endpoint = cls.defaults.handshake_endpoint
        if raw is None:
            return default_endpoint
        candidate = str(raw).strip()
        return candidate or default_endpoint

    @classmethod
    def _resolve_handshake_enabled(cls, raw: Any) -> bool:
        if raw is None:
            return cls.defaults.handshake_enabled
        return coerce_bool(raw)


__all__ = [
    "BaseSourceParameters",
    "ChemblPipelineSourceConfig",
    "ParamsT",
    "SourceConfigDefaults",
]

