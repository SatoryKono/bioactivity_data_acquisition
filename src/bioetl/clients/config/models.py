from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class AuthConfig(BaseModel):
    """Аутентификация для REST-источника."""

    model_config = ConfigDict(extra="forbid")

    type: Literal["none", "api_key", "bearer", "basic", "custom"] = "none"
    header_name: str | None = None
    query_param: str | None = None
    token_env: str | None = None


class RateLimitConfig(BaseModel):
    """Описание rate-limit в запросах."""

    model_config = ConfigDict(extra="forbid")

    requests_per_minute: int | None = None
    burst: int | None = None


class PagingConfig(BaseModel):
    """Конфигурация пагинации для эндпоинта."""

    model_config = ConfigDict(extra="forbid")

    type: Literal["none", "page", "offset", "cursor", "link"] = "none"
    page_param: str | None = None
    page_size_param: str | None = None
    default_page_size: int | None = None
    max_page_size: int | None = None
    cursor_param: str | None = None
    next_link_path: str | None = None
    offset_param: str | None = None
    limit_param: str | None = None

    @model_validator(mode="after")
    def _validate_required_keys(self) -> "PagingConfig":
        if self.type == "page" and not self.page_param:
            msg = "page_param обязателен для пагинации типа 'page'"
            raise ValueError(msg)
        if self.type == "offset" and not self.offset_param:
            msg = "offset_param обязателен для пагинации типа 'offset'"
            raise ValueError(msg)
        if self.type == "cursor" and not self.cursor_param:
            msg = "cursor_param обязателен для пагинации типа 'cursor'"
            raise ValueError(msg)
        if self.type == "link" and not self.next_link_path:
            msg = "next_link_path обязателен для пагинации типа 'link'"
            raise ValueError(msg)
        return self


class QueryConfig(BaseModel):
    """Конфигурация query-параметров эндпоинта."""

    model_config = ConfigDict(extra="forbid")

    fixed: Mapping[str, Any] = Field(default_factory=dict)
    allowed_params: list[str] = Field(default_factory=list)
    rename: Mapping[str, str] = Field(default_factory=dict)


class FieldConfig(BaseModel):
    """Описание поля ответа."""

    model_config = ConfigDict(extra="forbid")

    name: str
    path: str
    type: Literal["str", "int", "float", "bool", "any"] = "any"


class ResponseConfig(BaseModel):
    """Схема ответа REST-эндпоинта."""

    model_config = ConfigDict(extra="forbid")

    format: Literal["json", "xml"] = "json"
    record_path: str | None = None
    fields: list[FieldConfig] = Field(default_factory=list)
    extra_metadata: list[FieldConfig] = Field(default_factory=list)


class ResourceConfig(BaseModel):
    """Конфигурация отдельного REST-ресурса."""

    model_config = ConfigDict(extra="forbid")

    path: str
    method: Literal["GET", "POST", "PUT", "DELETE", "PATCH"] = "GET"
    headers: Mapping[str, str] = Field(default_factory=dict)
    auth: AuthConfig | None = None
    query: QueryConfig = Field(default_factory=QueryConfig)
    paging: PagingConfig = Field(default_factory=PagingConfig)
    response: ResponseConfig = Field(default_factory=ResponseConfig)

    @field_validator("path")
    @classmethod
    def _ensure_leading_slash(cls, value: str) -> str:
        if not value.startswith("/"):
            return f"/{value}"
        return value


class SourceConfig(BaseModel):
    """Базовая конфигурация REST-источника."""

    model_config = ConfigDict(extra="forbid")

    source: str
    protocol: Literal["http"] = "http"
    base_url: str
    default_timeout: float | None = 30.0
    rate_limit: RateLimitConfig | None = None
    auth: AuthConfig = Field(default_factory=AuthConfig)
    headers: Mapping[str, str] = Field(default_factory=dict)
    resources: Mapping[str, ResourceConfig]

    @field_validator("base_url")
    @classmethod
    def _strip_trailing_slash(cls, value: str) -> str:
        return value[:-1] if value.endswith("/") else value

    @model_validator(mode="after")
    def _ensure_resources_present(self) -> "SourceConfig":
        if not self.resources:
            msg = "resources должны быть заданы хотя бы для одного эндпоинта"
            raise ValueError(msg)
        return self


__all__ = [
    "AuthConfig",
    "RateLimitConfig",
    "PagingConfig",
    "QueryConfig",
    "FieldConfig",
    "ResponseConfig",
    "ResourceConfig",
    "SourceConfig",
]
