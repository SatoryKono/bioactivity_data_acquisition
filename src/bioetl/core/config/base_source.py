"""Base configuration models for data sources."""
from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Callable, ClassVar, Generic, TypeVar, cast

from pydantic import BaseModel, ConfigDict, Field
from pydantic_core import PydanticUndefined

from bioetl.config.models.http import HTTPClientConfig
from bioetl.config.models.source import SourceConfig


class BaseSourceParameters(BaseModel):
    """Base parameter model for a source with strict field validation."""

    model_config = ConfigDict(extra="forbid")

    @classmethod
    def from_mapping(cls: type[ParametersT], params: Mapping[str, Any] | None) -> ParametersT:
        """Create parameters from an arbitrary mapping."""

        if params is None:
            return cls()
        normalized = cls._normalize_mapping(params)
        return cls(**normalized)

    @staticmethod
    def _normalize_mapping(params: Mapping[str, Any]) -> dict[str, Any]:
        """Return a copy of the mapping with keys coerced to strings."""

        return {str(key): value for key, value in params.items()}


ParametersT = TypeVar("ParametersT", bound=BaseSourceParameters)
SelfConfigT = TypeVar("SelfConfigT", bound="BaseSourceConfig[Any]")


class BaseSourceConfig(BaseModel, Generic[ParametersT]):
    """Base configuration model for external data sources."""

    model_config = ConfigDict(extra="forbid")

    enabled: bool = Field(default=True)
    description: str | None = Field(default=None)
    http_profile: str | None = Field(default=None)
    http: HTTPClientConfig | None = Field(default=None)
    parameters: ParametersT

    #: Parameter model type used when building configurations.
    parameters_model: ClassVar[type[BaseSourceParameters]] = BaseSourceParameters
    #: Field name that stores batch size; ``None`` disables batch handling.
    batch_field: ClassVar[str | None] = "batch_size"
    #: Hard default for batch size when the model omits an explicit default.
    default_batch_size: ClassVar[int | None] = None

    @classmethod
    def from_source_config(
        cls: type[SelfConfigT],
        config: SourceConfig,
    ) -> SelfConfigT:
        """Build a specialized configuration object from the generic source config."""

        parameters = cls._build_parameters(config.parameters)
        payload = cls._build_payload(config=config, parameters=parameters)
        return cls(**payload)

    @classmethod
    def _build_parameters(cls, params: Mapping[str, Any] | None) -> ParametersT:
        if not hasattr(cls, "parameters_model"):
            msg = f"{cls.__name__}.parameters_model is not configured"
            raise TypeError(msg)
        parameters_cls = cast(type[ParametersT], cls.parameters_model)
        return parameters_cls.from_mapping(params or {})

    @classmethod
    def _build_payload(
        cls,
        *,
        config: SourceConfig,
        parameters: ParametersT,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "enabled": config.enabled,
            "description": config.description,
            "http_profile": config.http_profile,
            "http": config.http,
            "parameters": parameters,
        }

        batch_field = cls.batch_field
        if batch_field:
            batch_value = cls._resolve_batch_value(config=config, parameters=parameters)
            if batch_value is not None:
                payload[batch_field] = batch_value

        return payload

    @classmethod
    def _resolve_batch_value(
        cls,
        *,
        config: SourceConfig,
        parameters: ParametersT,  # noqa: ARG003 - consumed by subclass overrides
    ) -> int | None:
        if config.batch_size is not None:
            return int(config.batch_size)
        return cls._default_batch_value()

    @classmethod
    def _default_batch_value(cls) -> int | None:
        if cls.default_batch_size is not None:
            return cls.default_batch_size

        batch_field = cls.batch_field
        if not batch_field:
            return None

        field_info = cls.model_fields.get(batch_field)
        if field_info is None:
            return None

        default_value = field_info.default
        if default_value is not None and default_value is not PydanticUndefined:
            return cast(int, default_value)

        default_factory = field_info.default_factory
        if default_factory is not None:
            produced = cast(Callable[[], Any], default_factory)()
            if isinstance(produced, int):
                return produced
        return None

