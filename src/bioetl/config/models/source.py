"""Source configuration models."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, ClassVar, Generic, TypeVar, cast

from pydantic import BaseModel, ConfigDict, Field, PositiveInt
from pydantic_core import PydanticUndefined

from .http import HTTPClientConfig

ParametersT = TypeVar("ParametersT", bound="SourceParameters")
SelfSourceConfigT = TypeVar("SelfSourceConfigT", bound="SourceConfig[Any]")


class SourceParameters(BaseModel):
    """Base parameter model for per-source overrides."""

    model_config = ConfigDict(extra="allow")

    @classmethod
    def from_mapping(cls: type[ParametersT], params: Mapping[str, Any] | None) -> ParametersT:
        """Build a parameter model from an arbitrary mapping."""

        if params is None:
            return cls()
        normalized = cls._normalize_mapping(params)
        return cls(**normalized)

    @staticmethod
    def _normalize_mapping(params: Mapping[str, Any]) -> dict[str, Any]:
        """Return a new mapping with stringified keys."""

        return {str(key): value for key, value in params.items()}


class SourceConfig(BaseModel, Generic[ParametersT]):
    """Unified per-source configuration model for pipelines and clients."""

    model_config = ConfigDict(extra="forbid")

    enabled: bool = Field(default=True, description="Toggle processing of this data source.")
    description: str | None = Field(
        default=None, description="Human readable description of the source."
    )
    http_profile: str | None = Field(
        default=None,
        description="Reference to a named HTTP profile defined under http.profiles.",
    )
    http: HTTPClientConfig | None = Field(
        default=None,
        description="Inline HTTP overrides that take precedence over profile settings.",
    )
    batch_size: PositiveInt | None = Field(
        default=None,
        description="Batch size used when paginating requests for this source.",
    )
    parameters: ParametersT = Field(
        default_factory=SourceParameters,
        description="Free-form parameters consumed by source-specific components.",
    )

    parameters_model: ClassVar[type[SourceParameters] | None] = None
    batch_field: ClassVar[str | None] = None
    default_batch_size: ClassVar[int | None] = None

    # ------------------------------------------------------------------ #
    # Factory helpers
    # ------------------------------------------------------------------ #

    @classmethod
    def from_source_config(cls: type[SelfSourceConfigT], config: "SourceConfig[Any]") -> SelfSourceConfigT:
        """Build a specialized configuration object from a generic instance."""

        if isinstance(config, cls):
            return config
        parameters = cls._build_parameters(config.parameters)
        payload = cls._build_payload(config=config, parameters=parameters)
        return cls(**payload)

    @classmethod
    def _build_parameters(cls, params: SourceParameters | Mapping[str, Any] | None) -> ParametersT:
        model = cls.parameters_model
        if model is None:
            if params is None:
                return cast(ParametersT, SourceParameters())
            if isinstance(params, SourceParameters):
                return cast(ParametersT, params)
            return cast(ParametersT, SourceParameters(**SourceParameters._normalize_mapping(params)))
        if params is None:
            return model()
        if isinstance(params, model):
            return params
        if isinstance(params, SourceParameters):
            return model(**params.model_dump())
        if isinstance(params, Mapping):
            return model.from_mapping(params)
        as_dict = getattr(params, "model_dump", None)
        if callable(as_dict):
            dumped = as_dict()
            if isinstance(dumped, Mapping):
                return model.from_mapping(dumped)
        raise TypeError(
            f"{cls.__name__} parameters must be mapping-compatible; received {type(params)!r}"
        )

    @classmethod
    def _build_payload(
        cls,
        *,
        config: "SourceConfig[Any]",
        parameters: Any,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "enabled": config.enabled,
            "description": config.description,
            "http_profile": config.http_profile,
            "http": config.http,
            "batch_size": config.batch_size,
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
        config: "SourceConfig[Any]",
        parameters: Any,  # noqa: ARG003 - hook for subclass overrides
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
            produced = default_factory()
            if isinstance(produced, int):
                return produced
        return None

    # ------------------------------------------------------------------ #
    # Convenience helpers
    # ------------------------------------------------------------------ #

    def parameters_mapping(self) -> dict[str, Any]:
        """Return parameters as a deterministic mapping."""

        dumped = self.parameters.model_dump()
        return {str(key): value for key, value in dumped.items()}

    def get_parameter(self, key: str, default: Any | None = None) -> Any | None:
        """Return a parameter value with a fallback."""

        return self.parameters_mapping().get(key, default)

    def resolve_effective_batch_size(
        self,
        *,
        default: int | None = None,
        hard_cap: int | None = None,
    ) -> int | None:
        """Return an effective batch size honoring defaults and caps."""

        candidate: int | None = None
        explicit = getattr(self, "batch_size", None)
        if explicit is not None:
            candidate = int(explicit)
        elif default is not None:
            candidate = int(default)

        if candidate is None:
            return None

        if hard_cap is not None:
            candidate = min(candidate, hard_cap)
        return max(candidate, 1)
