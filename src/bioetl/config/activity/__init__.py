"""Typed configuration helpers for the ChEMBL activity pipeline."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, cast

from pydantic import BaseModel, ConfigDict, Field, PositiveInt, model_validator

from ..models.base import build_source_config
from ..models.http import HTTPClientConfig
from ..models.source import SourceConfig


class ActivitySourceParameters(BaseModel):
    """Free-form parameters specific to the activity source."""

    model_config = ConfigDict(extra="forbid")

    base_url: str | None = Field(
        default=None,
        description="Override for the ChEMBL API base URL when fetching activities.",
    )
    select_fields: Sequence[str] | None = Field(
        default=None,
        description="Optional list of field names to fetch via `only` parameter. If not provided, uses default API_ACTIVITY_FIELDS.",
    )

    @classmethod
    def from_mapping(cls, params: Mapping[str, Any]) -> ActivitySourceParameters:
        """Construct the parameters object from a raw mapping.

        Parameters
        ----------
        params
            Raw mapping of parameters.

        Returns
        -------
        ActivitySourceParameters
            Constructed parameters object.
        """
        select_fields_raw = params.get("select_fields")
        select_fields: Sequence[str] | None = None
        if select_fields_raw is not None:
            if isinstance(select_fields_raw, Sequence) and not isinstance(
                select_fields_raw, (str, bytes)
            ):
                # Type narrowing: after isinstance check, select_fields_raw is Sequence[Any]
                # Явно типизируем элементы для избежания предупреждений типизации
                # basedpyright не может вывести тип field из Sequence[Any]
                # Используем генератор списка с явной типизацией через cast
                select_fields = tuple(str(cast(Any, field)) for field in select_fields_raw)  # pyright: ignore[reportUnknownVariableType]

        return cls(
            base_url=params.get("base_url"),
            select_fields=select_fields,
        )


class ActivitySourceConfig(BaseModel):
    """Pipeline-specific view over the generic :class:`SourceConfig`."""

    model_config = ConfigDict(extra="forbid")

    enabled: bool = Field(default=True)
    description: str | None = Field(default=None)
    http_profile: str | None = Field(default=None)
    http: HTTPClientConfig | None = Field(default=None)
    batch_size: PositiveInt = Field(
        default=25,
        description="Effective batch size for pagination requests (capped at 25).",
    )
    parameters: ActivitySourceParameters = Field(default_factory=ActivitySourceParameters)

    @model_validator(mode="after")
    def enforce_limits(self) -> ActivitySourceConfig:
        """Ensure the configured values adhere to documented constraints.

        Returns
        -------
        ActivitySourceConfig
            Self with enforced limits.
        """
        if self.batch_size > 25:
            self.batch_size = 25
        return self

    @classmethod
    def from_source_config(
        cls: type[ActivitySourceConfig],
        config: SourceConfig,
    ) -> ActivitySourceConfig:
        """Create an :class:`ActivitySourceConfig` from the generic :class:`SourceConfig`.

        Parameters
        ----------
        config
            Generic source configuration.

        Returns
        -------
        ActivitySourceConfig
            Pipeline-specific configuration.
        """
        return build_source_config(cls, ActivitySourceParameters, config)
