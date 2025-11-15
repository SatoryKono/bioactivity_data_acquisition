"""Typed configuration helpers for the ChEMBL activity pipeline."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from pydantic import ConfigDict, Field, PositiveInt, model_validator

from bioetl.clients.base import normalize_select_fields
from ..models.http import HTTPClientConfig
from ..models.source import SourceConfig, SourceParameters
from ..utils import coerce_max_url_length


class ActivitySourceParameters(SourceParameters):
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
    def from_mapping(
        cls, params: Mapping[str, Any] | None = None
    ) -> ActivitySourceParameters:
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
        if params is None:
            return cls()

        normalized = dict(cls._normalize_mapping(params))
        normalized.pop("max_url_length", None)

        normalized["select_fields"] = normalize_select_fields(normalized.get("select_fields"))

        return cls(**normalized)


class ActivitySourceConfig(SourceConfig[ActivitySourceParameters]):
    """Pipeline-specific view over the generic :class:`SourceConfig`."""

    enabled: bool = Field(default=True)
    description: str | None = Field(default=None)
    http_profile: str | None = Field(default=None)
    http: HTTPClientConfig | None = Field(default=None)
    batch_size: PositiveInt | None = Field(
        default=25,
        description="Effective batch size for pagination requests (capped at 25).",
    )
    max_url_length: PositiveInt = Field(
        default=2000,
        description="Upper bound for paginated URL length when building endpoints.",
    )
    parameters: ActivitySourceParameters = Field(default_factory=ActivitySourceParameters)

    parameters_model = ActivitySourceParameters
    batch_field = "batch_size"
    default_batch_size = 25

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
        if self.max_url_length > 2000:
            self.max_url_length = 2000
        return self

    @classmethod
    def _build_payload(
        cls,
        *,
        config: SourceConfig,
        parameters: ActivitySourceParameters,
    ) -> dict[str, Any]:
        """Extend the base payload with activity-specific fields."""

        payload = super()._build_payload(config=config, parameters=parameters)
        payload["max_url_length"] = coerce_max_url_length(config.parameters_mapping())
        return payload
