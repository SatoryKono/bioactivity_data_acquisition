"""Typed configuration helpers for the ChEMBL activity pipeline."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, cast

from pydantic import Field, PositiveInt, model_validator

from bioetl.core.runtime import BaseSourceConfig, BaseSourceParameters

from ..models.http import HTTPClientConfig
from ..models.source import SourceConfig


class ActivitySourceParameters(BaseSourceParameters):
    """Free-form parameters specific to the activity source."""

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

        select_fields_raw = normalized.get("select_fields")
        select_fields: Sequence[str] | None = None
        if select_fields_raw is not None:
            if isinstance(select_fields_raw, Sequence) and not isinstance(
                select_fields_raw, (str, bytes)
            ):
                # Type narrowing: after isinstance check, select_fields_raw is Sequence[Any]
                # Explicitly cast elements to silence type checking warnings.
                # basedpyright cannot infer the field type from Sequence[Any].
                # Use a generator expression with explicit string conversion.
                select_fields = tuple(
                    str(cast(Any, field)) for field in select_fields_raw
                )  # pyright: ignore[reportUnknownVariableType]
            else:
                select_fields = (str(select_fields_raw),)

        normalized["select_fields"] = select_fields

        return cls(**normalized)


class ActivitySourceConfig(BaseSourceConfig[ActivitySourceParameters]):
    """Pipeline-specific view over the generic :class:`SourceConfig`."""

    enabled: bool = Field(default=True)
    description: str | None = Field(default=None)
    http_profile: str | None = Field(default=None)
    http: HTTPClientConfig | None = Field(default=None)
    batch_size: PositiveInt = Field(
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
        payload["max_url_length"] = cls._extract_max_url_length(config.parameters)
        return payload

    @staticmethod
    def _extract_max_url_length(parameters: Mapping[str, Any] | None) -> int:
        """Extract a validated ``max_url_length`` value from the raw parameters."""

        mapping = parameters or {}
        raw = mapping.get("max_url_length")
        if raw is None:
            return 2000
        try:
            value = int(raw)
        except (TypeError, ValueError) as exc:
            msg = "max_url_length must be coercible to an integer"
            raise ValueError(msg) from exc
        if value <= 0:
            msg = "max_url_length must be a positive integer"
            raise ValueError(msg)
        return value
