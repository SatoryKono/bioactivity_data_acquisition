"""Typed configuration helpers for the ChEMBL assay pipeline."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from pydantic import Field, PositiveInt, model_validator

from ..models.http import HTTPClientConfig
from ..models.source import SourceConfig, SourceParameters
from ..utils import coerce_bool


class AssaySourceParameters(SourceParameters):
    """Free-form parameters specific to the assay source."""

    base_url: str | None = Field(
        default=None,
        description="Override for the ChEMBL API base URL when fetching assays.",
    )
    handshake_endpoint: str = Field(
        default="/status.json",
        description="Endpoint used for dry-run handshake checks.",
    )
    handshake_enabled: bool = Field(
        default=True,
        description="Whether to perform handshake checks before extraction.",
    )

    @classmethod
    def from_mapping(
        cls,
        params: Mapping[str, Any] | None = None,
    ) -> AssaySourceParameters:
        """Construct the parameters object from a raw mapping.

        Parameters
        ----------
        params
            Raw mapping of parameters. ``None`` is treated as an empty mapping.

        Returns
        -------
        AssaySourceParameters
            Constructed parameters object.
        """
        normalized = cls._normalize_mapping(params or {})
        return cls(
            base_url=normalized.get("base_url"),
            handshake_endpoint=normalized.get("handshake_endpoint", "/status.json"),
            handshake_enabled=coerce_bool(normalized.get("handshake_enabled", True)),
        )


class AssaySourceConfig(SourceConfig):
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
    parameters: AssaySourceParameters = Field(default_factory=AssaySourceParameters)

    parameters_model = AssaySourceParameters
    batch_field = "batch_size"
    default_batch_size = 25

    @model_validator(mode="after")
    def enforce_limits(self) -> AssaySourceConfig:
        """Ensure the configured values adhere to documented constraints.

        Returns
        -------
        AssaySourceConfig
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
        parameters: AssaySourceParameters,
    ) -> dict[str, Any]:
        """Extend the base payload with assay-specific fields."""

        payload = super()._build_payload(config=config, parameters=parameters)
        payload["max_url_length"] = cls._extract_max_url_length(config.parameters_mapping())
        return payload

    @staticmethod
    def _extract_max_url_length(parameters: Mapping[str, Any] | None) -> int:
        """Extract max_url_length from parameters.

        Parameters
        ----------
        parameters
            Parameters mapping.

        Returns
        -------
        int
            Extracted max_url_length value.

        Raises
        ------
        ValueError
            If max_url_length is invalid.
        """
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
