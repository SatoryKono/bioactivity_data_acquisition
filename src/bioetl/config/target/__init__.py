"""Typed configuration helpers for the ChEMBL target pipeline."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from pydantic import ConfigDict, Field, PositiveInt

from bioetl.clients.base import normalize_select_fields

from ..common import BatchSizeLimitMixin
from ..models.http import HTTPClientConfig
from ..models.source import SourceConfig, SourceParameters


class TargetSourceParameters(SourceParameters):
    """Free-form parameters specific to the target source."""

    model_config = ConfigDict(extra="forbid")
    base_url: str | None = Field(
        default=None,
        description="Override for the ChEMBL API base URL when fetching targets.",
    )
    select_fields: Sequence[str] | None = Field(
        default=None,
        description="Optional list of field names to fetch via `only` parameter.",
    )

    @classmethod
    def from_mapping(
        cls, params: Mapping[str, Any] | None
    ) -> TargetSourceParameters:
        """Construct the parameters object from a raw mapping.

        Parameters
        ----------
        params
            Raw mapping of parameters.

        Returns
        -------
        TargetSourceParameters
            Constructed parameters object.
        """
        if params is None:
            return cls()

        normalized = dict(cls._normalize_mapping(params))
        select_fields = normalize_select_fields(normalized.get("select_fields"))

        return cls(
            base_url=normalized.get("base_url"),
            select_fields=select_fields,
        )


class TargetSourceConfig(
    BatchSizeLimitMixin, SourceConfig[TargetSourceParameters]
):
    """Pipeline-specific view over the generic :class:`SourceConfig`."""

    enabled: bool = Field(default=True)
    description: str | None = Field(default=None)
    http_profile: str | None = Field(default=None)
    http: HTTPClientConfig | None = Field(default=None)
    batch_size: PositiveInt | None = Field(
        default=25,
        description="Effective batch size for pagination requests (capped at 25).",
    )
    parameters: TargetSourceParameters = Field(default_factory=TargetSourceParameters)

    parameters_model = TargetSourceParameters
    batch_field = "batch_size"
    default_batch_size = 25

