"""Typed configuration helpers for the ChEMBL document pipeline."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, PositiveInt, model_validator

from ..models.base import build_source_config
from ..models.http import HTTPClientConfig
from ..models.source import SourceConfig


class DocumentSourceParameters(BaseModel):
    """Free-form parameters specific to the document source."""

    model_config = ConfigDict(extra="forbid")

    base_url: str | None = Field(
        default=None,
        description="Override for the ChEMBL API base URL when fetching documents.",
    )
    select_fields: Sequence[str] | None = Field(
        default=None,
        description="Optional list of field names to fetch via `only` parameter. If not provided, uses default API_DOCUMENT_FIELDS.",
    )

    @classmethod
    def from_mapping(cls, params: Mapping[str, Any]) -> DocumentSourceParameters:
        """Construct the parameters object from a raw mapping.

        Parameters
        ----------
        params
            Raw mapping of parameters.

        Returns
        -------
        DocumentSourceParameters
            Constructed parameters object.
        """
        select_fields_raw = params.get("select_fields")
        select_fields: Sequence[str] | None = None
        if select_fields_raw is not None:
            if isinstance(select_fields_raw, Sequence) and not isinstance(
                select_fields_raw, (str, bytes)
            ):
                select_fields = tuple(str(field) for field in select_fields_raw)

        return cls(
            base_url=params.get("base_url"),
            select_fields=select_fields,
        )


class DocumentSourceConfig(BaseModel):
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
    parameters: DocumentSourceParameters = Field(default_factory=DocumentSourceParameters)

    @model_validator(mode="after")
    def enforce_limits(self) -> DocumentSourceConfig:
        """Ensure the configured values adhere to documented constraints.

        Returns
        -------
        DocumentSourceConfig
            Self with enforced limits.
        """
        if self.batch_size > 25:
            self.batch_size = 25
        return self

    @classmethod
    def from_source_config(cls, config: SourceConfig) -> DocumentSourceConfig:
        """Create a :class:`DocumentSourceConfig` from the generic :class:`SourceConfig`.

        Parameters
        ----------
        config
            Generic source configuration.

        Returns
        -------
        DocumentSourceConfig
            Pipeline-specific configuration.
        """
        return build_source_config(cls, DocumentSourceParameters, config)
