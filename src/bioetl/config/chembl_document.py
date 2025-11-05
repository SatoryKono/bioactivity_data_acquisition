"""Typed configuration helpers for the ChEMBL document pipeline."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Mapping

from pydantic import BaseModel, ConfigDict, Field, PositiveInt, model_validator

from .models import HTTPClientConfig, SourceConfig


class DocumentSourceParameters(BaseModel):
    """Free-form parameters specific to the document source."""

    model_config = ConfigDict(extra="forbid")

    base_url: str | None = Field(
        default=None,
        description="Override for the ChEMBL API base URL when fetching documents.",
    )
    select_fields: Sequence[str] | None = Field(
        default=None,
        description="List of fields to select from the API response.",
    )

    @classmethod
    def from_mapping(cls, params: Mapping[str, Any]) -> "DocumentSourceParameters":
        """Construct the parameters object from a raw mapping."""

        select_fields_raw = params.get("select_fields")
        select_fields: Sequence[str] | None = None
        if (
            select_fields_raw is not None
            and isinstance(select_fields_raw, Sequence)
            and not isinstance(select_fields_raw, (str, bytes))
        ):
            select_fields = [str(field) for field in select_fields_raw]

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
    def enforce_limits(self) -> "DocumentSourceConfig":
        """Ensure the configured values adhere to documented constraints."""

        if self.batch_size > 25:
            self.batch_size = 25
        return self

    @classmethod
    def from_source_config(cls, config: SourceConfig) -> "DocumentSourceConfig":
        """Create a :class:`DocumentSourceConfig` from the generic :class:`SourceConfig`."""

        batch_size = config.batch_size if config.batch_size is not None else 25
        parameters = DocumentSourceParameters.from_mapping(config.parameters)

        return cls(
            enabled=config.enabled,
            description=config.description,
            http_profile=config.http_profile,
            http=config.http,
            batch_size=batch_size,
            parameters=parameters,
        )

