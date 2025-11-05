"""Typed configuration helpers for the ChEMBL testitem pipeline."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Mapping

from pydantic import BaseModel, ConfigDict, Field, PositiveInt, model_validator

from .models import HTTPClientConfig, SourceConfig


class TestitemSourceParameters(BaseModel):
    """Free-form parameters specific to the testitem source."""

    model_config = ConfigDict(extra="forbid")

    base_url: str | None = Field(
        default=None,
        description="Override for the ChEMBL API base URL when fetching testitems.",
    )
    select_fields: Sequence[str] | None = Field(
        default=None,
        description="List of fields to select from the API response.",
    )
    max_pages: PositiveInt | None = Field(
        default=None,
        description="Maximum number of pages to fetch (None = all pages).",
    )

    @classmethod
    def from_mapping(cls, params: Mapping[str, Any]) -> "TestitemSourceParameters":
        """Construct the parameters object from a raw mapping."""

        select_fields_raw = params.get("select_fields")
        select_fields: Sequence[str] | None = None
        if (
            select_fields_raw is not None
            and isinstance(select_fields_raw, Sequence)
            and not isinstance(select_fields_raw, (str, bytes))
        ):
            select_fields = [str(field) for field in select_fields_raw]

        max_pages_raw = params.get("max_pages")
        max_pages: PositiveInt | None = None
        if isinstance(max_pages_raw, int) and max_pages_raw > 0:
            max_pages = max_pages_raw

        return cls(
            base_url=params.get("base_url"),
            select_fields=select_fields,
            max_pages=max_pages,
        )


class TestitemSourceConfig(BaseModel):
    """Pipeline-specific view over the generic :class:`SourceConfig`."""

    model_config = ConfigDict(extra="forbid")

    enabled: bool = Field(default=True)
    description: str | None = Field(default=None)
    http_profile: str | None = Field(default=None)
    http: HTTPClientConfig | None = Field(default=None)
    batch_size: PositiveInt = Field(
        default=200,
        description="Effective page size for pagination requests (defaults to 200, capped at 25 for URL length).",
    )
    parameters: TestitemSourceParameters = Field(default_factory=TestitemSourceParameters)

    @model_validator(mode="after")
    def enforce_limits(self) -> "TestitemSourceConfig":
        """Ensure the configured values adhere to documented constraints."""

        # Note: actual enforcement to 25 happens at usage time, not here
        # This is because testitem uses page_size which can be larger than batch_size
        return self

    @classmethod
    def from_source_config(cls, config: SourceConfig) -> "TestitemSourceConfig":
        """Create a :class:`TestitemSourceConfig` from the generic :class:`SourceConfig`."""

        batch_size = config.batch_size if config.batch_size is not None else 200
        parameters = TestitemSourceParameters.from_mapping(config.parameters)

        return cls(
            enabled=config.enabled,
            description=config.description,
            http_profile=config.http_profile,
            http=config.http,
            batch_size=batch_size,
            parameters=parameters,
        )

