"""Typed configuration helpers for the ChEMBL document pipeline."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from pydantic import ConfigDict, Field, PositiveInt

from bioetl.clients.base import normalize_select_fields

from ..common import BatchSizeLimitMixin
from ..models.http import HTTPClientConfig
from ..models.source import SourceConfig, SourceParameters


class DocumentSourceParameters(SourceParameters):
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
    def from_mapping(
        cls,
        params: Mapping[str, Any] | None = None,
    ) -> DocumentSourceParameters:
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
        data = dict(params or {})
        select_fields = normalize_select_fields(data.get("select_fields"))

        payload: dict[str, Any] = {}
        if "base_url" in data:
            payload["base_url"] = data.get("base_url")
        if select_fields is not None:
            payload["select_fields"] = select_fields

        return cls.model_validate(payload)


class DocumentSourceConfig(
    BatchSizeLimitMixin, SourceConfig[DocumentSourceParameters]
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
    parameters: DocumentSourceParameters = Field(default_factory=DocumentSourceParameters)

    parameters_model = DocumentSourceParameters
    batch_field = "batch_size"
    default_batch_size = 25

