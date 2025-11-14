"""Typed configuration helpers for the ChEMBL testitem pipeline."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, cast

from pydantic import ConfigDict, Field, PositiveInt

from ..models.http import HTTPClientConfig
from ..models.source import SourceConfig, SourceParameters


class TestItemSourceParameters(SourceParameters):
    """Free-form parameters specific to the testitem source."""

    model_config = ConfigDict(extra="forbid")
    base_url: str | None = Field(
        default=None,
        description="Override for the ChEMBL API base URL when fetching testitems.",
    )
    select_fields: Sequence[str] | None = Field(
        default=None,
        description="Optional list of field names to fetch via `only` parameter.",
    )
    max_pages: PositiveInt | None = Field(
        default=None,
        description="Maximum number of pages to fetch during pagination.",
    )

    @classmethod
    def from_mapping(
        cls, params: Mapping[str, Any] | None
    ) -> TestItemSourceParameters:
        """Construct the parameters object from a raw mapping.

        Parameters
        ----------
        params
            Raw mapping of parameters.

        Returns
        -------
        TestItemSourceParameters
            Constructed parameters object.
        """
        if params is None:
            return cls()

        normalized = cls._normalize_mapping(params)

        select_fields_raw = normalized.get("select_fields")
        select_fields: Sequence[str] | None = None
        if select_fields_raw is not None:
            if isinstance(select_fields_raw, Sequence) and not isinstance(
                select_fields_raw, (str, bytes)
            ):
                # Type narrowing: after isinstance check, select_fields_raw is Sequence[Any]
                # Explicitly annotate elements to avoid type narrowing warnings.
                # basedpyright cannot infer the field type from Sequence[Any].
                # Use a generator with explicit casting to strings.
                select_fields = tuple(str(cast(Any, field)) for field in select_fields_raw)  # pyright: ignore[reportUnknownVariableType]

        max_pages_raw = normalized.get("max_pages")
        max_pages: PositiveInt | None = None
        if max_pages_raw is not None:
            try:
                max_pages_value = int(max_pages_raw)
                if max_pages_value > 0:
                    max_pages = max_pages_value
            except (TypeError, ValueError):
                pass

        return cls(
            base_url=normalized.get("base_url"),
            select_fields=select_fields,
            max_pages=max_pages,
        )


class TestItemSourceConfig(SourceConfig[TestItemSourceParameters]):
    """Pipeline-specific view over the generic :class:`SourceConfig`."""

    enabled: bool = Field(default=True)
    description: str | None = Field(default=None)
    http_profile: str | None = Field(default=None)
    http: HTTPClientConfig | None = Field(default=None)
    page_size: PositiveInt = Field(
        default=200,
        description="Effective page size for pagination requests.",
    )
    parameters: TestItemSourceParameters = Field(default_factory=TestItemSourceParameters)

    parameters_model = TestItemSourceParameters
    batch_field = "page_size"
    default_batch_size = 200

    @classmethod
    def _resolve_batch_value(
        cls,
        *,
        config: SourceConfig,
        parameters: TestItemSourceParameters,  # noqa: ARG003
    ) -> int:
        if config.batch_size is not None:
            return int(config.batch_size)
        raw_parameters = config.parameters_mapping()
        batch_size_raw = raw_parameters.get("batch_size")
        if isinstance(batch_size_raw, int) and batch_size_raw > 0:
            return batch_size_raw
        default_value = cls._default_batch_value()
        return int(default_value) if default_value is not None else 200
