"""Typed configuration helpers for the ChEMBL testitem pipeline."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, cast

from pydantic import BaseModel, ConfigDict, Field, PositiveInt

from ..models.base import build_source_config
from ..models.http import HTTPClientConfig
from ..models.source import SourceConfig


class TestItemSourceParameters(BaseModel):
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
    def from_mapping(cls, params: Mapping[str, Any]) -> TestItemSourceParameters:
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

        max_pages_raw = params.get("max_pages")
        max_pages: PositiveInt | None = None
        if max_pages_raw is not None:
            try:
                max_pages_value = int(max_pages_raw)
                if max_pages_value > 0:
                    max_pages = max_pages_value
            except (TypeError, ValueError):
                pass

        return cls(
            base_url=params.get("base_url"),
            select_fields=select_fields,
            max_pages=max_pages,
        )


class TestItemSourceConfig(BaseModel):
    """Pipeline-specific view over the generic :class:`SourceConfig`."""

    model_config = ConfigDict(extra="forbid")

    enabled: bool = Field(default=True)
    description: str | None = Field(default=None)
    http_profile: str | None = Field(default=None)
    http: HTTPClientConfig | None = Field(default=None)
    page_size: PositiveInt = Field(
        default=200,
        description="Effective page size for pagination requests.",
    )
    parameters: TestItemSourceParameters = Field(default_factory=TestItemSourceParameters)

    @classmethod
    def from_source_config(cls, config: SourceConfig) -> TestItemSourceConfig:
        """Create a :class:`TestItemSourceConfig` from the generic :class:`SourceConfig`.

        Parameters
        ----------
        config
            Generic source configuration.

        Returns
        -------
        TestItemSourceConfig
            Pipeline-specific configuration.
        """
        page_size = cls._resolve_page_size(config)
        base_config = build_source_config(cls, TestItemSourceParameters, config)
        return base_config.model_copy(update={"page_size": page_size})

    @staticmethod
    def _resolve_page_size(config: SourceConfig) -> int:
        if config.batch_size is not None:
            return config.batch_size
        parameters = config.parameters
        batch_size_raw = parameters.get("batch_size")
        if isinstance(batch_size_raw, int) and batch_size_raw > 0:
            return batch_size_raw
        return 200
