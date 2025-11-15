from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from typing import Any, ClassVar, Protocol

import pandas as pd

from bioetl.clients.chembl_config import EntityConfig

__all__ = [
    "EntityConfig",
    "ChemblEntityConfigMixin",
    "ChemblEntityFetcherBase",
    "ChemblClientProtocol",
    "ChemblEntityClientProtocol",
]


class ChemblClientProtocol(Protocol):
    def paginate(
        self,
        endpoint: str,
        *,
        params: Mapping[str, Any] | None = None,
        page_size: int = ...,
        items_key: str | None = None,
    ) -> Iterator[Mapping[str, Any]]: ...

class ChemblEntityClientProtocol(Protocol):
    def fetch_by_ids(
        self,
        ids: Sequence[str],
        fields: Sequence[str] | None = None,
        *,
        page_limit: int | None = None,
    ) -> pd.DataFrame: ...

    def fetch_all(
        self,
        *,
        limit: int | None = None,
        fields: Sequence[str] | None = None,
        page_size: int | None = None,
    ) -> pd.DataFrame: ...

    def iterate_records(
        self,
        *,
        params: Mapping[str, Any] | None = None,
        limit: int | None = None,
        fields: Sequence[str] | None = None,
        page_size: int | None = None,
    ) -> Iterator[Mapping[str, Any]]: ...

class ChemblEntityConfigMixin:
    ENTITY_CONFIG: ClassVar[EntityConfig | None]
    DEFAULT_BATCH_SIZE: ClassVar[int | None]
    DEFAULT_MAX_URL_LENGTH: ClassVar[int | None]
    REQUIRE_MAX_URL_LENGTH: ClassVar[bool]

    def __init__(
        self,
        chembl_client: ChemblClientProtocol,
        *,
        entity_config: EntityConfig | None = ...,
        batch_size: int | None = ...,
        max_url_length: int | None = ...,
    ) -> None: ...

    def _normalize_batch_size(self, batch_size: int | None) -> int | None: ...

    def _normalize_max_url_length(
        self, max_url_length: int | None
    ) -> int | None: ...


class ChemblEntityFetcherBase(ChemblEntityClientProtocol):
    _chembl_client: ChemblClientProtocol
    _config: EntityConfig

    @classmethod
    def _init_from_entity_config(
        cls,
        instance: "ChemblEntityFetcherBase",
        chembl_client: ChemblClientProtocol,
        *,
        entity_config: EntityConfig,
        batch_size: int | None = ...,
        max_url_length: int | None = ...,
    ) -> None: ...

    def __init__(
        self,
        chembl_client: ChemblClientProtocol,
        config: EntityConfig,
        *,
        batch_size: int | None = ...,
        max_url_length: int | None = ...,
    ) -> None: ...

    def _validate_identifiers(self, ids: Sequence[str]) -> list[str]: ...

    def _chunk_identifiers(
        self,
        ids: Sequence[str],
        *,
        select_fields: Sequence[str] | None = ...,
    ) -> Iterator[Sequence[str]]: ...

    def _build_chunk_params(
        self,
        chunk: Sequence[str],
        *,
        fields: Sequence[str] | None = ...,
    ) -> dict[str, Any]: ...

    def _empty_frame(self, fields: Sequence[str] | None) -> pd.DataFrame: ...

    def _records_to_frame(
        self,
        records: Sequence[Mapping[str, Any]],
        fields: Sequence[str] | None,
    ) -> pd.DataFrame: ...

    def _resolve_page_size(
        self,
        requested: int | None,
        limit: int | None,
    ) -> int: ...

