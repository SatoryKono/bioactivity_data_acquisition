from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from typing import Any, Protocol

import pandas as pd

from bioetl.clients.chembl_config import EntityConfig

__all__ = [
    "EntityConfig",
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

class ChemblEntityFetcherBase(ChemblEntityClientProtocol):
    _chembl_client: Any
    _config: EntityConfig

    def __init__(self, chembl_client: Any, config: EntityConfig) -> None: ...

