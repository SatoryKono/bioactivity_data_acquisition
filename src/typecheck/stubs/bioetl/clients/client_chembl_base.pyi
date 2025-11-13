from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping, Sequence
from typing import Any, Protocol

from bioetl.clients.chembl_config import EntityConfig

__all__ = [
    "EntityConfig",
    "ChemblEntityFetcherBase",
    "ChemblClientProtocol",
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

class ChemblEntityFetcherBase:
    _chembl_client: Any
    _config: EntityConfig

    def __init__(self, chembl_client: Any, config: EntityConfig) -> None: ...
    def fetch_by_ids(
        self,
        ids: Iterable[str],
        fields: Sequence[str],
        page_limit: int = 1000,
    ) -> dict[str, dict[str, Any]] | dict[str, list[dict[str, Any]]]: ...
    def _build_dict_result(
        self,
        records: list[dict[str, Any]],
        unique_ids: set[str],
    ) -> dict[str, dict[str, Any]]: ...
    def _build_list_result(
        self,
        records: list[dict[str, Any]],
        unique_ids: set[str],
    ) -> dict[str, list[dict[str, Any]]]: ...

