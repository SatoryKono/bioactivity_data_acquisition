from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from typing import Any, Protocol

__all__ = [
    "EntityConfig",
    "ChemblEntityFetcherBase",
    "ChemblClientProtocol",
    "make_entity_config",
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

class EntityConfig:
    endpoint: str
    filter_param: str
    id_key: str
    items_key: str
    log_prefix: str
    chunk_size: int
    supports_list_result: bool
    dedup_priority: Callable[[dict[str, Any], dict[str, Any]], dict[str, Any]] | None
    base_endpoint_length: int
    enable_url_length_check: bool

    def __init__(
        self,
        *,
        endpoint: str,
        filter_param: str,
        id_key: str,
        items_key: str,
        log_prefix: str,
        chunk_size: int = 100,
        supports_list_result: bool = False,
        dedup_priority: Callable[[dict[str, Any], dict[str, Any]], dict[str, Any]] | None = None,
        base_endpoint_length: int = 0,
        enable_url_length_check: bool = False,
    ) -> None: ...


def make_entity_config(
    *,
    endpoint: str,
    filter_param: str,
    id_key: str,
    items_key: str,
    log_prefix: str,
    chunk_size: int = 100,
    supports_list_result: bool = False,
    dedup_priority: Callable[[dict[str, Any], dict[str, Any]], dict[str, Any]] | None = None,
    base_endpoint_length: int = 0,
    enable_url_length_check: bool = False,
) -> EntityConfig: ...


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

