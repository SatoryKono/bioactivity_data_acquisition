from __future__ import annotations

from typing import Any, Protocol
from collections.abc import Iterator, Mapping


class Query(Protocol):
    def only(self, *fields: str) -> Query:
        ...

    def __iter__(self) -> Iterator[Mapping[str, Any]]:
        ...


class Resource(Protocol):
    def filter(self, **filters: Any) -> Query:
        ...


class Client(Protocol):
    activity: Resource
    assay: Resource
    target: Resource
    data_validity_lookup: Resource
    mechanism: Resource


new_client: Client

