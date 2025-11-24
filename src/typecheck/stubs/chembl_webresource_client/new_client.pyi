from __future__ import annotations

from collections.abc import Iterator, Mapping
from typing import Any, Protocol

class Query(Protocol):
    def only(self, *fields: str) -> Query: ...
    def __iter__(self) -> Iterator[Mapping[str, Any]]: ...

class Resource(Protocol):
    def filter(self, **filters: Any) -> Query: ...

class Client(Protocol):
    activity: Resource
    assay: Resource
    target: Resource
    data_validity_lookup: Resource
    mechanism: Resource

new_client: Client
