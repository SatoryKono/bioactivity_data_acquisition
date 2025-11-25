"""Protocol definitions for infrastructure client adapters."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any, Protocol


class ChemblEntityClientProtocol(Protocol):
    """Minimal contract for iterating over ChEMBL entities."""

    def iterate_entities(self, ids: Iterable[str]) -> Iterable[Mapping[str, Any]]:
        ...

    def fetch_record(self, id: str) -> Mapping[str, Any]:
        ...


class ChemblClientFactoryProtocol(Protocol):
    """Factory contract returning clients implementing ``ChemblEntityClientProtocol``."""

    def build(
        self,
        entity_name: str,
        *,
        source_name: str = "chembl",
        source_config: Any | None = None,
        options: Mapping[str, Any] | None = None,
        chembl_client_kwargs: Mapping[str, Any] | None = None,
        fresh_http_client: bool = False,
    ) -> Any:
        ...

    def build_http_client(
        self,
        *,
        source_name: str = "chembl",
        source_config: Any | None = None,
        options: Mapping[str, Any] | None = None,
        fresh_http_client: bool = False,
    ) -> tuple[Any, str, Any]:
        ...
