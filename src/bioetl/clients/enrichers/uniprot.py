from __future__ import annotations

from collections.abc import Iterator
from typing import Any

from bioetl.base_classes import BaseApiClient
from bioetl.clients.enrichers._base import _BaseEnricherClient


class UniProtClient(_BaseEnricherClient):
    def __init__(self, transport: BaseApiClient) -> None:
        super().__init__(transport, "uniprot")

    def fetch(self, uniprot_id: str) -> Iterator[dict[str, Any]]:
        return self._get(f"/uniprot/{uniprot_id}")


__all__ = ["UniProtClient"]
