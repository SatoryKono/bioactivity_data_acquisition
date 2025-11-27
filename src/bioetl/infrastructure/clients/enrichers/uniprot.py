from __future__ import annotations

from collections.abc import Iterator
from typing import Any

from bioetl.infrastructure.clients.enrichers._base import _BaseEnricherClient
from bioetl.core.http.interfaces import BaseApiClient


class UniProtClient(_BaseEnricherClient):
    def __init__(self, api_client: BaseApiClient) -> None:
        super().__init__(api_client, "uniprot")

    def fetch(self, uniprot_id: str) -> Iterator[dict[str, Any]]:
        return self._get(f"/uniprot/{uniprot_id}")


__all__ = ["UniProtClient"]
