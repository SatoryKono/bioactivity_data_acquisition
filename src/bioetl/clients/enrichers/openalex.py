from __future__ import annotations

from collections.abc import Iterator
from typing import Any

from bioetl.base_classes import BaseApiClient
from bioetl.clients.enrichers._base import _BaseEnricherClient


class OpenAlexClient(_BaseEnricherClient):
    def __init__(self, transport: BaseApiClient) -> None:
        super().__init__(transport, "openalex")

    def fetch(self, oa_id: str) -> Iterator[dict[str, Any]]:
        return self._get(f"/works/{oa_id}")


__all__ = ["OpenAlexClient"]
