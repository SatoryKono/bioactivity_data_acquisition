from __future__ import annotations

from collections.abc import Iterator
from typing import Any

from bioetl.clients.enrichers._base import _BaseEnricherClient
from bioetl.clients.common import BaseApiClient


class OpenAlexClient(_BaseEnricherClient):
    def __init__(self, api_client: BaseApiClient) -> None:
        super().__init__(api_client, "openalex")

    def fetch(self, oa_id: str) -> Iterator[dict[str, Any]]:
        return self._get(f"/works/{oa_id}")


__all__ = ["OpenAlexClient"]
