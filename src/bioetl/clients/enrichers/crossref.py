from __future__ import annotations

from typing import Any

from bioetl.clients.enrichers._base import _BaseEnricherClient
from bioetl.core.http.api_client import UnifiedAPIClient


class CrossrefClient(_BaseEnricherClient):
    def __init__(self, api_client: UnifiedAPIClient) -> None:
        super().__init__(api_client, "crossref")

    def fetch(self, doi: str) -> dict[str, Any]:
        return self._get(f"/works/{doi}")


__all__ = ["CrossrefClient"]
