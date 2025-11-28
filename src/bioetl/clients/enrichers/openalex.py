from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from .base import BaseEnricherClient
from bioetl.core.http.interfaces import BaseApiClient
from bioetl.core.http.types import JSONRecordStream


class OpenAlexClient(BaseEnricherClient):
    def __init__(self, api_client: BaseApiClient) -> None:
        super().__init__(api_client, "openalex")

    def fetch(
        self, oa_id: str, params: Mapping[str, Any] | None = None
    ) -> JSONRecordStream:
        return self._get(f"/works/{oa_id}", params=params)

    def search(
        self, query: str, params: Mapping[str, Any] | None = None
    ) -> JSONRecordStream:
        merged_params = {"search": query, **(params or {})}
        return self._get("/works", params=merged_params)


__all__ = ["OpenAlexClient"]
