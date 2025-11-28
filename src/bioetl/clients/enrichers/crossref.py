from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from .base import BaseEnricherClient
from bioetl.core.http.interfaces import BaseApiClient
from bioetl.core.http.types import JSONRecordStream


class CrossrefClient(BaseEnricherClient):
    def __init__(self, api_client: BaseApiClient) -> None:
        super().__init__(api_client, "crossref")

    def fetch(
        self, doi: str, params: Mapping[str, Any] | None = None
    ) -> JSONRecordStream:
        return self._get(f"/works/{doi}", params=params)

    def search(
        self, query: str, params: Mapping[str, Any] | None = None
    ) -> JSONRecordStream:
        merged_params = {"query": query, **(params or {})}
        return self._get("/works", params=merged_params)


__all__ = ["CrossrefClient"]
