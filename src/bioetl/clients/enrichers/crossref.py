from __future__ import annotations

from collections.abc import Iterator
from typing import Any

from bioetl.base_classes import BaseApiClient
from bioetl.clients.enrichers._base import _BaseEnricherClient


class CrossrefClient(_BaseEnricherClient):
    def __init__(self, transport: BaseApiClient) -> None:
        super().__init__(transport, "crossref")

    def fetch(self, doi: str) -> Iterator[dict[str, Any]]:
        return self._get(f"/works/{doi}")


__all__ = ["CrossrefClient"]
