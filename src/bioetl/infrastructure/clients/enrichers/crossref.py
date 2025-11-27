from __future__ import annotations

from collections.abc import Iterator
from typing import Any

from ._base import _BaseEnricherClient
from bioetl.core.http.interfaces import BaseApiClient


class CrossrefClient(_BaseEnricherClient):
    def __init__(self, api_client: BaseApiClient) -> None:
        super().__init__(api_client, "crossref")

    def fetch(self, doi: str) -> Iterator[dict[str, Any]]:
        return self._get(f"/works/{doi}")


__all__ = ["CrossrefClient"]
