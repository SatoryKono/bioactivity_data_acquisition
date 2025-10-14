"""Client for Crossref works API."""
from __future__ import annotations

from typing import Mapping

from .base import BaseClient


class CrossrefClient(BaseClient):
    """HTTP client that resolves DOIs via the Crossref API."""

    def fetch_by_doi(self, doi: str) -> Mapping[str, object]:
        escaped = doi.replace("/", "%2F")
        return self.get_json(f"works/{escaped}")

    def search(self, query: str) -> Mapping[str, object]:
        return self.get_json("works", params={"query": query})
