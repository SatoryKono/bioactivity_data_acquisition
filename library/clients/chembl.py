"""Client for accessing ChEMBL document metadata."""
from __future__ import annotations

from typing import Mapping

from .base import BaseClient


class ChEMBLClient(BaseClient):
    """HTTP client for the ChEMBL documents API."""

    def fetch_document(self, document_id: str) -> Mapping[str, object]:
        return self._get(f"document/{document_id}")

    def search_documents(self, query: str) -> Mapping[str, object]:
        return self._get("documents", params={"search": query})
