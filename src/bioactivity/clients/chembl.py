"""Client for the ChEMBL API."""
from __future__ import annotations

from typing import Any

from bioactivity.clients.base import BaseApiClient


class ChEMBLClient(BaseApiClient):
    """HTTP client for the ChEMBL document endpoint."""

    def __init__(self, **kwargs: Any) -> None:
        super().__init__("https://www.ebi.ac.uk/chembl/api/data", **kwargs)

    def fetch_by_doc_id(self, doc_id: str) -> dict[str, Any]:
        """Retrieve a document by its ChEMBL document identifier."""

        payload = self._request("GET", f"document/{doc_id}")
        return self._parse_document(payload)

    def _parse_document(self, payload: dict[str, Any]) -> dict[str, Any]:
        document = dict(payload)
        record: dict[str, Any | None] = {
            "source": "chembl",
            "document_chembl_id": document.get("document_chembl_id"),
            "pubmed_id": document.get("pubmed_id"),
            "doi": document.get("doi"),
            "title": document.get("title"),
            "abstract": document.get("abstract"),
        }
        return {key: value for key, value in record.items() if value is not None}