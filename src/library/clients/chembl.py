"""Client for the ChEMBL API."""
from __future__ import annotations

from typing import Any

from library.clients.base import BaseApiClient
from library.config import APIClientConfig


class ChEMBLClient(BaseApiClient):
    """HTTP client for the ChEMBL document endpoint."""

    def __init__(self, config: APIClientConfig, **kwargs: Any) -> None:
        super().__init__(config, **kwargs)

    def fetch_by_doc_id(self, doc_id: str) -> dict[str, Any]:
        """Retrieve a document by its ChEMBL document identifier."""
        
        # ChEMBL API иногда возвращает XML вместо JSON, поэтому добавляем дополнительные заголовки
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json"
        }
        
        payload = self._request("GET", f"document/{doc_id}", headers=headers)
        return self._parse_document(payload)

    def _parse_document(self, payload: dict[str, Any]) -> dict[str, Any]:
        document = dict(payload)
        
        # Ensure all required fields are present with proper defaults
        record: dict[str, Any | None] = {
            "source": "chembl",
            "document_chembl_id": document.get("document_chembl_id"),
            "title": document.get("title"),
            "doi": document.get("doi"),
            "pubmed_id": document.get("pubmed_id"),
            "doc_type": document.get("doc_type"),
            "journal": document.get("journal"),
            "year": document.get("year"),
            # ChemBL-specific fields (согласно схеме)
            "chembl_title": document.get("title"),
            "chembl_doi": document.get("doi"),
            "chembl_pubmed_id": document.get("pubmed_id"),
            "chembl_journal": document.get("journal"),
            "chembl_year": document.get("year"),
            "chembl_volume": document.get("volume"),
            "chembl_issue": document.get("issue"),
            # Legacy fields for backward compatibility
            "abstract": document.get("abstract"),
        }
        
        # Ensure doc_type is set to a default value if not present
        if record["doc_type"] is None:
            record["doc_type"] = "PUBLICATION"  # Default document type
        
        # Return all fields, including None values, to maintain schema consistency
        return record