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
            "document_pubmed_id": document.get("pubmed_id"),
            "chembl_doc_type": document.get("doc_type"),
            "journal": document.get("journal"),
            "year": document.get("year"),
            # ChemBL-specific fields (согласно схеме)
            "chembl_title": document.get("title"),
            "chembl_doi": document.get("doi"),
            "chembl_pmid": document.get("pubmed_id"),
            "chembl_journal": document.get("journal"),
            "chembl_year": document.get("year"),
            "chembl_volume": document.get("volume"),
            "chembl_issue": document.get("issue"),
            "chembl_abstract": document.get("abstract"),
            "chembl_issn": document.get("issn"),
            "chembl_authors": self._extract_authors(document),
            "chembl_error": None,  # Will be set if there's an error
            # Legacy fields for backward compatibility
            "abstract": document.get("abstract"),
        }
        
        # Ensure chembl_doc_type is set to a default value if not present
        if record["chembl_doc_type"] is None:
            record["chembl_doc_type"] = "PUBLICATION"  # Default document type
        
        # Return all fields, including None values, to maintain schema consistency
        return record

    def _extract_authors(self, document: dict[str, Any]) -> list[str] | None:
        """Извлекает авторов из ChEMBL документа."""
        authors = document.get("authors")
        if isinstance(authors, list):
            # Обрабатываем список авторов
            author_names = []
            for author in authors:
                if isinstance(author, dict):
                    # Если автор представлен как словарь
                    name = author.get("name") or author.get("author_name")
                    if name:
                        author_names.append(str(name))
                elif isinstance(author, str):
                    # Если автор представлен как строка
                    author_names.append(author)
            return author_names if author_names else None
        elif isinstance(authors, str):
            # Если авторы представлены как строка
            return [authors]
        
        return None

    def _create_empty_record(self, doc_id: str, error_msg: str) -> dict[str, Any]:
        """Создает пустую запись для случая ошибки."""
        return {
            "source": "chembl",
            "document_chembl_id": doc_id,
            "title": None,
            "doi": None,
            "document_pubmed_id": None,
            "journal": None,
            "year": None,
            "abstract": None,
            "chembl_doc_type": None,
            # ChemBL-specific fields
            "chembl_title": None,
            "chembl_doi": None,
            "chembl_pmid": None,
            "chembl_journal": None,
            "chembl_year": None,
            "chembl_volume": None,
            "chembl_issue": None,
            "chembl_abstract": None,
            "chembl_issn": None,
            "chembl_authors": None,
            "chembl_error": error_msg,
            # Legacy fields
            "abstract": None,
        }