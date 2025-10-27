"""HTTP client for ChEMBL document endpoints - simplified version."""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any

from library.clients.base import BaseApiClient
from library.config import APIClientConfig

logger = logging.getLogger(__name__)


class ChEMBLClient(BaseApiClient):
    """HTTP client for ChEMBL document endpoints."""

    def __init__(self, config: APIClientConfig, **kwargs: Any) -> None:
        super().__init__(config, **kwargs)

    def fetch_by_doc_id(self, document_chembl_id: str) -> dict[str, Any]:
        """Fetch document data by ChEMBL document ID."""
        logger.info(f"Fetching document {document_chembl_id} from ChEMBL...")
        try:
            payload = self._request("GET", f"document/{document_chembl_id}")
            logger.info(f"Received payload keys: {list(payload.keys())}")
            
            # Fetch source metadata if src_id is available
            source_fields = {}
            if "src_id" in payload and payload["src_id"]:
                source_fields = self.fetch_source_metadata(payload["src_id"])
            
            # Extract relevant document fields
            data = {
                # Legacy fields
                "document_chembl_id": document_chembl_id,
                "document_pubmed_id": payload.get("pubmed_id"),
                "document_classification": payload.get("doc_type"),
                "referenses_on_previous_experiments": payload.get("refs"),
                "original_experimental_document": payload.get("original_experimental_document"),
                "chembl_title": payload.get("title"),
                "chembl_abstract": payload.get("abstract"),
                "chembl_authors": payload.get("authors"),
                "chembl_journal": payload.get("journal"),
                "chembl_year": payload.get("year"),
                "chembl_volume": payload.get("volume"),
                "chembl_issue": payload.get("issue"),
                "chembl_first_page": payload.get("first_page"),
                "chembl_last_page": payload.get("last_page"),
                "chembl_doi": payload.get("doi"),
                "chembl_patent_id": payload.get("patent_id"),
                "chembl_ridx": payload.get("ridx"),
                "chembl_teaser": payload.get("teaser"),
                
                # NEW: CHEMBL.DOCS.* fields according to specification
                "CHEMBL.DOCS.document_chembl_id": payload.get("document_chembl_id", document_chembl_id),
                "CHEMBL.DOCS.doc_type": payload.get("doc_type"),
                "CHEMBL.DOCS.title": payload.get("title"),
                "CHEMBL.DOCS.journal": payload.get("journal"),
                "CHEMBL.DOCS.year": payload.get("year"),
                "CHEMBL.DOCS.volume": payload.get("volume"),
                "CHEMBL.DOCS.issue": payload.get("issue"),
                "CHEMBL.DOCS.first_page": payload.get("first_page"),
                "CHEMBL.DOCS.last_page": payload.get("last_page"),
                "CHEMBL.DOCS.doi": payload.get("doi"),
                "CHEMBL.DOCS.pubmed_id": payload.get("pubmed_id"),
                "CHEMBL.DOCS.abstract": payload.get("abstract"),
                "CHEMBL.DOCS.chembl_release": payload.get("src_id"),  # JSON reference to release
                
                # NEW: CHEMBL.SOURCE.* fields
                **source_fields,  # adds CHEMBL.SOURCE.* fields
                
                "source_system": "ChEMBL",
                "extracted_at": datetime.utcnow().isoformat()
            }
            
            non_none_count = sum(1 for v in data.values() if v is not None)
            logger.info(f"Mapped fields: {list(data.keys())}, non-None values: {non_none_count}")
            return data
        except Exception as e:
            logger.warning(f"Failed to fetch document {document_chembl_id}: {e}")
            return {
                "document_chembl_id": document_chembl_id,
                "source_system": "ChEMBL",
                "extracted_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                "error": str(e)
            }

    def fetch_documents_batch(
        self, 
        document_chembl_ids: list[str],
        batch_size: int = 50
    ) -> dict[str, dict[str, Any]]:
        """Fetch multiple documents in batch."""
        if not document_chembl_ids:
            return {}
        
        results = {}
        
        # Process in chunks to avoid overwhelming the API
        for i in range(0, len(document_chembl_ids), batch_size):
            chunk = document_chembl_ids[i:i + batch_size]
            
            # For each document in the chunk, make individual requests
            for doc_id in chunk:
                try:
                    result = self.fetch_by_doc_id(doc_id)
                    results[doc_id] = result
                except Exception as e:
                    logger.warning(f"Failed to fetch document {doc_id} in batch: {e}")
                    results[doc_id] = {
                        "document_chembl_id": doc_id,
                        "source_system": "ChEMBL",
                        "extracted_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                        "error": str(e)
                    }
        
        return results

    def fetch_source_metadata(self, src_id: int) -> dict[str, Any]:
        """Fetch source metadata from ChEMBL."""
        try:
            payload = self._request("GET", f"source/{src_id}")
            return {
                "CHEMBL.SOURCE.src_id": payload.get("src_id"),
                "CHEMBL.SOURCE.src_description": payload.get("src_description"),
                "CHEMBL.SOURCE.src_short_name": payload.get("src_short_name"),
                "CHEMBL.SOURCE.src_url": payload.get("src_url"),
                "CHEMBL.SOURCE.data": json.dumps(payload) if payload else None
            }
        except Exception as e:
            logger.warning(f"Failed to fetch source {src_id}: {e}")
            return {
                "CHEMBL.SOURCE.src_id": None,
                "CHEMBL.SOURCE.src_description": None,
                "CHEMBL.SOURCE.src_short_name": None,
                "CHEMBL.SOURCE.src_url": None,
                "CHEMBL.SOURCE.data": None,
            }

    def get_chembl_status(self) -> dict[str, Any]:
        """Get ChEMBL version and release information."""
        try:
            payload = self._request("GET", "status.json")
            return {
                "version": payload.get("chembl_db_version", "unknown"),
                "release_date": payload.get("chembl_release_date"),
                "timestamp": datetime.utcnow().isoformat()
            }
        except Exception as e:
            logger.warning(f"Failed to get ChEMBL version: {e}")
            return {
                "version": "unknown",
                "release_date": None,
                "timestamp": datetime.utcnow().isoformat()
            }

    def _request(self, method: str, url: str, **kwargs: Any) -> dict[str, Any]:
        """Make HTTP request using base client functionality."""
        try:
            # Add format=json parameter to get JSON response instead of XML
            if '?' in url:
                url += '&format=json'
            else:
                url += '?format=json'
            
            # Use the base client's request method
            response = self.session.get(f"{self.base_url}/{url}", timeout=self.timeout)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Request failed: {e}")
            raise
