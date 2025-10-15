"""ChEMBL API client."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from library.clients import BasePublicationsClient, ClientConfig
from library.io.normalize import parse_chembl_document
from library.utils.errors import ExtractionError
from library.utils.logging import bind_context


class ChemblClient(BasePublicationsClient):
    """Client for querying ChEMBL documents."""

    def __init__(self, config: ClientConfig, **kwargs: Any) -> None:
        super().__init__(config, **kwargs)

    def fetch_by_doc_id(self, doc_id: str) -> Mapping[str, Any]:
        context = {"doc_id": doc_id}
        payload = self._request(f"document/{doc_id}.json", context=context)
        if not isinstance(payload, Mapping):
            raise ExtractionError("ChEMBL response is not a JSON object")
        record = parse_chembl_document(payload)
        if not record.get("document_chembl_id"):
            record["document_chembl_id"] = doc_id
        bind_context(self.logger, **context).info("chembl_document", **record)
        return record

    def fetch_publications(self, query: str) -> list[dict[str, Any]]:
        record = dict(self.fetch_by_doc_id(query))
        return [record]
