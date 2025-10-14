"""ChEMBL client implementation."""
from __future__ import annotations

from typing import Any, Dict

from .base import BaseClient
from ..io.normalize import coerce_text, normalise_doi


class ChemblClient(BaseClient):
    SOURCE = "chembl"

    def __init__(self, *, run_id: str, session=None, global_limiter=None) -> None:
        super().__init__(
            source=self.SOURCE,
            base_url="https://www.ebi.ac.uk/chembl/api/data",
            session=session,
            per_client_rps=2.0,
            global_limiter=global_limiter,
            run_id=run_id,
        )

    def fetch_by_doc_id(self, doc_id: str | None) -> Dict[str, Any]:
        if not doc_id:
            return {}
        encoded = self.encode_identifier(doc_id)
        self.logger.info("fetching document", extra={"doc_id": doc_id})
        payload = self.get_json(f"document/{encoded}.json")
        document = payload.get("document", {})
        parsed = parse_document(document)
        parsed.setdefault("chembl.document_chembl_id", coerce_text(doc_id))
        return parsed


def parse_document(data: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "chembl.document_chembl_id": coerce_text(data.get("document_chembl_id")),
        "chembl.title": coerce_text(data.get("title")),
        "chembl.abstract": coerce_text(data.get("abstract")),
        "chembl.doi": normalise_doi(data.get("doi")),
        "chembl.pmid": coerce_text(data.get("pubmed_id")),
    }

