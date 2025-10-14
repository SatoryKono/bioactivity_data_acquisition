"""ChEMBL API client."""
from __future__ import annotations

from typing import Dict, Optional

from library.clients.base import BaseClient, SessionManager
from library.io.normalize import coerce_text, normalise_doi, to_lc_stripped


class ChemblClient(BaseClient):
    base_url = "https://www.ebi.ac.uk/chembl/api/data/"

    def __init__(self, session_manager: SessionManager, rate_per_sec: float = 5.0) -> None:
        super().__init__(session_manager, name="chembl", rate_per_sec=rate_per_sec)

    def fetch_by_doc_id(self, doc_id: str) -> Dict[str, Optional[str]]:
        payload = self.get_json(f"document/{doc_id}")
        doi = normalise_doi(payload.get("doi"))
        pmid = to_lc_stripped(coerce_text(payload.get("pubmed_id")))
        return {
            "document_chembl_id": coerce_text(payload.get("document_chembl_id")) or doc_id,
            "doi": doi,
            "pmid": pmid,
            "title": coerce_text(payload.get("title")),
        }
