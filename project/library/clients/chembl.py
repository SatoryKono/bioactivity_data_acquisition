"""ChEMBL API client."""
from __future__ import annotations

from .base import BasePublicationsClient, ClientConfig
from ..io.normalize import coerce_text, normalise_doi, to_lc_stripped


class ChemblClient(BasePublicationsClient):
    base_url = "https://www.ebi.ac.uk/chembl/api/data/"

    def __init__(self, config: ClientConfig) -> None:
        super().__init__(config)

    def fetch_publications(self, query: str) -> list[dict[str, str | None]]:
        """Fetch publications for a given query."""
        try:
            result = self.fetch_by_doc_id(query)
            return [result] if result else []
        except Exception:
            return []

    def fetch_by_doc_id(self, doc_id: str) -> dict[str, str | None]:
        payload = self.get_json(f"document/{doc_id}")
        doi = normalise_doi(payload.get("doi"))
        pmid = to_lc_stripped(coerce_text(payload.get("pubmed_id")))
        return {
            "document_chembl_id": coerce_text(payload.get("document_chembl_id")) or doc_id,
            "doi": doi,
            "pmid": pmid,
            "title": coerce_text(payload.get("title")),
        }
