"""OpenAlex client."""
from __future__ import annotations

from typing import Dict, Optional

from library.clients.base import BaseClient, SessionManager
from library.io.normalize import coerce_text, normalise_doi


class OpenAlexClient(BaseClient):
    base_url = "https://api.openalex.org/"

    def __init__(self, session_manager: SessionManager, rate_per_sec: float = 4.0) -> None:
        super().__init__(session_manager, name="openalex", rate_per_sec=rate_per_sec)

    def fetch_by_doi(self, doi: str) -> Dict[str, Optional[str]]:
        norm = normalise_doi(doi)
        if not norm:
            return {}
        payload = self.get_json(f"works/doi:{norm}")
        return self._parse_work(payload)

    def fetch_by_pmid(self, pmid: str) -> Dict[str, Optional[str]]:
        payload = self.get_json(f"works/pmid:{pmid}")
        return self._parse_work(payload)

    def _parse_work(self, payload: Dict[str, object]) -> Dict[str, Optional[str]]:
        doi = normalise_doi(coerce_text(payload.get("doi")))
        title = coerce_text(payload.get("title"))
        journal = None
        host_venue = payload.get("host_venue") or {}
        if isinstance(host_venue, dict):
            journal = coerce_text(host_venue.get("display_name"))
        return {
            "doi": doi,
            "title": title,
            "journal": journal,
        }
