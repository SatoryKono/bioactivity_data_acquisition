"""Crossref client."""
from __future__ import annotations

from typing import Dict, Optional

from library.clients.base import BaseClient, SessionManager
from library.io.normalize import coerce_text, normalise_doi


class CrossrefClient(BaseClient):
    base_url = "https://api.crossref.org/"

    def __init__(self, session_manager: SessionManager, rate_per_sec: float = 4.0) -> None:
        super().__init__(session_manager, name="crossref", rate_per_sec=rate_per_sec)

    def fetch_by_doi(self, doi: str) -> Dict[str, Optional[str]]:
        norm = normalise_doi(doi)
        if not norm:
            return {}
        payload = self.get_json(f"works/{norm}")
        return self._parse_message(payload.get("message", {}))

    def fetch_by_pmid(self, pmid: str) -> Dict[str, Optional[str]]:
        params = {"filter": f"pubmed:{pmid}"}
        payload = self.get_json("works", params=params)
        items = payload.get("message", {}).get("items", [])
        if not items:
            return {}
        return self._parse_message(items[0])

    def _parse_message(self, message: Dict[str, object]) -> Dict[str, Optional[str]]:
        doi = normalise_doi(coerce_text(message.get("DOI")))
        title = None
        titles = message.get("title")
        if isinstance(titles, list) and titles:
            title = coerce_text(titles[0])
        journal = None
        container_title = message.get("container-title")
        if isinstance(container_title, list) and container_title:
            journal = coerce_text(container_title[0])
        return {
            "doi": doi,
            "title": title,
            "journal": journal,
        }
