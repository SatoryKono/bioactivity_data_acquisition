"""Crossref client."""
from __future__ import annotations

from .base import BasePublicationsClient, ClientConfig
from ..io.normalize import coerce_text, normalise_doi


class CrossrefClient(BasePublicationsClient):
    base_url = "https://api.crossref.org/"

    def __init__(self, config: ClientConfig) -> None:
        super().__init__(config)

    def fetch_publications(self, query: str) -> list[dict[str, str | None]]:
        """Fetch publications for a given query."""
        try:
            # Try as DOI first
            result = self.fetch_by_doi(query)
            if result:
                return [result]
            # Try as PMID
            result = self.fetch_by_pmid(query)
            return [result] if result else []
        except Exception:
            return []

    def fetch_by_doi(self, doi: str) -> dict[str, str | None]:
        norm = normalise_doi(doi)
        if not norm:
            return {}
        payload = self.get_json(f"works/{norm}")
        return self._parse_message(payload.get("message", {}))

    def fetch_by_pmid(self, pmid: str) -> dict[str, str | None]:
        params = {"filter": f"pubmed:{pmid}"}
        payload = self.get_json("works", params=params)
        items = payload.get("message", {}).get("items", [])
        if not items:
            return {}
        return self._parse_message(items[0])

    def _parse_message(self, message: dict[str, object]) -> dict[str, str | None]:
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
