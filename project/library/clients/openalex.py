"""OpenAlex client."""
from __future__ import annotations

from .base import BasePublicationsClient, ClientConfig
from ..io.normalize import coerce_text, normalise_doi


class OpenAlexClient(BasePublicationsClient):
    base_url = "https://api.openalex.org/"

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
        payload = self.get_json(f"works/doi:{norm}")
        return self._parse_work(payload)

    def fetch_by_pmid(self, pmid: str) -> dict[str, str | None]:
        payload = self.get_json(f"works/pmid:{pmid}")
        return self._parse_work(payload)

    def _parse_work(self, payload: dict[str, object]) -> dict[str, str | None]:
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
