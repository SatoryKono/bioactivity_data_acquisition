"""Semantic Scholar client."""
from __future__ import annotations

from collections.abc import Iterable

from library.clients.base import BaseClient, SessionManager
from library.io.normalize import coerce_text, normalise_doi


class SemanticScholarClient(BaseClient):
    base_url = "https://api.semanticscholar.org/graph/v1/"

    def __init__(self, session_manager: SessionManager, rate_per_sec: float = 2.0) -> None:
        super().__init__(session_manager, name="semscholar", rate_per_sec=rate_per_sec)
        self.fields = "paperId,title,externalIds,publicationVenue"

    def fetch_by_pmid(self, pmid: str) -> dict[str, str | None]:
        payload = self.get_json(f"paper/PMID:{pmid}", params={"fields": self.fields})
        return self._parse_payload(payload)

    def fetch_batch(self, pmids: Iterable[str]) -> dict[str, dict[str, str | None]]:
        ids = [f"PMID:{pmid}" for pmid in pmids]
        if not ids:
            return {}
        response = self._request(
            "POST",
            self._build_url("paper/batch"),
            json={"ids": ids, "fields": self.fields},
        )
        items = response.json()
        parsed: dict[str, dict[str, str | None]] = {}
        for item in items:
            parsed_entry = self._parse_payload(item)
            pmid = parsed_entry.get("pmid")
            if pmid:
                parsed[pmid] = parsed_entry
        return parsed

    def _parse_payload(self, payload: dict[str, object]) -> dict[str, str | None]:
        external_ids = payload.get("externalIds") or {}
        doi = None
        pmid = None
        if isinstance(external_ids, dict):
            doi = normalise_doi(coerce_text(external_ids.get("DOI")))
            pmid = coerce_text(external_ids.get("PMID"))
        venue = payload.get("publicationVenue") or {}
        venue_name = None
        if isinstance(venue, dict):
            venue_name = coerce_text(venue.get("name"))
        return {
            "pmid": pmid,
            "doi": doi,
            "title": coerce_text(payload.get("title")),
            "venue": venue_name,
        }
