"""Client for the Semantic Scholar API."""
from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from bioactivity.clients.base import BaseApiClient


class SemanticScholarClient(BaseApiClient):
    """HTTP client for Semantic Scholar."""

    _DEFAULT_FIELDS = [
        "title",
        "abstract",
        "publicationVenue",
        "publicationTypes",
        "externalIds",
        "year",
        "authors",
    ]

    def __init__(self, **kwargs: Any) -> None:
        default_headers = {"Accept": "application/json"}
        super().__init__("https://api.semanticscholar.org/graph/v1/paper", default_headers=default_headers, **kwargs)

    def fetch_by_pmid(self, pmid: str) -> dict[str, Any]:
        identifier = f"PMID:{pmid}"
        payload = self._request("GET", identifier, params={"fields": ",".join(self._DEFAULT_FIELDS)})
        return self._parse_paper(payload)

    def fetch_by_pmids(self, pmids: Iterable[str]) -> dict[str, dict[str, Any]]:
        ids = [f"PMID:{pmid}" for pmid in pmids]
        if not ids:
            return {}
        payload = self._request(
            "POST",
            "batch",
            json={"ids": ids, "fields": ",".join(self._DEFAULT_FIELDS)},
        )
        papers = payload.get("data") or payload.get("papers") or []
        result: dict[str, dict[str, Any]] = {}
        for paper in papers:
            pmid_value = self._extract_pmid(paper)
            if pmid_value:
                result[pmid_value] = self._parse_paper(paper)
        return result

    def _parse_paper(self, payload: dict[str, Any]) -> dict[str, Any]:
        external_ids = payload.get("externalIds") or {}
        authors = payload.get("authors")
        if isinstance(authors, list):
            author_names = [author.get("name") for author in authors if isinstance(author, dict)]
        else:
            author_names = None

        record: dict[str, Any | None] = {
            "source": "semantic_scholar",
            "pmid": self._extract_pmid(payload),
            "doi": external_ids.get("DOI"),
            "title": payload.get("title"),
            "abstract": payload.get("abstract"),
            "year": payload.get("year"),
            "publication_venue": (payload.get("publicationVenue") or {}).get("name")
            if isinstance(payload.get("publicationVenue"), dict)
            else None,
            "publication_types": payload.get("publicationTypes"),
            "authors": author_names,
        }
        return {key: value for key, value in record.items() if value is not None}

    def _extract_pmid(self, payload: dict[str, Any]) -> str | None:
        external_ids = payload.get("externalIds") or {}
        pmid = external_ids.get("PubMed") or external_ids.get("PMID")
        if isinstance(pmid, int | float):
            return str(int(pmid))
        if isinstance(pmid, str):
            return pmid.split(":")[-1]
        return None
