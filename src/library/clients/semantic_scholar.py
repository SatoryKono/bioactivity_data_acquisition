"""Client for the Semantic Scholar API."""
from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from library.clients.base import BaseApiClient
from library.config import APIClientConfig


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

    def __init__(self, config: APIClientConfig, **kwargs: Any) -> None:
        headers = dict(config.headers)
        headers.setdefault("Accept", "application/json")
        enhanced = config.model_copy(update={"headers": headers})
        super().__init__(enhanced, **kwargs)

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
            "semantic_scholar_pmid": self._extract_pmid(payload),
            "semantic_scholar_doi": external_ids.get("DOI"),
            "semantic_scholar_semantic_scholar_id": payload.get("paperId"),
            "semantic_scholar_publication_types": payload.get("publicationTypes"),
            "semantic_scholar_venue": (payload.get("publicationVenue") or {}).get("name")
            if isinstance(payload.get("publicationVenue"), dict)
            else None,
            "semantic_scholar_external_ids": str(external_ids) if external_ids else None,
            "semantic_scholar_error": None,  # Will be set if there's an error
            # Legacy fields for backward compatibility
            "title": payload.get("title"),
            "abstract": payload.get("abstract"),
            "year": payload.get("year"),
            "authors": author_names,
        }
        # Return all fields, including None values, to maintain schema consistency
        return record

    def _extract_pmid(self, payload: dict[str, Any]) -> str | None:
        external_ids = payload.get("externalIds") or {}
        pmid = external_ids.get("PubMed") or external_ids.get("PMID")
        if isinstance(pmid, int | float):
            return str(int(pmid))
        if isinstance(pmid, str):
            return pmid.split(":")[-1]
        return None
