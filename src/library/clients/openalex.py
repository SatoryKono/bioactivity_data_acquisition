"""Client for the OpenAlex API."""
from __future__ import annotations

from typing import Any

from library.clients.base import ApiClientError, BaseApiClient
from library.config import APIClientConfig


class OpenAlexClient(BaseApiClient):
    """HTTP client for OpenAlex works."""

    def __init__(self, config: APIClientConfig, **kwargs: Any) -> None:
        super().__init__(config, **kwargs)

    def fetch_by_doi(self, doi: str) -> dict[str, Any]:
        """Fetch a work by DOI with fallback to a filter query."""

        path = f"https://doi.org/{doi}"
        try:
            payload = self._request("GET", path)
            return self._parse_work(payload)
        except ApiClientError as exc:
            self.logger.info("openalex_doi_fallback", doi=doi, error=str(exc))
            payload = self._request("GET", "", params={"filter": f"doi:{doi}"})
            results = payload.get("results", [])
            if not results:
                raise
            return self._parse_work(results[0])

    def fetch_by_pmid(self, pmid: str) -> dict[str, Any]:
        """Fetch a work by PMID with fallback search."""

        payload = self._request("GET", "", params={"filter": f"pmid:{pmid}"})
        results = payload.get("results", [])
        if results:
            return self._parse_work(results[0])

        self.logger.info("openalex_pmid_fallback", pmid=pmid)
        payload = self._request("GET", "", params={"search": pmid})
        results = payload.get("results", [])
        if not results:
            raise ApiClientError(f"No OpenAlex work found for PMID {pmid}")
        return self._parse_work(results[0])

    def _parse_work(self, work: dict[str, Any]) -> dict[str, Any]:
        ids = work.get("ids") or {}
        
        # Извлекаем DOI и убираем префикс https://doi.org/
        doi_value = work.get("doi") or ids.get("doi")
        if doi_value and doi_value.startswith("https://doi.org/"):
            doi_value = doi_value.replace("https://doi.org/", "")
        
        record: dict[str, Any | None] = {
            "source": "openalex",
            "openalex_doi_key": doi_value,
            "openalex_title": work.get("title") or work.get("display_name"),
            "openalex_doc_type": work.get("type"),
            "openalex_type_crossref": work.get("type_crossref"),
            "openalex_publication_year": work.get("publication_year"),
            "openalex_error": None,  # Will be set if there's an error
        }
        # Return all fields, including None values, to maintain schema consistency
        return record
