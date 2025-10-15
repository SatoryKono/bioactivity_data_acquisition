"""Client for the Crossref API."""
from __future__ import annotations

from typing import Any, Dict, Optional
from urllib.parse import quote

from bioactivity.clients.base import ApiClientError, BaseApiClient


class CrossrefClient(BaseApiClient):
    """HTTP client for Crossref works."""

    def __init__(self, **kwargs: Any) -> None:
        super().__init__("https://api.crossref.org/works", **kwargs)

    def fetch_by_doi(self, doi: str) -> Dict[str, Any]:
        """Fetch Crossref work by DOI with fallback search."""

        encoded = quote(doi, safe="")
        try:
            payload = self._request("GET", encoded)
            message = payload.get("message", payload)
            return self._parse_work(message)
        except ApiClientError as exc:
            self.logger.info("fallback_to_search", doi=doi, error=str(exc))
            payload = self._request("GET", "", params={"query.bibliographic": doi})
            items = payload.get("message", {}).get("items", [])
            if not items:
                raise
            return self._parse_work(items[0])

    def fetch_by_pmid(self, pmid: str) -> Dict[str, Any]:
        """Fetch Crossref work by PubMed identifier with fallback query."""

        try:
            payload = self._request("GET", "", params={"filter": f"pmid:{pmid}"})
        except ApiClientError as exc:
            self.logger.info("pmid_lookup_failed", pmid=pmid, error=str(exc))
            payload = {"message": {"items": []}}
        items = payload.get("message", {}).get("items", [])
        if items:
            return self._parse_work(items[0])

        self.logger.info("pmid_fallback_to_query", pmid=pmid)
        payload = self._request("GET", "", params={"query": pmid})
        items = payload.get("message", {}).get("items", [])
        if not items:
            raise ApiClientError(f"No Crossref work found for PMID {pmid}")
        return self._parse_work(items[0])

    def _parse_work(self, work: Dict[str, Any]) -> Dict[str, Any]:
        record: Dict[str, Optional[Any]] = {
            "source": "crossref",
            "doi": work.get("DOI"),
            "title": (work.get("title") or [None])[0] if isinstance(work.get("title"), list) else work.get("title"),
            "pmid": work.get("pub-med-id") or work.get("PMID"),
            "issued": work.get("issued"),
        }
        return {key: value for key, value in record.items() if value is not None}