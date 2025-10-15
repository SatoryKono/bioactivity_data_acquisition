"""PubMed client."""
from __future__ import annotations

from collections.abc import Iterable

from .base import BasePublicationsClient, ClientConfig
from ..io.normalize import coerce_text, normalise_doi


class PubMedClient(BasePublicationsClient):
    base_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/"

    def __init__(self, config: ClientConfig) -> None:
        super().__init__(config)

    def fetch_publications(self, query: str) -> list[dict[str, str | None]]:
        """Fetch publications for a given query."""
        try:
            result = self.fetch_by_pmid(query)
            return [result] if result else []
        except Exception:
            return []

    def fetch_by_pmid(self, pmid: str) -> dict[str, str | None]:
        payload = self._summary_request([pmid])
        return payload.get(pmid, {})

    def fetch_batch(self, pmids: Iterable[str]) -> dict[str, dict[str, str | None]]:
        pmid_list = list(pmids)
        if not pmid_list:
            return {}
        return self._summary_request(pmid_list)

    def _summary_request(self, pmids: list[str]) -> dict[str, dict[str, str | None]]:
        params = {
            "db": "pubmed",
            "id": ",".join(pmids),
            "retmode": "json",
        }
        data = self._request("GET", self._build_url("esummary.fcgi"), params=params).json()
        result = data.get("result", {})
        summaries: dict[str, dict[str, str | None]] = {}
        for pmid in pmids:
            raw = result.get(pmid, {})
            doi = None
            article_ids = raw.get("articleids", [])
            for entry in article_ids:
                if entry.get("idtype") == "doi":
                    doi = normalise_doi(entry.get("value"))
                    break
            summaries[pmid] = {
                "pmid": pmid,
                "title": coerce_text(raw.get("title")),
                "journal": coerce_text(raw.get("fulljournalname")),
                "doi": doi,
            }
        return summaries
