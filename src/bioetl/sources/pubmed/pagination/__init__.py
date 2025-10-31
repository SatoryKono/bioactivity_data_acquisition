"""Pagination helpers for the PubMed E-utilities API."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from bioetl.core.api_client import UnifiedAPIClient
from bioetl.sources.pubmed.parser import parse_efetch_response, parse_esearch_response

__all__ = ["WebEnvPaginator"]


@dataclass(slots=True)
class WebEnvPaginator:
    """Iterate through PubMed search results using the WebEnv/QueryKey flow."""

    client: UnifiedAPIClient
    batch_size: int = 200

    def fetch_all(
        self,
        search_params: Mapping[str, Any],
        *,
        fetch_params: Mapping[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch all records for a PubMed search query.

        The paginator issues a single ``esearch`` request to obtain the ``WebEnv``
        and ``QueryKey`` tokens, then iteratively calls ``efetch`` using a
        deterministic batch size until all records are exhausted.
        """

        esearch_params = dict(search_params)
        esearch_params.setdefault("db", "pubmed")
        esearch_params.setdefault("retmax", 10_000)
        esearch_params.setdefault("usehistory", "y")

        esearch_response = self.client.request_json("/esearch.fcgi", params=esearch_params)
        web_env, query_key, total_count = parse_esearch_response(esearch_response)

        if not web_env or not query_key or total_count <= 0:
            return []

        fetch_defaults = dict(fetch_params or {})
        fetch_defaults.setdefault("db", "pubmed")
        fetch_defaults.setdefault("retmode", "xml")
        fetch_defaults.setdefault("rettype", "abstract")

        results: list[dict[str, Any]] = []
        offset = 0

        while offset < total_count:
            retmax = min(self.batch_size, total_count - offset)
            query = dict(fetch_defaults)
            query.update(
                {
                    "WebEnv": web_env,
                    "query_key": query_key,
                    "retstart": offset,
                    "retmax": retmax,
                }
            )

            xml_payload = self.client.request_text("/efetch.fcgi", params=query)
            batch = parse_efetch_response(xml_payload)

            if not batch:
                break

            results.extend(batch)
            offset += retmax

        return results
