"""Pagination helpers for the PubMed E-utilities API."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping

from bioetl.core.api_client import UnifiedAPIClient
from bioetl.core.pagination import (
    TokenInitialization,
    TokenPaginationStrategy,
    TokenRequest,
)
from bioetl.sources.pubmed.parser import parse_efetch_response, parse_esearch_response

__all__ = ["WebEnvPaginator"]


@dataclass(slots=True)
class WebEnvPaginator:
    """Iterate through PubMed search results using the WebEnv/QueryKey flow."""

    client: UnifiedAPIClient
    batch_size: int = 200
    _strategy: TokenPaginationStrategy = field(init=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "_strategy", TokenPaginationStrategy(self.client))

    def fetch_all(
        self,
        search_params: Mapping[str, Any],
        *,
        fetch_params: Mapping[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch all records for a PubMed search query."""

        def initializer(client: UnifiedAPIClient) -> TokenInitialization[dict[str, Any]] | None:
            esearch_params = dict(search_params)
            esearch_params.setdefault("db", "pubmed")
            esearch_params.setdefault("retmax", 10_000)
            esearch_params.setdefault("usehistory", "y")

            esearch_response = client.request_json("/esearch.fcgi", params=esearch_params)
            web_env, query_key, total_count = parse_esearch_response(esearch_response)

            if not web_env or not query_key or total_count <= 0:
                return None

            fetch_defaults = dict(fetch_params or {})
            fetch_defaults.setdefault("db", "pubmed")
            fetch_defaults.setdefault("retmode", "xml")
            fetch_defaults.setdefault("rettype", "abstract")

            state = {
                "web_env": web_env,
                "query_key": query_key,
                "fetch_defaults": fetch_defaults,
            }
            return TokenInitialization(state=state, total_count=total_count)

        def fetcher(
            client: UnifiedAPIClient,
            state: Mapping[str, Any],
            offset: int,
            chunk_size: int,
        ) -> list[Mapping[str, Any]]:
            query = dict(state.get("fetch_defaults", {}))
            query.update(
                {
                    "WebEnv": state.get("web_env"),
                    "query_key": state.get("query_key"),
                    "retstart": offset,
                    "retmax": chunk_size,
                }
            )

            xml_payload = client.request_text("/efetch.fcgi", params=query)
            return parse_efetch_response(xml_payload)

        request: TokenRequest[Mapping[str, Any]] = TokenRequest(
            initializer=initializer,
            fetcher=fetcher,
            batch_size=self.batch_size,
        )
        return self._strategy.paginate(request)
