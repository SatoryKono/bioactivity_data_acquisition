"""Request builder enforcing PubMed E-utilities etiquette."""

from __future__ import annotations

from typing import Any, Mapping, Sequence

from bioetl.adapters.base import AdapterConfig
from bioetl.core.api_client import APIConfig
from bioetl.sources.common.request import BaseRequestBuilder, RequestSpec

__all__ = ["PubMedRequestBuilder"]


class PubMedRequestBuilder(BaseRequestBuilder):
    """Build requests for the PubMed EFetch and ESearch endpoints."""

    DEFAULT_USER_AGENT = "bioetl-pubmed-client/1.0"

    def __init__(self, api_config: APIConfig, adapter_config: AdapterConfig):
        super().__init__(api_config)
        self.tool = getattr(adapter_config, "tool", "bioactivity_etl") or "bioactivity_etl"
        self.email = getattr(adapter_config, "email", "") or ""
        self.api_key = getattr(adapter_config, "api_key", "") or ""

        headers = self.base_headers
        user_agent = headers.get("User-Agent", self.DEFAULT_USER_AGENT)
        if self.email and "mailto:" not in user_agent:
            user_agent = f"{user_agent} (mailto:{self.email})"
        headers["User-Agent"] = user_agent
        self._base_headers = headers

    def efetch(self, pmids: Sequence[str]) -> RequestSpec:
        """Build a request specification for the ``efetch.fcgi`` endpoint."""

        params: dict[str, Any] = {
            "db": "pubmed",
            "id": ",".join(pmids),
            "retmode": "xml",
            "rettype": "abstract",
        }
        params.update(self._etiquette_params())
        return super().build("/efetch.fcgi", params=params)

    def esearch(self, query: Mapping[str, Any]) -> RequestSpec:
        """Build a request specification for the ``esearch.fcgi`` endpoint."""

        params: dict[str, Any] = {**query}
        params.update(self._etiquette_params())
        return super().build("/esearch.fcgi", params=params)

    def _etiquette_params(self) -> dict[str, Any]:
        params = {"tool": self.tool}
        if self.email:
            params["email"] = self.email
        if self.api_key:
            params["api_key"] = self.api_key
        return params
