"""Request builder for Crossref REST API calls."""

from __future__ import annotations

from typing import Any, Mapping

from bioetl.adapters.base import AdapterConfig
from bioetl.core.api_client import APIConfig
from bioetl.sources.common.request import BaseRequestBuilder, RequestSpec

__all__ = ["CrossrefRequestBuilder"]


class CrossrefRequestBuilder(BaseRequestBuilder):
    """Construct Crossref requests with API etiquette applied."""

    DEFAULT_USER_AGENT = "bioetl-crossref-client/1.0"

    def __init__(self, api_config: APIConfig, adapter_config: AdapterConfig):
        super().__init__(api_config)
        mailto = getattr(adapter_config, "mailto", "") or ""
        self._mailto = mailto.strip()

        headers = self.base_headers
        user_agent = headers.get("User-Agent", self.DEFAULT_USER_AGENT)
        if self._mailto and "mailto:" not in user_agent:
            user_agent = f"{user_agent} (mailto:{self._mailto})"
        headers["User-Agent"] = user_agent
        self._base_headers = headers

    @property
    def mailto(self) -> str | None:
        """Return the configured contact email if provided."""

        return self._mailto or None

    def build(
        self,
        path: str,
        *,
        params: Mapping[str, Any] | None = None,
        request_id: str | None = None,
        page: int | str | None = None,
    ) -> RequestSpec:
        """Build a Crossref request applying etiquette defaults."""

        query: dict[str, Any] = {}
        if params:
            query.update(params)
        if self._mailto and "mailto" not in query:
            query["mailto"] = self._mailto

        return super().build(path, params=query, request_id=request_id, page=page)
