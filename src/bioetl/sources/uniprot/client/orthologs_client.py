"""Client helpers for retrieving UniProt ortholog relationships."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from bioetl.core.api_client import UnifiedAPIClient
from bioetl.sources.uniprot.request import build_ortholog_query

__all__ = ["UniProtOrthologsClient"]


@dataclass(slots=True)
class UniProtOrthologsClient:
    """Wrapper around :class:`UnifiedAPIClient` for ortholog lookups."""

    api: UnifiedAPIClient
    endpoint: str = "/search"
    default_fields: str | None = None
    default_format: str = "json"
    default_size: int = 200

    def fetch(
        self,
        accession: str,
        *,
        fields: str | None = None,
        size: int | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch ortholog records for the provided accession."""

        query = build_ortholog_query(accession)
        if not query:
            return []

        params: dict[str, Any] = {
            "query": query,
            "format": self.default_format,
            "size": int(size) if size is not None else self.default_size,
        }

        selected_fields = fields or self.default_fields
        if selected_fields:
            params["fields"] = selected_fields

        payload = self.api.request_json(self.endpoint, params=params)
        entries = payload.get("results") or payload.get("entries") or []
        if not isinstance(entries, list):
            return []

        return [entry for entry in entries if isinstance(entry, dict)]
