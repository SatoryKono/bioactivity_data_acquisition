"""Client helpers for UniProt search queries."""

from __future__ import annotations

from dataclasses import dataclass
from collections.abc import Iterable
from typing import Any

from bioetl.core.api_client import UnifiedAPIClient
from bioetl.sources.uniprot.request import build_gene_query, build_search_query

__all__ = ["UniProtSearchClient"]


@dataclass(slots=True)
class UniProtSearchClient:
    """Typed wrapper around :class:`~bioetl.core.api_client.UnifiedAPIClient`."""

    api: UnifiedAPIClient
    endpoint: str = "/search"
    default_fields: str | None = None
    default_format: str = "json"

    def fetch(
        self,
        accessions: Iterable[str | int | float | None],
        *,
        fields: str | None = None,
        size: int | None = None,
    ) -> dict[str, dict[str, Any]]:
        """Fetch UniProt entries for the provided accessions."""

        values = list(accessions)
        query = build_search_query(values)
        if not query:
            return {}

        params: dict[str, Any] = {
            "query": query,
            "format": self.default_format,
        }

        selected_fields = fields or self.default_fields
        if selected_fields:
            params["fields"] = selected_fields

        if size is not None:
            params["size"] = int(size)
        else:
            accession_list = [
                str(value).strip()
                for value in values
                if value is not None and str(value).strip()
            ]
            if accession_list:
                params["size"] = max(len(accession_list), 25)

        payload = self.api.request_json(self.endpoint, params=params)
        entries = payload.get("results") or payload.get("entries") or []
        if not isinstance(entries, list):
            return {}

        result: dict[str, dict[str, Any]] = {}
        for item in entries:
            if not isinstance(item, dict):
                continue
            primary = item.get("primaryAccession") or item.get("accession")
            if not primary:
                continue
            result[str(primary)] = item
        return result

    def search_by_gene(
        self,
        gene_symbol: str,
        *,
        organism: str | None = None,
        fields: str | None = None,
    ) -> dict[str, Any] | None:
        """Search for a UniProt entry using gene and optional organism filters."""

        query = build_gene_query(gene_symbol, organism)
        if not query:
            return None

        params: dict[str, Any] = {
            "query": query,
            "format": self.default_format,
            "size": 1,
        }
        selected_fields = fields or self.default_fields
        if selected_fields:
            params["fields"] = selected_fields

        payload = self.api.request_json(self.endpoint, params=params)
        entries = payload.get("results") or payload.get("entries") or []
        if not isinstance(entries, list) or not entries:
            return None

        first = entries[0]
        return first if isinstance(first, dict) else None
