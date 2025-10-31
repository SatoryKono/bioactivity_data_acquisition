"""Client helpers for retrieving UniProt ortholog relationships."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any
import pandas as pd

from bioetl.core.api_client import UnifiedAPIClient
from bioetl.core.deprecation import warn_legacy_client
from bioetl.sources.uniprot.request import build_ortholog_query

__all__ = ["UniProtOrthologsClient"]


warn_legacy_client(__name__, replacement="bioetl.adapters.uniprot")


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


@dataclass(slots=True)
class UniProtOrthologClientAdapter:
    """Compatibility adapter exposing legacy ``fetch(accession, taxonomy_id)``.

    Wraps a ``UniProtOrthologsClient`` and converts results to a dataframe with
    priority scoring using a provided priority map.
    """

    client: UniProtOrthologsClient
    priority_map: dict[str, int]

    def fetch(self, accession: str, taxonomy_id: Any | None = None) -> pd.DataFrame:
        entries = self.client.fetch(accession)
        records: list[dict[str, Any]] = []
        for item in entries:
            ortholog_acc = item.get("primaryAccession") or item.get("accession")
            organism = item.get("organism", {}) if isinstance(item.get("organism"), dict) else item.get("organism")
            if isinstance(organism, dict):
                organism_name = organism.get("scientificName") or organism.get("commonName")
                organism_id = organism.get("taxonId") or organism.get("taxonIdentifier")
            else:
                organism_name = organism
                organism_id = item.get("organismId")
            organism_id_str = str(organism_id) if organism_id is not None else None
            priority = self.priority_map.get(organism_id_str or "", 99)
            records.append(
                {
                    "source_accession": accession,
                    "ortholog_accession": ortholog_acc,
                    "organism": organism_name,
                    "organism_id": organism_id,
                    "priority": priority,
                }
            )

        if taxonomy_id is not None:
            try:
                taxonomy_str = str(int(taxonomy_id))
            except (TypeError, ValueError):
                taxonomy_str = None
            if taxonomy_str:
                for record in records:
                    if str(record.get("organism_id")) == taxonomy_str:
                        record["priority"] = min(int(record.get("priority", 99)), -1)

        return pd.DataFrame(records).convert_dtypes()
