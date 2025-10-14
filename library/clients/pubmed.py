"""Client for interacting with the NCBI PubMed API."""
from __future__ import annotations

from typing import Mapping

from .base import BaseClient


class PubMedClient(BaseClient):
    """Client tailored for the ESummary endpoint."""

    def fetch_by_pmid(self, pmid: str) -> Mapping[str, object]:
        return self._get("esummary.fcgi", params={"db": "pubmed", "id": pmid, "retmode": "json"})
