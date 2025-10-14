"""ChEMBL API client."""

from __future__ import annotations

from library.clients import BasePublicationsClient


class ChemblClient(BasePublicationsClient):
    """Client for querying ChEMBL documents."""

    def fetch_publications(self, query: str) -> list[dict[str, str | None]]:
        self.logger.info("chembl_fetch_placeholder", query=query)
        return [
            {
                "source": "chembl",
                "identifier": query,
                "title": f"ChEMBL publication for {query}",
                "published_at": None,
                "doi": None,
            }
        ]
