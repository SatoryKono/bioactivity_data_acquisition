"""Crossref client."""

from __future__ import annotations

from library.clients import BasePublicationsClient


class CrossrefClient(BasePublicationsClient):
    """Stub client returning placeholder Crossref records."""

    def fetch_publications(self, query: str) -> list[dict[str, str | None]]:
        self.logger.info("crossref_fetch_placeholder", query=query)
        return [
            {
                "source": "crossref",
                "identifier": query,
                "title": f"Crossref publication for {query}",
                "published_at": None,
                "doi": None,
            }
        ]
