"""Semantic Scholar client."""

from __future__ import annotations

from library.clients import BasePublicationsClient


class SemanticScholarClient(BasePublicationsClient):
    """Stub client returning placeholder Semantic Scholar records."""

    def fetch_publications(self, query: str) -> list[dict[str, str | None]]:
        self.logger.info("semscholar_fetch_placeholder", query=query)
        return [
            {
                "source": "semscholar",
                "identifier": query,
                "title": f"Semantic Scholar publication for {query}",
                "published_at": None,
                "doi": None,
            }
        ]
