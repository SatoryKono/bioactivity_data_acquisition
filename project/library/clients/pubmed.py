"""PubMed client for publication metadata."""

from __future__ import annotations

from library.clients import BasePublicationsClient


class PubmedClient(BasePublicationsClient):
    """Stub client returning placeholder PubMed records."""

    def fetch_publications(self, query: str) -> list[dict[str, str | None]]:
        self.logger.info("pubmed_fetch_placeholder", query=query)
        return [
            {
                "source": "pubmed",
                "identifier": query,
                "title": f"PubMed publication for {query}",
                "published_at": None,
                "doi": None,
            }
        ]
