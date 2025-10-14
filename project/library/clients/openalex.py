"""OpenAlex client."""

from __future__ import annotations

from library.clients import BasePublicationsClient


class OpenAlexClient(BasePublicationsClient):
    """Stub client returning placeholder OpenAlex records."""

    def fetch_publications(self, query: str) -> list[dict[str, str | None]]:
        self.logger.info("openalex_fetch_placeholder", query=query)
        return [
            {
                "source": "openalex",
                "identifier": query,
                "title": f"OpenAlex publication for {query}",
                "published_at": None,
                "doi": None,
            }
        ]
