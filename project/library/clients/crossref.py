"""Crossref API client."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any
from urllib.parse import quote

from library.clients import BasePublicationsClient, ClientConfig
from library.io.normalize import normalise_doi, parse_crossref_work
from library.utils.errors import ExtractionError
from library.utils.logging import bind_context


class CrossrefClient(BasePublicationsClient):
    """Client for the Crossref works API."""

    def __init__(self, config: ClientConfig, **kwargs: Any) -> None:
        super().__init__(config, **kwargs)

    def fetch_by_doi(self, doi: str) -> Mapping[str, Any]:
        normalised = normalise_doi(doi)
        if not normalised:
            raise ExtractionError("DOI is required for Crossref lookup")
        payload = self._request(f"works/{quote(normalised)}")
        record = parse_crossref_work(payload)
        bind_context(self.logger, doi=record.get("doi")).info("crossref_record", **record)
        return record
