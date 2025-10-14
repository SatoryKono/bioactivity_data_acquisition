"""OpenAlex API client."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any
from urllib.parse import quote

from library.clients import BasePublicationsClient, ClientConfig
from library.io.normalize import coerce_text, normalise_doi, parse_openalex_work
from library.utils.errors import ExtractionError
from library.utils.logging import bind_context


class OpenAlexClient(BasePublicationsClient):
    """Client for the OpenAlex works API."""

    def __init__(self, config: ClientConfig, **kwargs: Any) -> None:
        super().__init__(config, **kwargs)

    def fetch_by_doi(self, doi: str) -> Mapping[str, Any]:
        normalised = normalise_doi(doi)
        if not normalised:
            raise ExtractionError("DOI is required for OpenAlex lookup")
        payload = self._request(f"works/https://doi.org/{quote(normalised)}", context={"doi": normalised})
        record = parse_openalex_work(payload)
        bind_context(self.logger, doi=record.get("doi")).info("openalex_record", **record)
        return record

    def fetch_by_pmid(self, pmid: str) -> Mapping[str, Any] | None:
        normalized_pmid = coerce_text(pmid)
        if not normalized_pmid:
            return None
        payload = self._request(f"works/PMID:{normalized_pmid}", context={"pmid": normalized_pmid})
        record = parse_openalex_work(payload)
        bind_context(self.logger, pmid=normalized_pmid, doi=record.get("doi")).info(
            "openalex_record",
            **record,
        )
        return record
