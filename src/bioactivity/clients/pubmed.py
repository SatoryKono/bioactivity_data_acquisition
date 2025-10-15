"""Client for the PubMed API."""
from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from bioactivity.config import APIClientConfig
from bioactivity.clients.base import ApiClientError, BaseApiClient


class PubMedClient(BaseApiClient):
    """HTTP client for PubMed E-utilities."""

    def __init__(self, config: APIClientConfig, **kwargs: Any) -> None:
        headers = dict(config.headers)
        headers.setdefault("Accept", "application/json")
        enhanced = config.model_copy(update={"headers": headers})
        super().__init__(enhanced, **kwargs)

    def fetch_by_pmid(self, pmid: str) -> dict[str, Any]:
        payload = self._request("GET", "", params={"id": pmid, "format": "json"})
        record = self._extract_record(payload, pmid)
        if record is None:
            raise ApiClientError(f"No PubMed record found for PMID {pmid}")
        return record

    def fetch_by_pmids(self, pmids: Iterable[str]) -> dict[str, dict[str, Any]]:
        pmid_list = list(pmids)
        if not pmid_list:
            return {}
        payload = self._request("POST", "batch", json={"ids": pmid_list})
        records = payload.get("records", [])
        result: dict[str, dict[str, Any]] = {}
        for pmid in pmid_list:
            record = next((item for item in records if str(item.get("pmid")) == str(pmid)), None)
            if record is not None:
                result[str(pmid)] = self._normalise_record(record)
        return result

    def _extract_record(self, payload: dict[str, Any], pmid: str) -> dict[str, Any] | None:
        if "result" in payload and isinstance(payload["result"], dict):
            data = payload["result"].get(pmid)
            if data is None and "uids" in payload["result"]:
                keys = payload["result"]["uids"]
                for key in keys:
                    if str(key) == str(pmid):
                        data = payload["result"].get(key)
                        break
            if data is not None:
                return self._normalise_record(data)

        if "records" in payload and isinstance(payload["records"], list):
            for item in payload["records"]:
                if str(item.get("pmid")) == str(pmid):
                    return self._normalise_record(item)

        if payload.get("pmid") and str(payload.get("pmid")) == str(pmid):
            return self._normalise_record(payload)
        return None

    def _normalise_record(self, record: dict[str, Any]) -> dict[str, Any]:
        authors = record.get("authors") or record.get("authorList")
        if isinstance(authors, dict):
            authors = authors.get("authors")
        if isinstance(authors, list):
            formatted_authors = [self._format_author(author) for author in authors]
        else:
            formatted_authors = None

        doi_value: str | None
        doi_list = record.get("doiList")
        if isinstance(doi_list, list) and doi_list:
            doi_value = doi_list[0]
        else:
            doi_value = record.get("doi")

        parsed: dict[str, Any | None] = {
            "source": "pubmed",
            "pmid": record.get("pmid") or record.get("PMID"),
            "title": record.get("title") or record.get("articleTitle"),
            "abstract": record.get("abstract"),
            "doi": doi_value,
            "authors": formatted_authors,
        }
        return {key: value for key, value in parsed.items() if value is not None}

    def _format_author(self, author: Any) -> str:
        if isinstance(author, str):
            return author
        if isinstance(author, dict):
            last = author.get("lastName") or author.get("lastname")
            fore = author.get("foreName") or author.get("forename")
            if last and fore:
                return f"{fore} {last}"
            return author.get("name") or " ".join(filter(None, [fore, last]))
        return str(author)