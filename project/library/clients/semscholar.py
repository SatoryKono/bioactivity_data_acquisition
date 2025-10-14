"""Semantic Scholar client for publication metadata."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from library.clients import BasePublicationsClient, ClientConfig
from library.io.normalize import coerce_text, parse_semscholar_paper
from library.utils.logging import bind_context

_FIELDS = "paperId,title,externalIds,year"


class SemanticScholarClient(BasePublicationsClient):
    """Client for the Semantic Scholar Graph API."""

    def __init__(self, config: ClientConfig, *, batch_size: int = 50, **kwargs: Any) -> None:
        super().__init__(config, **kwargs)
        self.batch_size = batch_size

    def fetch_by_pmid(self, pmid: str) -> dict[str, Any] | None:
        if not pmid:
            return None
        payload = self._request(
            f"paper/PMID:{pmid}",
            params={"fields": _FIELDS},
            context={"pmid": pmid},
        )
        if isinstance(payload, Mapping):
            record = parse_semscholar_paper(payload)
            if record:
                bind_context(self.logger, pmid=record.get("pmid"), doi=record.get("doi")).info(
                    "semscholar_record", **record
                )
            return record
        return None

    def fetch_batch_by_pmid(self, pmids: Iterable[str]) -> dict[str, dict[str, Any]]:
        unique_pmids = [coerce_text(pmid) for pmid in pmids if coerce_text(pmid)]
        if not unique_pmids:
            return {}

        seen: set[str] = set()
        ordered_pmids = [pmid for pmid in unique_pmids if not (pmid in seen or seen.add(pmid))]
        records: dict[str, dict[str, Any]] = {}
        for start in range(0, len(ordered_pmids), self.batch_size):
            chunk = ordered_pmids[start : start + self.batch_size]
            payload = self._request(
                "paper/batch",
                method="POST",
                json_payload={"ids": [f"PMID:{pmid}" for pmid in chunk], "fields": _FIELDS},
                context={"pmid": ",".join(chunk)},
            )
            if isinstance(payload, list):
                for entry in payload:
                    if isinstance(entry, Mapping):
                        record = parse_semscholar_paper(entry)
                        pmid = record.get("pmid")
                        if pmid:
                            records[pmid] = record
                            bind_context(self.logger, pmid=pmid, doi=record.get("doi")).info(
                                "semscholar_record",
                                **record,
                            )
        return records
