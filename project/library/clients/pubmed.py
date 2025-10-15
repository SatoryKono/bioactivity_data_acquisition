"""PubMed client for publication metadata."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from library.clients import BasePublicationsClient, ClientConfig
from library.io.normalize import coerce_text, parse_pubmed_summary
from library.utils.logging import bind_context


class PubmedClient(BasePublicationsClient):
    """Client for PubMed E-utilities."""

    def __init__(self, config: ClientConfig, *, batch_size: int = 100, **kwargs: Any) -> None:
        super().__init__(config, **kwargs)
        self.batch_size = batch_size

    def fetch_by_pmid(self, pmid: str) -> dict[str, Any] | None:
        results = self.fetch_batch_by_pmid([pmid])
        return results.get(coerce_text(pmid) or pmid)

    def fetch_batch_by_pmid(self, pmids: Iterable[str]) -> dict[str, dict[str, Any]]:
        unique_pmids = [coerce_text(pmid) for pmid in pmids if coerce_text(pmid)]
        if not unique_pmids:
            return {}

        seen: set[str] = set()
        ordered_pmids = [pmid for pmid in unique_pmids if not (pmid in seen or seen.add(pmid))]
        records: dict[str, dict[str, Any]] = {}
        for start in range(0, len(ordered_pmids), self.batch_size):
            chunk = ordered_pmids[start : start + self.batch_size]
            context = {"pmid": ",".join(chunk)}
            payload = self._request(
                "esummary.fcgi",
                params={"db": "pubmed", "id": ",".join(chunk), "retmode": "json"},
                context=context,
            )
            result = payload.get("result", {}) if isinstance(payload, Mapping) else {}
            for pmid in chunk:
                entry = result.get(pmid) or result.get(str(pmid))
                if isinstance(entry, Mapping):
                    records[pmid] = parse_pubmed_summary(pmid, entry)
                    bind_context(self.logger, pmid=pmid).info("pubmed_record", **records[pmid])
        return records
