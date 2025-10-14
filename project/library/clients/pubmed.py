"""PubMed client using the NCBI citation endpoint."""
from __future__ import annotations

from typing import Dict, Iterable, List, Mapping

from .base import BaseClient
from ..io.normalize import coerce_text, normalise_doi


class PubMedClient(BaseClient):
    SOURCE = "pubmed"

    def __init__(self, *, run_id: str, session=None, global_limiter=None) -> None:
        super().__init__(
            source=self.SOURCE,
            base_url="https://api.ncbi.nlm.nih.gov/lit/ctxp/v1",
            session=session,
            per_client_rps=3.0,
            global_limiter=global_limiter,
            run_id=run_id,
        )

    def fetch_by_pmid(self, pmid: str | None) -> Dict[str, str | None]:
        pmid_text = coerce_text(pmid)
        if not pmid_text:
            return {}
        self.logger.info("fetching pubmed by pmid", extra={"pmid": pmid_text})
        payload = self.get_json("pubmed/", params={"format": "csl", "id": pmid_text})
        return parse_pubmed_payload(payload)

    def fetch_batch_by_pmids(self, pmids: Iterable[str]) -> Mapping[str, Dict[str, str | None]]:
        unique = [coerce_text(p) for p in pmids]
        filtered: List[str] = [p for p in unique if p]
        if not filtered:
            return {}
        joined = ",".join(filtered)
        self.logger.info("batch fetching pubmed", extra={"extra_fields": {"count": len(filtered)}})
        payload = self.get_json("pubmed/", params={"format": "csl", "id": joined})
        records = normalise_batch_payload(payload)
        return {coerce_text(rec.get("id")): parse_pubmed_payload(rec) for rec in records if rec.get("id")}


def normalise_batch_payload(payload) -> List[Dict[str, str | None]]:
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        items = payload.get("records")
        if isinstance(items, list):
            return items
        return [payload]
    return []


def parse_pubmed_payload(payload) -> Dict[str, str | None]:
    if isinstance(payload, list) and payload:
        payload = payload[0]
    if not isinstance(payload, dict):
        return {}
    return {
        "pubmed.pmid": coerce_text(payload.get("id")),
        "pubmed.title": coerce_text(payload.get("title")),
        "pubmed.doi": normalise_doi(payload.get("DOI")),
    }

