"""OpenAlex client."""
from __future__ import annotations

from typing import Any, Dict

from .base import BaseClient
from ..io.normalize import coerce_text, normalise_doi


class OpenAlexClient(BaseClient):
    SOURCE = "openalex"

    def __init__(self, *, run_id: str, session=None, global_limiter=None) -> None:
        super().__init__(
            source=self.SOURCE,
            base_url="https://api.openalex.org",
            session=session,
            per_client_rps=5.0,
            global_limiter=global_limiter,
            run_id=run_id,
        )

    def fetch_by_doi(self, doi: str | None) -> Dict[str, Any]:
        norm = normalise_doi(doi)
        if not norm:
            return {}
        path = f"works/doi:{norm}"
        self.logger.info("fetching openalex by doi", extra={"doi": norm})
        payload = self.get_json(path)
        return parse_work(payload)

    def fetch_by_pmid(self, pmid: str | None) -> Dict[str, Any]:
        pmid_text = coerce_text(pmid)
        if not pmid_text:
            return {}
        path = f"works/pmid:{pmid_text}"
        self.logger.info("fetching openalex by pmid", extra={"pmid": pmid_text})
        payload = self.get_json(path)
        return parse_work(payload)


def parse_work(payload: Dict[str, Any]) -> Dict[str, Any]:
    ids = payload.get("ids", {}) if isinstance(payload.get("ids"), dict) else {}
    return {
        "openalex.doi": normalise_doi(payload.get("doi")),
        "openalex.title": coerce_text(payload.get("title")),
        "openalex.pmid": coerce_text(ids.get("pmid")),
    }

